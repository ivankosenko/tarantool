/*
 * Copyright 2010-2019, Tarantool AUTHORS, please see AUTHORS file.
 *
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * 1. Redistributions of source code must retain the above
 *    copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer in the documentation and/or other materials
 *    provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * <COPYRIGHT HOLDER> OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

#include <stddef.h>
#include "coio_popen.h"
#include "coio_task.h"
#include "fiber.h"
#include "say.h"
#include "fio.h"
#include <stdio.h>
#include <stdlib.h>
#include <dirent.h>
#include <sys/wait.h>
#include <paths.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>

/*
 * On OSX this global variable is not declared
 * in <unistd.h>
 */
extern char **environ;


struct popen_data {
	/* process id */
	pid_t pid;
	int fh[3];
	/*
	 * Three handles:
	 * [0] write to stdin of the child process
	 * [1] read from stdout of the child process
	 * [2] read from stderr of the child process
	 */

	/* The ID of socket was read recently
	 * (STDERR_FILENO or STDOUT_FILENO */
	int prev_source;
};

/*
 * Returns next socket to read.
 * Use this function when both STDOUT and STDERR outputs
 * are ready for reading.
 * */
static inline int
get_handle_in_order(struct popen_data *data)
{
	/*
	 * Invert the order of handles to be read
	 */
	const int mask = STDERR_FILENO | STDOUT_FILENO;
	data->prev_source ^= mask;

	/*
	 * If handle is not available, invert it back
	 */
	if (data->fh[data->prev_source] < 0)
		data->prev_source ^= mask;
	/*
	 * if both reading handles are invalid return -1
	 */
	return data->fh[data->prev_source];
}

static struct popen_data *
popen_data_new()
{
	struct popen_data *data =
		(struct popen_data *)calloc(1, sizeof(*data));
	data->fh[0] = -1;
	data->fh[1] = -1;
	data->fh[2] = -1;
	data->prev_source = STDERR_FILENO;
	/*
	 * if both streams are ready then
	 * start reading from STDOUT
	 */
	return data;
}

void *
coio_popen_impl(const char *command, const char *type)
{
	pid_t pid;
	int socket_rw[2] = {-1,-1};
	int socket_err[2] = {-1,-1};
	errno = 0;

	char *argv[] = {"sh", "-c", NULL, NULL};
	argv[2] = (char *)command;

	if ((*type != 'r' && *type != 'w') || type[1] != '\0') {
		errno = EINVAL;
		return NULL;
	}

	struct popen_data *data = popen_data_new();
	if (data == NULL)
		return NULL;

	/*
	 * Enable non-blocking for the parent side
	 * and close-on-exec on the child's side.
	 * The socketpair on OSX doesn't support
	 * SOCK_NONBLOCK & SOCK_CLOEXEC flags.
	 */
	if (socketpair(AF_UNIX, SOCK_STREAM, 0, socket_rw) < 0 ||
		fcntl(socket_rw[0], F_SETFL, O_NONBLOCK) < 0 ||
		fcntl(socket_rw[1], F_SETFD, FD_CLOEXEC) < 0) {
		goto on_error;
	}

	if (socketpair(AF_UNIX, SOCK_STREAM, 0, socket_err) < 0 ||
		fcntl(socket_err[0], F_SETFL, O_NONBLOCK) < 0 ||
	    	fcntl(socket_err[1], F_SETFD, FD_CLOEXEC) < 0) {
		goto on_error;
	}

	pid = fork();

	if (pid < 0)
		goto on_error;
	else if (pid == 0) /* child */ {
		/* Setup stdin/stdout */
		close(socket_rw[0]);
		int fno = (*type == 'r') ? STDOUT_FILENO
					 : STDIN_FILENO;
		if (socket_rw[1] != fno) {
			dup2(socket_rw[1], fno);
			close(socket_rw[1]);
		}

		/* setup stderr */
		close(socket_err[0]);
		if (socket_err[1] != STDERR_FILENO) {
			dup2(socket_err[1], STDERR_FILENO);
			close(socket_err[1]);
		}

		execve(_PATH_BSHELL, argv, environ);
		_exit(127);
		unreachable();
	}

	/* parent process */
	close(socket_rw[1]);
	close(socket_err[1]);

	if (*type == 'r')
		data->fh[STDOUT_FILENO] = socket_rw[0];
	else
		data->fh[STDIN_FILENO] = socket_rw[0];

	data->fh[STDERR_FILENO] = socket_err[0];
	data->pid = pid;

	return data;

on_error:
	if (data)
		free(data);
	if (socket_rw[0] >= 0) {
		close(socket_rw[0]);
		close(socket_rw[1]);
	}
	if (socket_err[0] >= 0) {
		close(socket_err[0]);
		close(socket_err[1]);
	}
	return NULL;
}

int
coio_try_pclose_impl(void *fh)
{
	struct popen_data *data = (struct popen_data *)fh;

	if (data == NULL){
		errno = EBADF;
		return -1;
	}

	/* Close all handles */
	for(int i = 0; i < 3; ++i) {
		if (data->fh[i] >= 0) {
			close(data->fh[i]);
			data->fh[i] = -1;
		}
	}

	int pstat;
	pid_t pid = waitpid(data->pid, &pstat, WNOHANG);

	int rc = 0;

	if (pid == 0)
		return -2;		/* Process is still running */
	else if (pid < 0) {
		if (errno == ECHILD)
			rc = 0;		/* Child process is not found
 					 * (may be is already dead)
 					 */
		else if (errno == EINTR)
			return -2;	/* Retry */
		else
			rc = -1;	/* An error occurred */
	}

	free(data);
	return rc;
}

int
coio_popen_try_to_read(void *fh, void *buf, size_t count,
	size_t *read_bytes, int *source_id)
{
	struct popen_data *data = (struct popen_data *)fh;

	if (data == NULL){
		errno = EBADF;
		return -1;
	}

	fd_set rfds;
	FD_ZERO(&rfds);
	ssize_t received = 0;
	int num = 0;

	if (data->fh[STDOUT_FILENO] >= 0) {
		FD_SET(data->fh[STDOUT_FILENO], &rfds);
		++num;
	}
	if (data->fh[STDERR_FILENO] >= 0) {
		FD_SET(data->fh[STDERR_FILENO], &rfds);
		++num;
	}

	if (num == 0) {
		/*
		 * There are no open handles for reading
		 */
		errno = EBADF;
		return -1;
	}

	struct timeval tv = {0,0};
	int max_h = MAX(data->fh[STDOUT_FILENO],
			data->fh[STDERR_FILENO]);

	errno = 0;
	int retv = select(max_h + 1, &rfds, NULL, NULL, &tv);
	switch (retv) {
	case -1:	/* Error */
		return -1;
	case 0:		/* Not ready yet */
		return -2;
	case 1: {        /* One socket is ready */

		/* Choose the socket */
		int fno = STDOUT_FILENO;
		if (!FD_ISSET(data->fh[fno], &rfds))
			fno = STDERR_FILENO;
		if (!FD_ISSET(data->fh[fno], &rfds)) {
			unreachable();
			return -1;
		}

		received = read(data->fh[fno], buf, count);
		if (received < 0)
			goto on_error;
		data->prev_source = fno;
		*read_bytes = received;
		*source_id = fno;
		return 0;
		}
	case 2: {        /* Both sockets are ready */
		received = read(get_handle_in_order(data), buf, count);
		if (received < 0)
			goto on_error;
		*read_bytes = received;
		*source_id = (int)data->prev_source;
		return 0;
		}
	}

	unreachable();
	return -1;

on_error:
	if (errno == EINTR) {
		*read_bytes = 0;
		return -2;	/* Repeat */
	} else
		return -1;	/* Error */
}

int
coio_popen_try_to_write(void *fh, const void *buf, size_t count,
	size_t *written)
{
	if (count == 0)
		return 0;

	struct popen_data *data = (struct popen_data *)fh;

	if (data == NULL){
		errno = EBADF;
		return -1;
	}

	if (data->fh[STDIN_FILENO] < 0) {
		/*
		 * There are no open handles for writing
		 */
		errno = EBADF;
		return -1;
	}

	fd_set wfds;
	FD_ZERO(&wfds);

	int wh = data->fh[STDIN_FILENO];
	FD_SET(wh, &wfds);

	struct timeval tv = {0,0};

	int retv = select(wh + 1, NULL, &wfds, NULL, &tv);
	if (retv < 0)
		goto on_error;
	else if (retv == 0)
		return -2;	/* Not ready yet */

	assert(retv == 1);	/* The socket is ready */

	if (FD_ISSET(wh, &wfds)) {
		ssize_t rc = write(wh, buf, count);
		if (rc < 0)
			goto on_error;
		*written = rc;
		return (*written == count) ? 0
					   : -2;
	}

	unreachable();
	return -1;

on_error:
	if (errno == EINTR) {
		*written = 0;
		return -2;	/* Repeat */
	} else
		return -1;	/* Error */
}