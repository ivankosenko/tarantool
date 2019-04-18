#ifndef TARANTOOL_LIB_CORE_COIO_POPEN_H_INCLUDED
#define TARANTOOL_LIB_CORE_COIO_POPEN_H_INCLUDED
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

#if defined(__cplusplus)
extern "C" {
#endif /* defined(__cplusplus) */

/**
 * Implementation of fio.popen.
 * The function opens a process by creating a pipe,
 * forking, and invoking the shell.
 *
 * @param command pointer to a null-terminated string
 * containing a shell command line.
 * This command is passed to /bin/sh using the -c flag;
 *
 * @param type pointer to a null-terminated string
 * which must contain either the letter 'r' for reading
 * or the letter 'w' for writing.
 *
 * @return handle of the pipe for reading or writing
 * (depends on value of type).
 * In a case of error returns NULL.
 */
void *
coio_popen_impl(const char *command, const char *type);

/**
 * Implementation of fio.pclose.
 * The function tries to retrieve status of
 * the associated process.
 * If the associated process is terminated then releases
 * allocated resources.
 * If the associated process is still running the function
 * returns immediately. In this case repeat the call.
 *
 * @param fh handle returned by fio.popen.
 *
 * @return 0 if the process is terminated
 * @return -1 for an error
 * @return -2 if the process is still running
 */
int
coio_try_pclose_impl(void *fh);

/**
 * The function reads up to count bytes from the handle
 * associated with the child process.
 * Returns immediately
 *
 * @param fd handle returned by fio.popen.
 * @param buf a buffer to be read into
 * @param count size of buffer in bytes
 * @param read_bytes A pointer to the
 * variable that receives the number of bytes read.
 * @param source_id A pointer to the variable that receives a
 * source stream id, 1 - for STDOUT, 2 - for STDERR.
 *
 * @return 0 data were successfully read
 * @return -1 an error occurred, see errno for error code
 * @return -2 there is nothing to read yet
 */
int
coio_popen_try_to_read(void *fh, void *buf, size_t count,
	size_t *read_bytes, int *source_id);

/**
 * The function writes up to count bytes to the handle
 * associated with the child process.
 * Tries to write as much as possible without blocking
 * and immediately returns.
 *
 * @param fd handle returned by fio.popen.
 * @param buf a buffer to be written from
 * @param count size of buffer in bytes
 * @param written A pointer to the
 * variable that receives the number of bytes actually written.
 * If function fails the number of written bytes is undefined.
 *
 * @return 0 all data were successfully written
 * @return -1 an error occurred, see errno for error code
 * @return -2 the writing can block
 */
int
coio_popen_try_to_write(void *fh, const void *buf, size_t count,
	size_t *written);


#if defined(__cplusplus)
} /* extern "C" */
#endif /* defined(__cplusplus) */

#endif /* TARANTOOL_LIB_CORE_COIO_POPEN_H_INCLUDED */
