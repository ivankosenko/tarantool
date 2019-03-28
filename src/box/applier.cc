/*
 * Copyright 2010-2016, Tarantool AUTHORS, please see AUTHORS file.
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
#include "applier.h"

#include <msgpuck.h>

#include "xlog.h"
#include "fiber.h"
#include "fiber_cond.h"
#include "coio.h"
#include "coio_buf.h"
#include "wal.h"
#include "xrow.h"
#include "replication.h"
#include "iproto_constants.h"
#include "version.h"
#include "trigger.h"
#include "xrow_io.h"
#include "error.h"
#include "session.h"
#include "cfg.h"
#include "schema.h"
#include "txn.h"
#include "box.h"

STRS(applier_state, applier_STATE);

static inline void
applier_set_state(struct applier *applier, enum applier_state state)
{
	applier->state = state;
	say_debug("=> %s", applier_state_strs[state] +
		  strlen("APPLIER_"));
	trigger_run_xc(&applier->on_state, applier);
}

/**
 * Write a nice error message to log file on SocketError or ClientError
 * in applier_f().
 */
static inline void
applier_log_error(struct applier *applier, struct error *e)
{
	uint32_t errcode = box_error_code(e);
	if (applier->last_logged_errcode == errcode)
		return;
	switch (applier->state) {
	case APPLIER_CONNECT:
		say_info("can't connect to master");
		break;
	case APPLIER_CONNECTED:
	case APPLIER_READY:
		say_info("can't join/subscribe");
		break;
	case APPLIER_AUTH:
		say_info("failed to authenticate");
		break;
	case APPLIER_SYNC:
	case APPLIER_FOLLOW:
	case APPLIER_INITIAL_JOIN:
	case APPLIER_FINAL_JOIN:
		say_info("can't read row");
		break;
	default:
		break;
	}
	error_log(e);
	switch (errcode) {
	case ER_LOADING:
	case ER_CFG:
	case ER_ACCESS_DENIED:
	case ER_NO_SUCH_USER:
	case ER_SYSTEM:
		say_info("will retry every %.2lf second",
			 replication_reconnect_interval());
		break;
	default:
		break;
	}
	applier->last_logged_errcode = errcode;
}

/*
 * Fiber function to write vclock to replication master.
 * To track connection status, replica answers master
 * with encoded vclock. In addition to DML requests,
 * master also sends heartbeat messages every
 * replication_timeout seconds (introduced in 1.7.7).
 * On such requests replica also responds with vclock.
 */
static int
applier_writer_f(va_list ap)
{
	struct applier *applier = va_arg(ap, struct applier *);
	struct ev_io io;
	coio_create(&io, applier->io.fd);

	while (!fiber_is_cancelled()) {
		/*
		 * Tarantool >= 1.7.7 sends periodic heartbeat
		 * messages so we don't need to send ACKs every
		 * replication_timeout seconds any more.
		 */
		if (applier->version_id >= version_id(1, 7, 7))
			fiber_cond_wait_timeout(&applier->writer_cond,
						TIMEOUT_INFINITY);
		else
			fiber_cond_wait_timeout(&applier->writer_cond,
						replication_timeout);
		/* Send ACKs only when in FOLLOW mode ,*/
		if (applier->state != APPLIER_SYNC &&
		    applier->state != APPLIER_FOLLOW)
			continue;
		try {
			struct xrow_header xrow;
			xrow_encode_vclock(&xrow, &replicaset.vclock);
			coio_write_xrow(&io, &xrow);
		} catch (SocketError *e) {
			/*
			 * There is no point trying to send ACKs if
			 * the master closed its end - we would only
			 * spam the log - so exit immediately.
			 */
			if (e->get_errno() == EPIPE)
				break;
			/*
			 * Do not exit, if there is a network error,
			 * the reader fiber will reconnect for us
			 * and signal our cond afterwards.
			 */
			e->log();
		} catch (Exception *e) {
			/*
			 * Out of memory encoding the message, ignore
			 * and try again after an interval.
			 */
			e->log();
		}
		fiber_gc();
	}
	return 0;
}

static int
apply_initial_join_row(struct xrow_header *row)
{
	struct request request;
	xrow_decode_dml(row, &request, dml_request_key_map(row->type));
	struct space *space = space_cache_find_xc(request.space_id);
	/* no access checks here - applier always works with admin privs */
	return space_apply_initial_join_row(space, &request);
}

/**
 * Process a no-op request.
 *
 * A no-op request does not affect any space, but it
 * promotes vclock and is written to WAL.
 */
static int
process_nop(struct request *request)
{
	assert(request->type == IPROTO_NOP);
	struct txn *txn = txn_begin_stmt(NULL);
	if (txn == NULL)
		return -1;
	return txn_commit_stmt(txn, request);
}

static int
apply_row(struct xrow_header *row)
{
	struct request request;
	if (xrow_decode_dml(row, &request, dml_request_key_map(row->type)) != 0)
		return -1;
	if (request.type == IPROTO_NOP)
		return process_nop(&request);
	struct space *space = space_cache_find(request.space_id);
	if (space == NULL)
		return -1;
	if (box_process_rw(&request, space, NULL) != 0) {
		say_error("error applying row: %s", request_str(&request));
		return -1;
	}
	return 0;
}

/**
 * Connect to a remote host and authenticate the client.
 */
void
applier_connect(struct applier *applier)
{
	struct ev_io *coio = &applier->io;
	struct ibuf *ibuf = &applier->ibuf;
	if (coio->fd >= 0)
		return;
	char greetingbuf[IPROTO_GREETING_SIZE];
	struct xrow_header row;

	struct uri *uri = &applier->uri;
	/*
	 * coio_connect() stores resolved address to \a &applier->addr
	 * on success. &applier->addr_len is a value-result argument which
	 * must be initialized to the size of associated buffer (addrstorage)
	 * before calling coio_connect(). Since coio_connect() performs
	 * DNS resolution under the hood it is theoretically possible that
	 * applier->addr_len will be different even for same uri.
	 */
	applier->addr_len = sizeof(applier->addrstorage);
	applier_set_state(applier, APPLIER_CONNECT);
	coio_connect(coio, uri, &applier->addr, &applier->addr_len);
	assert(coio->fd >= 0);
	coio_readn(coio, greetingbuf, IPROTO_GREETING_SIZE);
	applier->last_row_time = ev_monotonic_now(loop());

	/* Decode instance version and name from greeting */
	struct greeting greeting;
	if (greeting_decode(greetingbuf, &greeting) != 0)
		tnt_raise(LoggedError, ER_PROTOCOL, "Invalid greeting");

	if (strcmp(greeting.protocol, "Binary") != 0) {
		tnt_raise(LoggedError, ER_PROTOCOL,
			  "Unsupported protocol for replication");
	}

	if (applier->version_id != greeting.version_id) {
		say_info("remote master %s at %s running Tarantool %u.%u.%u",
			 tt_uuid_str(&greeting.uuid),
			 sio_strfaddr(&applier->addr, applier->addr_len),
			 version_id_major(greeting.version_id),
			 version_id_minor(greeting.version_id),
			 version_id_patch(greeting.version_id));
	}

	/* Save the remote instance version and UUID on connect. */
	applier->uuid = greeting.uuid;
	applier->version_id = greeting.version_id;

	/* Don't display previous error messages in box.info.replication */
	diag_clear(&fiber()->diag);

	/*
	 * Send an IPROTO_VOTE request to fetch the master's ballot
	 * before proceeding to "join". It will be used for leader
	 * election on bootstrap.
	 */
	xrow_encode_vote(&row);
	coio_write_xrow(coio, &row);
	coio_read_xrow(coio, ibuf, &row);
	if (row.type == IPROTO_OK) {
		xrow_decode_ballot_xc(&row, &applier->ballot);
	} else try {
		xrow_decode_error_xc(&row);
	} catch (ClientError *e) {
		if (e->errcode() != ER_UNKNOWN_REQUEST_TYPE)
			e->raise();
		/*
		 * Master isn't aware of IPROTO_VOTE request.
		 * It's OK - we can proceed without it.
		 */
	}

	applier_set_state(applier, APPLIER_CONNECTED);

	/* Detect connection to itself */
	if (tt_uuid_is_equal(&applier->uuid, &INSTANCE_UUID))
		tnt_raise(ClientError, ER_CONNECTION_TO_SELF);

	/* Perform authentication if user provided at least login */
	if (!uri->login)
		goto done;

	/* Authenticate */
	applier_set_state(applier, APPLIER_AUTH);
	xrow_encode_auth_xc(&row, greeting.salt, greeting.salt_len, uri->login,
			    uri->login_len, uri->password, uri->password_len);
	coio_write_xrow(coio, &row);
	coio_read_xrow(coio, ibuf, &row);
	applier->last_row_time = ev_monotonic_now(loop());
	if (row.type != IPROTO_OK)
		xrow_decode_error_xc(&row); /* auth failed */

	/* auth succeeded */
	say_info("authenticated");
done:
	applier_set_state(applier, APPLIER_READY);
}

/**
 * Execute and process JOIN request (bootstrap the instance).
 */
static void
applier_join(struct applier *applier)
{
	/* Send JOIN request */
	struct ev_io *coio = &applier->io;
	struct ibuf *ibuf = &applier->ibuf;
	struct xrow_header row;
	xrow_encode_join_xc(&row, &INSTANCE_UUID);
	coio_write_xrow(coio, &row);

	/**
	 * Tarantool < 1.7.0: if JOIN is successful, there is no "OK"
	 * response, but a stream of rows from checkpoint.
	 */
	if (applier->version_id >= version_id(1, 7, 0)) {
		/* Decode JOIN response */
		coio_read_xrow(coio, ibuf, &row);
		if (iproto_type_is_error(row.type)) {
			xrow_decode_error_xc(&row); /* re-throw error */
		} else if (row.type != IPROTO_OK) {
			tnt_raise(ClientError, ER_UNKNOWN_REQUEST_TYPE,
				  (uint32_t) row.type);
		}
		/*
		 * Start vclock. The vclock of the checkpoint
		 * the master is sending to the replica.
		 * Used to initialize the replica's initial
		 * vclock in bootstrap_from_master()
		 */
		xrow_decode_vclock_xc(&row, &replicaset.vclock);
	}

	applier_set_state(applier, APPLIER_INITIAL_JOIN);

	/*
	 * Receive initial data.
	 */
	uint64_t row_count = 0;
	while (true) {
		coio_read_xrow(coio, ibuf, &row);
		applier->last_row_time = ev_monotonic_now(loop());
		if (iproto_type_is_dml(row.type)) {
			if (apply_initial_join_row(&row) != 0)
				diag_raise();
			if (++row_count % 100000 == 0)
				say_info("%.1fM rows received", row_count / 1e6);
		} else if (row.type == IPROTO_OK) {
			if (applier->version_id < version_id(1, 7, 0)) {
				/*
				 * This is the start vclock if the
				 * server is 1.6. Since we have
				 * not initialized replication
				 * vclock yet, do it now. In 1.7+
				 * this vclock is not used.
				 */
				xrow_decode_vclock_xc(&row, &replicaset.vclock);
			}
			break; /* end of stream */
		} else if (iproto_type_is_error(row.type)) {
			xrow_decode_error_xc(&row);  /* rethrow error */
		} else {
			tnt_raise(ClientError, ER_UNKNOWN_REQUEST_TYPE,
				  (uint32_t) row.type);
		}
	}
	say_info("initial data received");

	applier_set_state(applier, APPLIER_FINAL_JOIN);

	/*
	 * Tarantool < 1.7.0: there is no "final join" stage.
	 * Proceed to "subscribe" and do not finish bootstrap
	 * until replica id is received.
	 */
	if (applier->version_id < version_id(1, 7, 0))
		return;

	/*
	 * Receive final data.
	 */
	while (true) {
		coio_read_xrow(coio, ibuf, &row);
		applier->last_row_time = ev_monotonic_now(loop());
		if (iproto_type_is_dml(row.type)) {
			vclock_follow_xrow(&replicaset.vclock, &row);
			if (apply_row(&row) != 0)
				diag_raise();
			if (++row_count % 100000 == 0)
				say_info("%.1fM rows received", row_count / 1e6);
		} else if (row.type == IPROTO_OK) {
			/*
			 * Current vclock. This is not used now,
			 * ignore.
			 */
			break; /* end of stream */
		} else if (iproto_type_is_error(row.type)) {
			xrow_decode_error_xc(&row);  /* rethrow error */
		} else {
			tnt_raise(ClientError, ER_UNKNOWN_REQUEST_TYPE,
				  (uint32_t) row.type);
		}
	}
	say_info("final data received");

	applier_set_state(applier, APPLIER_JOINED);
	applier_set_state(applier, APPLIER_READY);
}

/**
 * Helper struct to bind rows in a list.
 */
struct applier_tx_row {
	/* Next transaction row. */
	struct stailq_entry next;
	/* xrow_header struct for the current transaction row. */
	struct xrow_header row;
};

static struct applier_tx_row *
applier_read_tx_row(struct applier *applier)
{
	struct ev_io *coio = &applier->io;
	struct ibuf *ibuf = &applier->ibuf;

	struct applier_tx_row *tx_row = (struct applier_tx_row *)
		region_alloc(&fiber()->gc, sizeof(struct applier_tx_row));

	if (tx_row == NULL)
		tnt_raise(OutOfMemory, sizeof(struct applier_tx_row),
			  "region", "struct applier_tx_row");

	struct xrow_header *row = &tx_row->row;

	double timeout = replication_disconnect_timeout();
	/*
	 * Tarantool < 1.7.7 does not send periodic heartbeat
	 * messages so we can't assume that if we haven't heard
	 * from the master for quite a while the connection is
	 * broken - the master might just be idle.
	 */
	if (applier->version_id < version_id(1, 7, 7))
		coio_read_xrow(coio, ibuf, row);
	else
		coio_read_xrow_timeout_xc(coio, ibuf, row, timeout);

	applier->lag = ev_now(loop()) - row->tm;
	applier->last_row_time = ev_monotonic_now(loop());
	return tx_row;
}

/**
 * Read one transaction from network using applier's input buffer.
 * Transaction rows are placed onto fiber gc region.
 * We could not use applier input buffer for that because rpos is adjusted
 * after each xrow decoding and corresponding network input space is going
 * to be reused.
 */
static void
applier_read_tx(struct applier *applier, struct stailq *rows)
{
	int64_t tsn = 0;

	stailq_create(rows);
	do {
		struct applier_tx_row *tx_row = applier_read_tx_row(applier);
		struct xrow_header *row = &tx_row->row;

		if (iproto_type_is_error(row->type))
			xrow_decode_error_xc(row);

		/* Replication request. */
		if (row->replica_id == REPLICA_ID_NIL ||
		    row->replica_id >= VCLOCK_MAX) {
			/*
			 * A safety net, this can only occur
			 * if we're fed a strangely broken xlog.
			 */
			tnt_raise(ClientError, ER_UNKNOWN_REPLICA,
				  int2str(row->replica_id),
				  tt_uuid_str(&REPLICASET_UUID));
		}
		if (tsn == 0) {
			/*
			 * Transaction id must be derived from the log sequence
			 * number of the first row in the transaction.
			 */
			tsn = row->tsn;
			if (row->lsn != tsn)
				tnt_raise(ClientError, ER_PROTOCOL,
					  "Transaction id must be derived from "
					  "the lsn of the first row in the "
					  "transaction.");
		}
		if (tsn != row->tsn)
			tnt_raise(ClientError, ER_UNSUPPORTED,
				  "replication",
				  "interleaving transactions");

		assert(row->bodycnt <= 1);
		if (row->bodycnt == 1) {
			/* Save row body to gc region. */
			void *new_base = region_alloc(&fiber()->gc,
						      row->body->iov_len);
			if (new_base == NULL)
				tnt_raise(OutOfMemory, row->body->iov_len,
					  "region", "xrow body");
			memcpy(new_base, row->body->iov_base, row->body->iov_len);
			/* Adjust row body pointers. */
			row->body->iov_base = new_base;
		}
		stailq_add_tail(rows, &tx_row->next);

	} while (!stailq_last_entry(rows, struct applier_tx_row,
				    next)->row.is_commit);
}

/**
 * Apply all rows in the rows queue as a single transaction.
 *
 * Return 0 for success or -1 in case of an error.
 */
static int
applier_apply_tx(struct stailq *rows, struct txn *txn)
{
	int res = 0;
	struct applier_tx_row *item;
	stailq_foreach_entry(item, rows, next) {
		struct xrow_header *row = &item->row;
		res = apply_row(row);
		if (res != 0) {
			struct error *e = diag_last_error(diag_get());
			/*
			 * In case of ER_TUPLE_FOUND error and enabled
			 * replication_skip_conflict configuration
			 * option, skip applying the foreign row and
			 * replace it with NOP in the local write ahead
			 * log.
			 */
			if (e->type == &type_ClientError &&
			    box_error_code(e) == ER_TUPLE_FOUND &&
			    replication_skip_conflict) {
				diag_clear(diag_get());
				row->type = IPROTO_NOP;
				row->bodycnt = 0;
				res = apply_row(row);
			}
		}
		if (res != 0)
			break;
	}
	if (res == 0) {
		/*
		 * We are going to commit so it's a high time to check if
		 * the current transaction has non-local effects.
		 */
		if (txn_is_distributed(txn)) {
			/*
			 * A transaction mixes remote and local rows and
			 * countn't be replicated back because we don't
			 * support distributed transactions yet.
			 */
			diag_set(ClientError, ER_UNSUPPORTED,
				 "Applier", "distributed transactions");
			return -1;
		}
	}
	return res;
}

static inline void
applier_update_state(struct applier *applier,
		     struct vclock *vclock_at_subscribe)
{
	if (applier->state == APPLIER_SYNC ||
	    applier->state == APPLIER_FOLLOW)
		fiber_cond_signal(&applier->writer_cond);

	/*
	 * Stay 'orphan' until appliers catch up with
	 * the remote vclock at the time of SUBSCRIBE
	 * and the lag is less than configured.
	 */
	if (applier->state == APPLIER_SYNC &&
	    applier->lag <= replication_sync_lag &&
	    vclock_compare(vclock_at_subscribe,
			   &replicaset.vclock) <= 0) {
		/* Applier is synced, switch to "follow". */
		applier_set_state(applier, APPLIER_FOLLOW);
	}
}

/*
 * A structure to serialize transactions from all appliers into one
 * sequential stream and avoid races btween them.
 */
struct sequencer {
	/* Count of workers. */
	int worker_count;
	/* Count of worker fibers in the idle state. */
	int idle_worker_count;
	/* Vclock of the last read transaction. */
	struct vclock net_vclock;
	/* Vclock of the last transaction issued to wal. */
	struct vclock tx_vclock;
	/* Condition fired when a transaction was sent to wal. */
	struct fiber_cond tx_vclock_cond;
	/* List of appliers in reading state. */
	struct rlist network;
	/* List of appliers waiting for worker to be read. */
	struct rlist idle;
	/* Condition fired when there is an applier without reader. */
	struct fiber_cond idle_cond;
	/* shared diagnostic area. */
	struct diag diag;
};

/* An applier connected to a sequencer. */
struct sequencer_client {
	/* rlist anchor. */
	struct rlist list;
	/* Applier reference. */
	struct applier *applier;
	/* True if the applier disconnected from a sequencer. */
	bool done;
	/* Condition fired when the applier is going to be disconnected. */
	struct fiber_cond done_cond;
	/* Diagnostic area. */
	struct diag diag;
	/* Fiber currently reading from the applier socket. */
	struct fiber *listener;
	/* Count of worker processing current applier. */
	uint32_t worker_count;
	/* Master vclock at subscibe time. */
	struct vclock vclock_at_subscribe;
};

/* True is sequencer is in failed state. */
static inline bool
sequencer_is_aborted(struct sequencer *sequencer)
{
	return !diag_is_empty(&sequencer->diag);
}

static inline void
sequencer_abort(struct sequencer *sequencer)
{
	say_error("ABO");
	if (sequencer_is_aborted(sequencer)) {
		diag_clear(&fiber()->diag);
		return;
	}
	diag_move(&fiber()->diag, &sequencer->diag);
	/* Cancel all client that are in network. */
	struct sequencer_client *client;
	rlist_foreach_entry(client, &sequencer->network, list) {
		fiber_cancel(client->listener);
		say_error("CNC");
		}
}

/* True if a sequencer client is in failed state. */
static inline bool
sequencer_client_is_aborted(struct sequencer *sequencer,
			    struct sequencer_client *client)
{
	return !diag_is_empty(&client->diag) || sequencer_is_aborted(sequencer);
}

static inline void
sequencer_client_abort(struct sequencer *sequencer,
		       struct sequencer_client *client,
		       bool force)
{
	if (client->listener != NULL) {
		fiber_cancel(client->listener);
	}
	client->listener = NULL;
	rlist_del(&client->list);
	if (sequencer_client_is_aborted(sequencer, client)) {
		/* Don't override the first known error. */
		diag_clear(&fiber()->diag);
		return;
	}
	if (force)
		/* Abort sequencer. */
		sequencer_abort(sequencer);
	else
		diag_move(&fiber()->diag, &client->diag);
}

static inline void
sequencer_client_check(struct sequencer *sequencer,
		       struct sequencer_client *client)
{
	if (sequencer_client_is_aborted(sequencer, client))
		/* Count not continue processing. */
		tnt_raise(ClientError, ER_TRANSACTION_CONFLICT);
}

/* Detach an applier from a sequencer. */
static void
sequencer_detach(struct sequencer *sequencer, struct sequencer_client *client)
{
	if (diag_is_empty(&client->diag))
		diag_add_error(&client->diag,
			       diag_last_error(&sequencer->diag));
	client->done = true;
	fiber_cond_signal(&client->done_cond);
	if (rlist_empty(&sequencer->idle) &&
	    rlist_empty(&sequencer->network)) {
		/* Sequencer hasn't any connected applier, reset its state. */
		diag_clear(&sequencer->diag);
		vclock_copy(&sequencer->tx_vclock, &replicaset.vclock);
		vclock_copy(&sequencer->net_vclock, &replicaset.vclock);
	}
}

/*
 * Acquire an applier from a sequencers idle list.
 */
static inline struct sequencer_client *
sequencer_get(struct sequencer *sequencer)
{
	if (rlist_empty(&sequencer->idle))
		return NULL;
	struct sequencer_client *client;
	client = rlist_first_entry(&sequencer->idle,
				   struct sequencer_client, list);
	++client->worker_count;
	return client;
}

/*
 * Release an applier.
 */
static inline void
sequencer_put(struct sequencer *sequencer, struct sequencer_client *client)
{
	if (--client->worker_count == 0 &&
	    sequencer_client_is_aborted(sequencer, client))
		/*
		 * Applier is in failed state and there are no workers more
		 * so detach it from the sequencer.
		 */
		sequencer_detach(sequencer, client);
}

/*
 * Attach an applier to a sequencer and wait until
 * the applier was not detached.
 */
static void
sequencer_attach(struct sequencer *sequencer, struct applier *applier,
		 struct vclock *vclock_at_subscribe)
{
	struct sequencer_client client;
	if (sequencer_is_aborted(sequencer)) {
		/*
		 * The sequencer is in failed state, raise an error
		 * immediately.
		 */
		diag_add_error(&fiber()->diag,
			       diag_last_error(&sequencer->diag));
		diag_raise();
	}
	rlist_create(&client.list);
	client.applier = applier;
	client.done = false;
	fiber_cond_create(&client.done_cond);
	diag_create(&client.diag);
	client.listener = NULL;
	client.worker_count = 0;
	vclock_copy(&client.vclock_at_subscribe, vclock_at_subscribe);

	rlist_add_tail(&sequencer->idle, &client.list);
	fiber_cond_signal(&sequencer->idle_cond);
	while (!client.done) {
		fiber_cond_wait(&client.done_cond);
		if (fiber_is_cancelled()) {
			/* Applier is going do be stopped by cfg. */
			if (client.listener != NULL)
				/* Cancel network fiber. */
				fiber_cancel(client.listener);
		}
	}

	if (sequencer_is_aborted(sequencer))
		diag_add_error(&fiber()->diag,
			       diag_last_error(&sequencer->diag));
	else
		diag_move(&client.diag, &fiber()->diag);
	diag_raise();
}

/*
 * Read from applier until a new transaction was read.
 * Return transaction rows and previous lsn value.
 */
static void
sequencer_read_tx(struct sequencer *sequencer,
		  struct sequencer_client *client, struct stailq *rows,
		  int64_t *prev_lsn)
{
	struct applier *applier = client->applier;
	/* Move the client into network list. */
	rlist_move_tail(&sequencer->network, &client->list);
	client->listener = fiber();

	/* Read a transaction from a network. */
restart:
	try {
		applier_read_tx(client->applier, rows);
	} catch (...) {
		client->listener = NULL;
		rlist_del(&client->list);
		throw;
	}
	applier->last_row_time = ev_monotonic_now(loop());
	if (ibuf_used(&applier->ibuf) == 0)
		ibuf_reset(&applier->ibuf);
	sequencer_client_check(sequencer, client);
	struct xrow_header *first_row =
		&stailq_first_entry(rows, struct applier_tx_row,
				    next)->row;
	if (first_row->lsn <= vclock_get(&sequencer->net_vclock,
					 first_row->replica_id)) {
		/*
		 * We already have fetched this transaction, reply with a
		 * status and read the next one.
		 */
		applier_update_state(client->applier,
				     &client->vclock_at_subscribe);
		goto restart;
	}
	/*
	 * Remember a lsn of the previous transaction and follow
	 * network vclock.
	 */
	*prev_lsn = vclock_get(&sequencer->net_vclock, first_row->replica_id);
	vclock_follow(&sequencer->net_vclock, first_row->replica_id,
		      first_row->lsn);

	/* Allow to schedule the next transaction reading. */
	rlist_move_tail(&sequencer->idle, &client->list);
	fiber_cond_signal(&sequencer->idle_cond);
}

/*
 * Wait until the previous transaction was processed and sent to wal then
 * apply the current one.
 */
static void
sequencer_apply_tx(struct sequencer *sequencer,
		   struct sequencer_client *client, struct stailq *rows,
		   int64_t *prev_lsn)
{
	struct xrow_header *first_row =
		&stailq_first_entry(rows, struct applier_tx_row,
				    next)->row;
	/*
	 * We could apply the current transaction only after
	 * the previous one was processed by tx and sent to
	 * the wal.
	 */
	while (vclock_get(&sequencer->tx_vclock,
			  first_row->replica_id) != *prev_lsn) {
		fiber_cond_wait(&sequencer->tx_vclock_cond);
		sequencer_client_check(sequencer, client);
	}
	/*
	 * The previous transaction was sent to wal and it's
	 * a high time to process the current one.
	 */
	struct txn *txn;
	txn = txn_begin(false);
	if (txn == NULL ||
	    applier_apply_tx(rows, txn) != 0 ||
	    txn_prepare(txn) != 0)
		diag_raise();
	/*
	 * We are ready to commit the transaction so
	 * forward tx vclock to allow processing of the next
	 * transaction.
	 */
	vclock_follow(&sequencer->tx_vclock,
		      first_row->replica_id,
		      first_row->lsn);
	fiber_cond_signal(&sequencer->tx_vclock_cond);
	if (txn_commit(txn) != 0)
		diag_raise();
	/* Report local status to the master. */
	if (client->applier->state == APPLIER_SYNC ||
	    client->applier->state == APPLIER_FOLLOW)
		fiber_cond_signal(&client->applier->writer_cond);
}

/*
 * Sequencer worker fiber.
 * This fiber gets an applier from idle list and read one transaction from
 * a network. After networking worker returns the applier into tail of idle list
 * in order to allow reading and processing of further transactions.
 * For failed networking only the current applier is marked as failed and
 * going to be removed from a sequencer.
 * If apply or commit fail then a sequencer has ho chance to continue working
 * because of broken transaction sequence. In that case the sequencer set
 * failed flag and waits until all in-fly transaction processing is finished.
 */
static int
sequencer_f(va_list ap)
{
	struct sequencer *sequencer = va_arg(ap, struct sequencer *);
	++sequencer->worker_count;
	/*
	 * Set correct session type for use in on_replace()
	 * triggers.
	 */
	struct session *session = session_create_on_demand();
	if (session == NULL)
		return -1;
	session_set_type(session, SESSION_TYPE_APPLIER);

	while (!fiber_is_cancelled()) {
		struct sequencer_client *client = sequencer_get(sequencer);
		if (client == NULL) {
			/* Wait for an applier to read from network. */
			++sequencer->idle_worker_count;
			fiber_cond_wait(&sequencer->idle_cond);
			--sequencer->idle_worker_count;
			continue;
		}
		int64_t prev_lsn;
		struct stailq rows;
		bool network = true;
		try {
			sequencer_read_tx(sequencer, client, &rows, &prev_lsn);
			network = false;
			sequencer_apply_tx(sequencer, client, &rows, &prev_lsn);
			applier_update_state(client->applier,
					     &client->vclock_at_subscribe);
		} catch (...) {
			txn_rollback();
			sequencer_client_abort(sequencer, client,
					       network == false);
		}
		sequencer_put(sequencer, client);
	}
	--sequencer->worker_count;
	return 0;
}

/*
 * Sequencer scheduler fiber.
 * The scheduling target is to don't have any applier in the idle state
 * (without network reading worker.
 * It shares the idle condition with workers so it isn't possible to have
 * this condition without a waiter. Also false positives are possible -
 * scheduler might be woken up when there are idle workers. So scheduler just
 * forward fiber_cond_signal in such cases.
 */
static int
sequencer_scheduler_f(va_list ap)
{
	struct sequencer *sequencer = va_arg(ap, struct sequencer *);

	while (!fiber_is_cancelled()) {
		fiber_cond_wait(&sequencer->idle_cond);
		if (rlist_empty(&sequencer->idle))
			/* No idle appliers. */
			continue;

		if (sequencer->idle_worker_count > 0) {
			/* There are more idle workers - wake one of them. */
			fiber_cond_signal(&sequencer->idle_cond);
			fiber_reschedule();
			continue;
		}
		if (sequencer->worker_count < 768) {
			/* Spawn a new worker. */
			struct fiber *f = fiber_new("sequencer", sequencer_f);
			if (f == NULL)
				say_error("Couldn't create sequencer worker");
			else
				fiber_start(f, sequencer);
		}
	}
	return 0;
}

/* Sequencer singleton. */
static struct sequencer sequencer_singleton;
struct sequencer *sequencer = NULL;

/*
 * Create a sequencer.
 */
static void
sequencer_create(struct sequencer *sequencer)
{
	sequencer->worker_count = 0;
	sequencer->idle_worker_count = 0;
	vclock_create(&sequencer->net_vclock);
	vclock_create(&sequencer->tx_vclock);
	fiber_cond_create(&sequencer->tx_vclock_cond);
	rlist_create(&sequencer->network);
	rlist_create(&sequencer->idle);
	fiber_cond_create(&sequencer->idle_cond);
	struct fiber *f = fiber_new_xc("sequencer_scheduler",
				       sequencer_scheduler_f);
	fiber_start(f, sequencer);
}

/**
 * Execute and process SUBSCRIBE request (follow updates from a master).
 */
static void
applier_subscribe(struct applier *applier)
{
	if (sequencer == NULL) {
		/* Initialize the sequencer singleton. */
		sequencer_create(&sequencer_singleton);
		sequencer = &sequencer_singleton;
	}
	/* Send SUBSCRIBE request */
	struct ev_io *coio = &applier->io;
	struct ibuf *ibuf = &applier->ibuf;
	struct xrow_header row;
	struct vclock remote_vclock_at_subscribe;
	struct tt_uuid cluster_id = uuid_nil;

	struct vclock vclock;
	vclock_create(&vclock);
	vclock_copy(&vclock, &replicaset.vclock);
	xrow_encode_subscribe_xc(&row, &REPLICASET_UUID, &INSTANCE_UUID,
				 &vclock);
	coio_write_xrow(coio, &row);

	/* Read SUBSCRIBE response */
	if (applier->version_id >= version_id(1, 6, 7)) {
		coio_read_xrow(coio, ibuf, &row);
		if (iproto_type_is_error(row.type)) {
			xrow_decode_error_xc(&row);  /* error */
		} else if (row.type != IPROTO_OK) {
			tnt_raise(ClientError, ER_PROTOCOL,
				  "Invalid response to SUBSCRIBE");
		}
		/*
		 * In case of successful subscribe, the server
		 * responds with its current vclock.
		 *
		 * Tarantool > 2.1.1 also sends its cluster id to
		 * the replica, and replica has to check whether
		 * its and master's cluster ids match.
		 */
		vclock_create(&remote_vclock_at_subscribe);
		xrow_decode_subscribe_response_xc(&row,
						  &cluster_id,
						  &remote_vclock_at_subscribe);
		/*
		 * If master didn't send us its cluster id
		 * assume that it has done all the checks.
		 * In this case cluster_id will remain zero.
		 */
		if (!tt_uuid_is_nil(&cluster_id) &&
		    !tt_uuid_is_equal(&cluster_id, &REPLICASET_UUID)) {
			tnt_raise(ClientError, ER_REPLICASET_UUID_MISMATCH,
				  tt_uuid_str(&cluster_id),
				  tt_uuid_str(&REPLICASET_UUID));
		}

		say_info("subscribed");
		say_info("remote vclock %s local vclock %s",
			 vclock_to_string(&remote_vclock_at_subscribe),
			 vclock_to_string(&vclock));
	}
	/*
	 * Tarantool < 1.6.7:
	 * If there is an error in subscribe, it's sent directly
	 * in response to subscribe.  If subscribe is successful,
	 * there is no "OK" response, but a stream of rows from
	 * the binary log.
	 */

	if (applier->state == APPLIER_READY) {
		/*
		 * Tarantool < 1.7.7 does not send periodic heartbeat
		 * messages so we cannot enable applier synchronization
		 * for it without risking getting stuck in the 'orphan'
		 * mode until a DML operation happens on the master.
		 */
		if (applier->version_id >= version_id(1, 7, 7))
			applier_set_state(applier, APPLIER_SYNC);
		else
			applier_set_state(applier, APPLIER_FOLLOW);
	} else {
		/*
		 * Tarantool < 1.7.0 sends replica id during
		 * "subscribe" stage. We can't finish bootstrap
		 * until it is received.
		 */
		assert(applier->state == APPLIER_FINAL_JOIN);
		assert(applier->version_id < version_id(1, 7, 0));
	}

	/* Re-enable warnings after successful execution of SUBSCRIBE */
	applier->last_logged_errcode = 0;
	if (applier->version_id >= version_id(1, 7, 4)) {
		/* Enable replication ACKs for newer servers */
		assert(applier->writer == NULL);

		char name[FIBER_NAME_MAX];
		int pos = snprintf(name, sizeof(name), "applierw/");
		uri_format(name + pos, sizeof(name) - pos, &applier->uri, false);

		applier->writer = fiber_new_xc(name, applier_writer_f);
		fiber_set_joinable(applier->writer, true);
		fiber_start(applier->writer, applier);
	}

	applier->lag = TIMEOUT_INFINITY;

	/*
	 * Process a stream of rows from the binary log.
	 */
	while (true) {
		if (applier->state == APPLIER_FINAL_JOIN &&
		    instance_id != REPLICA_ID_NIL) {
			say_info("final data received");
			applier_set_state(applier, APPLIER_JOINED);
			applier_set_state(applier, APPLIER_READY);
			applier_set_state(applier, APPLIER_FOLLOW);
		}

		sequencer_attach(sequencer, applier, &remote_vclock_at_subscribe);
		fiber_gc();
	}
}

static inline void
applier_disconnect(struct applier *applier, enum applier_state state)
{
	applier_set_state(applier, state);
	if (applier->writer != NULL) {
		fiber_cancel(applier->writer);
		fiber_join(applier->writer);
		applier->writer = NULL;
	}

	coio_close(loop(), &applier->io);
	/* Clear all unparsed input. */
	ibuf_reinit(&applier->ibuf);
	fiber_gc();
}

static int
applier_f(va_list ap)
{
	struct applier *applier = va_arg(ap, struct applier *);

	/* Re-connect loop */
	while (!fiber_is_cancelled()) {
		try {
			applier_connect(applier);
			if (tt_uuid_is_nil(&REPLICASET_UUID)) {
				/*
				 * Execute JOIN if this is a bootstrap.
				 * The join will pause the applier
				 * until WAL is created.
				 */
				applier_join(applier);
			}
			applier_subscribe(applier);
			/*
			 * subscribe() has an infinite loop which
			 * is stoppable only with fiber_cancel().
			 */
			unreachable();
			return 0;
		} catch (ClientError *e) {
			if (e->errcode() == ER_CONNECTION_TO_SELF &&
			    tt_uuid_is_equal(&applier->uuid, &INSTANCE_UUID)) {
				/* Connection to itself, stop applier */
				applier_disconnect(applier, APPLIER_OFF);
				return 0;
			} else if (e->errcode() == ER_LOADING) {
				/* Autobootstrap */
				applier_log_error(applier, e);
				applier_disconnect(applier, APPLIER_LOADING);
				goto reconnect;
			} else if (e->errcode() == ER_CFG ||
				   e->errcode() == ER_ACCESS_DENIED ||
				   e->errcode() == ER_NO_SUCH_USER) {
				/* Invalid configuration */
				applier_log_error(applier, e);
				applier_disconnect(applier, APPLIER_LOADING);
				goto reconnect;
			} else if (e->errcode() == ER_SYSTEM) {
				/* System error from master instance. */
				applier_log_error(applier, e);
				applier_disconnect(applier, APPLIER_DISCONNECTED);
				goto reconnect;
			} else {
				/* Unrecoverable errors */
				applier_log_error(applier, e);
				applier_disconnect(applier, APPLIER_STOPPED);
				return -1;
			}
		} catch (FiberIsCancelled *e) {
			applier_disconnect(applier, APPLIER_OFF);
			break;
		} catch (SocketError *e) {
			applier_log_error(applier, e);
			applier_disconnect(applier, APPLIER_DISCONNECTED);
			goto reconnect;
		} catch (SystemError *e) {
			applier_log_error(applier, e);
			applier_disconnect(applier, APPLIER_DISCONNECTED);
			goto reconnect;
		} catch (Exception *e) {
			applier_log_error(applier, e);
			applier_disconnect(applier, APPLIER_STOPPED);
			return -1;
		}
		/* Put fiber_sleep() out of catch block.
		 *
		 * This is done to avoid the case when two or more
		 * fibers yield inside their try/catch blocks and
		 * throw an exception. Seems like the exception unwinder
		 * uses global state inside the catch block.
		 *
		 * This could lead to incorrect exception processing
		 * and crash the program.
		 *
		 * See: https://github.com/tarantool/tarantool/issues/136
		*/
reconnect:
		fiber_sleep(replication_reconnect_interval());
	}
	return 0;
}

void
applier_start(struct applier *applier)
{
	char name[FIBER_NAME_MAX];
	assert(applier->reader == NULL);

	int pos = snprintf(name, sizeof(name), "applier/");
	uri_format(name + pos, sizeof(name) - pos, &applier->uri, false);

	struct fiber *f = fiber_new_xc(name, applier_f);
	/**
	 * So that we can safely grab the status of the
	 * fiber any time we want.
	 */
	fiber_set_joinable(f, true);
	applier->reader = f;
	fiber_start(f, applier);
}

void
applier_stop(struct applier *applier)
{
	struct fiber *f = applier->reader;
	if (f == NULL)
		return;
	fiber_cancel(f);
	fiber_join(f);
	applier_set_state(applier, APPLIER_OFF);
	applier->reader = NULL;
}

struct applier *
applier_new(const char *uri)
{
	struct applier *applier = (struct applier *)
		calloc(1, sizeof(struct applier));
	if (applier == NULL) {
		diag_set(OutOfMemory, sizeof(*applier), "malloc",
			 "struct applier");
		return NULL;
	}
	coio_create(&applier->io, -1);
	ibuf_create(&applier->ibuf, &cord()->slabc, 1024);

	/* uri_parse() sets pointers to applier->source buffer */
	snprintf(applier->source, sizeof(applier->source), "%s", uri);
	int rc = uri_parse(&applier->uri, applier->source);
	/* URI checked by box_check_replication() */
	assert(rc == 0 && applier->uri.service != NULL);
	(void) rc;

	applier->last_row_time = ev_monotonic_now(loop());
	rlist_create(&applier->on_state);
	fiber_cond_create(&applier->resume_cond);
	fiber_cond_create(&applier->writer_cond);

	return applier;
}

void
applier_delete(struct applier *applier)
{
	assert(applier->reader == NULL && applier->writer == NULL);
	ibuf_destroy(&applier->ibuf);
	assert(applier->io.fd == -1);
	trigger_destroy(&applier->on_state);
	fiber_cond_destroy(&applier->resume_cond);
	fiber_cond_destroy(&applier->writer_cond);
	free(applier);
}

void
applier_resume(struct applier *applier)
{
	assert(!fiber_is_dead(applier->reader));
	applier->is_paused = false;
	fiber_cond_signal(&applier->resume_cond);
}

void
applier_pause(struct applier *applier)
{
	/* Sleep until applier_resume() wake us up */
	assert(fiber() == applier->reader);
	assert(!applier->is_paused);
	applier->is_paused = true;
	while (applier->is_paused && !fiber_is_cancelled())
		fiber_cond_wait(&applier->resume_cond);
}

struct applier_on_state {
	struct trigger base;
	struct applier *applier;
	enum applier_state desired_state;
	struct fiber_cond wakeup;
};

static void
applier_on_state_f(struct trigger *trigger, void *event)
{
	(void) event;
	struct applier_on_state *on_state =
		container_of(trigger, struct applier_on_state, base);

	struct applier *applier = on_state->applier;

	if (applier->state != APPLIER_OFF &&
	    applier->state != APPLIER_STOPPED &&
	    applier->state != on_state->desired_state)
		return;

	/* Wake up waiter */
	fiber_cond_signal(&on_state->wakeup);

	applier_pause(applier);
}

static inline void
applier_add_on_state(struct applier *applier,
		     struct applier_on_state *trigger,
		     enum applier_state desired_state)
{
	trigger_create(&trigger->base, applier_on_state_f, NULL, NULL);
	trigger->applier = applier;
	fiber_cond_create(&trigger->wakeup);
	trigger->desired_state = desired_state;
	trigger_add(&applier->on_state, &trigger->base);
}

static inline void
applier_clear_on_state(struct applier_on_state *trigger)
{
	fiber_cond_destroy(&trigger->wakeup);
	trigger_clear(&trigger->base);
}

static inline int
applier_wait_for_state(struct applier_on_state *trigger, double timeout)
{
	struct applier *applier = trigger->applier;
	double deadline = ev_monotonic_now(loop()) + timeout;
	while (applier->state != APPLIER_OFF &&
	       applier->state != APPLIER_STOPPED &&
	       applier->state != trigger->desired_state) {
		if (fiber_cond_wait_deadline(&trigger->wakeup, deadline) != 0)
			return -1; /* ER_TIMEOUT */
	}
	if (applier->state != trigger->desired_state) {
		assert(applier->state == APPLIER_OFF ||
		       applier->state == APPLIER_STOPPED);
		/* Re-throw the original error */
		assert(!diag_is_empty(&applier->reader->diag));
		diag_move(&applier->reader->diag, &fiber()->diag);
		return -1;
	}
	return 0;
}

void
applier_resume_to_state(struct applier *applier, enum applier_state state,
			double timeout)
{
	struct applier_on_state trigger;
	applier_add_on_state(applier, &trigger, state);
	applier_resume(applier);
	int rc = applier_wait_for_state(&trigger, timeout);
	applier_clear_on_state(&trigger);
	if (rc != 0)
		diag_raise();
	assert(applier->state == state);
}
