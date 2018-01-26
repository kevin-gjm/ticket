/**
 * Copyright (c) 2015, Willem-Hendrik Thiart
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 */

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>

#include "lmdb.h"
#include "lmdb_helpers.h"
#include "raft.h"
#include "uv_helpers.h"
#include "uv_multiplex.h"
#include "arraytools.h"

#include "parse_msg.h"
#include "def.h"
#include "zmq_server.h"
#include "sql_store.h"
#include "usage.h"



options_t opts;
options_t re_opts; //load from db

server_t server;
server_t *sv = &server;
int g_exit=0;



static peer_connection_t* __new_connection(server_t* sv);
static void __connect_to_peer(peer_connection_t* conn);
static void __connection_set_peer(peer_connection_t* conn, char* host, int port);
static void __connect_to_peer_at_host(peer_connection_t* conn, char* host, int port);
static void __start_raft_periodic_timer(server_t* sv);
static int __send_handshake_response(peer_connection_t* conn,
                                     handshake_state_e success,
                                     raft_node_t* leader);
static int __send_leave_response(peer_connection_t* conn);

static void __drop_db(server_t* sv);


static void __peer_msg_send(uv_stream_t* s, msg_t* msg)
{
	uv_buf_t bufs[1];
	struct pbc_wmessage* msg_pb = parse_msg_write(g_pb_env, msg);

	struct pbc_slice slice;

	pbc_wmessage_buffer(msg_pb, &slice);

	bufs[0].base = slice.buffer;
	bufs[0].len = slice.len;
    int e = uv_try_write(s, bufs, 1);
	
	pbc_wmessage_delete(msg_pb);
	
    if (e < 0)
        uv_fatal(e);
}



/** Initiate connection if we are disconnected */
static int __connect_if_needed(peer_connection_t* conn)
{
    if (CONNECTED != conn->connection_status)
    {
        if (DISCONNECTED == conn->connection_status)
            __connect_to_peer(conn);
        return -1;
    }
    return 0;
}

/** Raft callback for sending request vote message */
static int __raft_send_requestvote(
    raft_server_t* raft,
    void *user_data,
    raft_node_t *node,
    msg_requestvote_t* m
    )
{
    peer_connection_t* conn = raft_node_get_udata(node);

    int e = __connect_if_needed(conn);
    if (-1 == e)
        return 0;

    uv_buf_t bufs[1];
    char buf[RAFT_BUFLEN];
    msg_t msg = {};
    msg.type = MSG_REQUESTVOTE,
    msg.rv = *m;
    __peer_msg_send(conn->stream, &msg);
    return 0;
}

/** Raft callback for sending appendentries message */
static int __raft_send_appendentries(
    raft_server_t* raft,
    void *user_data,
    raft_node_t *node,
    msg_appendentries_t* m
    )
{
    uv_buf_t bufs[1];
    peer_connection_t* conn = raft_node_get_udata(node);

    int e = __connect_if_needed(conn);
    if (-1 == e)
        return 0;

    char buf[RAFT_BUFLEN], *ptr = buf;
    msg_t msg = {};
    msg.type = MSG_APPENDENTRIES;
	msg.ae = *m;
    msg.ae.term = m->term;
    msg.ae.prev_log_idx   = m->prev_log_idx;
    msg.ae.prev_log_term = m->prev_log_term;
    msg.ae.leader_commit = m->leader_commit;
    msg.ae.n_entries = m->n_entries;

	 __peer_msg_send(conn->stream, &msg);

    return 0;
}

static void __delete_connection(server_t* sv, peer_connection_t* conn)
{
    peer_connection_t* prev = NULL;
    if (sv->conns == conn)
        sv->conns = conn->next;
    else if (sv->conns != conn)
    {
        for (prev = sv->conns; prev->next != conn; prev = prev->next);
        prev->next = conn->next;
    }
    else
        assert(0);

    if (conn->node)
        raft_node_set_udata(conn->node, NULL);

    // TODO: make sure all resources are freed
    free(conn->connect);
	free(conn->stream);
    free(conn);
}

static peer_connection_t* __find_connection(server_t* sv, const char* host, int raft_port)
{
    peer_connection_t* conn;
    for (conn = sv->conns;
         conn && (0 != strcmp(host, inet_ntoa(conn->addr.sin_addr)) ||
                  conn->raft_port != raft_port);
         conn = conn->next)
        ;
    return conn;
}

static int __offer_cfg_change(server_t* sv,
                              raft_server_t* raft,
                              const unsigned char* data,
                              raft_logtype_e change_type)
{
    entry_cfg_change_t *change = (void*)data;
    peer_connection_t* conn = __find_connection(sv, change->host, change->raft_port);

    /* Node is being removed */
    if (RAFT_LOGTYPE_REMOVE_NODE == change_type)
    {
        raft_remove_node(raft, raft_get_node(sv->raft, change->node_id));
        if (conn)
        {   //__delete_connection(sv, conn); 
        	conn->node = NULL;
			printf("delete node id %d\n",change->node_id);
		}
        return 0;
    }

    /* Node is being added */
    if (!conn)
    {
        conn = __new_connection(sv);
        __connection_set_peer(conn, change->host, change->raft_port);
    }
    conn->server_port = change->server_port;

    int is_self = change->node_id == sv->node_id;

    switch (change_type)
    {
        case RAFT_LOGTYPE_ADD_NONVOTING_NODE:
            conn->node = raft_add_non_voting_node(raft, conn, change->node_id, is_self);
			printf("add no vote node:%x\n",conn->node);
            break;
        case RAFT_LOGTYPE_ADD_NODE:
            conn->node = raft_add_node(raft, conn, change->node_id, is_self);
			printf("add node:%x\n",conn->node);
        break;
        default:
            assert(0);
    }

    raft_node_set_udata(conn->node, conn);

    return 0;
}

/** Raft callback for applying an entry to the finite state machine */

int __raft_applylog(
    raft_server_t* raft,
    void *udata,
    raft_entry_t *ety
    )
{
	unsigned long last_apply;

    MDB_txn *txn;

    MDB_val key = { .mv_size = ety->data.len, .mv_data = ety->data.buf };
    MDB_val val = { .mv_size = 0, .mv_data = "\0" };

    int e = mdb_txn_begin(sv->db_env, NULL, 0, &txn);
    if (0 != e)
        mdb_fatal(e);

    /* Check if it's a configuration change */
    if (raft_entry_is_cfg_change(ety))
    {
        entry_cfg_change_t *change = ety->data.buf;
        if (RAFT_LOGTYPE_REMOVE_NODE != ety->type || !raft_is_leader(sv->raft))
            goto commit;

        peer_connection_t* conn = __find_connection(sv, change->host, change->raft_port);
        __send_leave_response(conn);
        goto commit;
    }
	zmq_server_msg_t msg = {0};
	struct pbc_slice slice;
	slice.buffer = ety->data.buf;
	slice.len = ety->data.len;
	parse_zmq_msg_read(g_pb_zmq_env,&slice,&msg);
	printf("!!!!!!!!!!!! sql:%s\n",msg.req.nqry.sql);

	store_exec(msg.req.nqry.sql,msg.req.nqry.use_tran);
	free_zmq_msg_mem_if_needed(&msg);


    
commit:
	
	/* This log affects the ticketd state machine */
	last_apply = raft_get_last_applied_idx(sv->raft);
	key.mv_data = "last_applied_idx";
	key.mv_size = strlen("last_applied_idx");
	val.mv_data = &last_apply;
	val.mv_size = sizeof(last_apply);
	e = mdb_put(txn, sv->state, &key, &val, 0);
	switch (e)
	{
	case 0:
		break;
	case MDB_MAP_FULL:
	{
		mdb_txn_abort(txn);
		return -1;
	}
	default:
		mdb_fatal(e);
	}
    /* We save the commit idx for performance reasons.
     * Note that Raft doesn't require this as it can figure it out itself. */
    e = mdb_puts_ulong(txn, sv->state, "commit_idx", raft_get_commit_idx(raft));

    e = mdb_txn_commit(txn);
    if (0 != e)
        mdb_fatal(e);

    return 0;
}
	


/** Raft callback for saving term field to disk.
 * This only returns when change has been made to disk. */
static int __raft_persist_term(
    raft_server_t* raft,
    void *udata,
    const unsigned long current_term
    )
{
	printf("store term %lu\n",current_term);
    return mdb_puts_ulong_commit(sv->db_env, sv->state, "term", current_term);
}

/** Raft callback for saving voted_for field to disk.
 * This only returns when change has been made to disk. */
static int __raft_persist_vote(
    raft_server_t* raft,
    void *udata,
    const int voted_for
    )
{
    return mdb_puts_int_commit(sv->db_env, sv->state, "voted_for", voted_for);
}

static void __peer_alloc_cb(uv_handle_t* handle, size_t size, uv_buf_t* buf)
{
    buf->len = size;
    buf->base = malloc(size);
}

static int __append_cfg_change(server_t* sv,
                               raft_logtype_e change_type,
                               char* host,
                               int raft_port, int server_port,
                               int node_id)
{
    entry_cfg_change_t *change = calloc(1, sizeof(*change));
    change->raft_port = raft_port;
    change->server_port = server_port;
    change->node_id = node_id;
    strcpy(change->host, host);
    change->host[IP_STR_LEN - 1] = 0;

    msg_entry_t entry;
    entry.id = rand();
    entry.data.buf = (void*)change;
    entry.data.len = sizeof(*change);
    entry.type = change_type;
    msg_entry_response_t r;
    int e = raft_recv_entry(sv->raft, &entry, &r);
	free(change);
    if (0 != e)
        return -1;
    return 0;
}



/** Parse raft peer traffic using binary protocol, and respond to message */
static int __handle_msg(msg_t * recv_msg, peer_connection_t* conn)
{
    int e;

    switch (recv_msg->type)
    {
    case MSG_HANDSHAKE:
    {

		printf("recv handshake\n");
        peer_connection_t* nconn = __find_connection(
            sv, inet_ntoa(conn->addr.sin_addr), recv_msg->hs.raft_port);
        if (nconn && conn != nconn)
            __delete_connection(sv, nconn);

        conn->connection_status = CONNECTED;
        conn->server_port = recv_msg->hs.server_port;
        conn->raft_port = recv_msg->hs.raft_port;

        raft_node_t* leader = raft_get_current_leader_node(sv->raft);

        /* Is this peer in our configuration already? */
        raft_node_t* node = raft_get_node(sv->raft, recv_msg->hs.node_id);
        if (node)
        {
        	printf("already in config\n");
            raft_node_set_udata(node, conn);
            conn->node = node;
        }

        if (!leader)
        {
            return __send_handshake_response(conn, HANDSHAKE_FAILURE, NULL);
        }
        else if (raft_node_get_id(leader) != sv->node_id)
        {
            return __send_handshake_response(conn, HANDSHAKE_FAILURE, leader);
        }
        else if (node)
        {
        	printf("i'm leader and have find this node in config\n");
            return __send_handshake_response(conn, HANDSHAKE_SUCCESS, NULL);
        }
        else
        {
        	printf("i'm leader not found in config then add no vote node\n");
            int e = __append_cfg_change(sv, RAFT_LOGTYPE_ADD_NONVOTING_NODE,
                                       inet_ntoa(conn->addr.sin_addr),
                                       recv_msg->hs.raft_port, recv_msg->hs.server_port,
                                       recv_msg->hs.node_id);
            if (0 != e)
                return __send_handshake_response(conn, HANDSHAKE_FAILURE, NULL);
            return __send_handshake_response(conn, HANDSHAKE_SUCCESS, NULL);
        }
    }
    break;
    case MSG_HANDSHAKE_RESPONSE:
        if (HANDSHAKE_FAILURE == recv_msg->hsr.success)
        {
            conn->server_port = recv_msg->hsr.server_port;

            /* We're being redirected to the leader */
            if (recv_msg->hsr.leader_port)
            {
                peer_connection_t* nconn =
                    __find_connection(sv, recv_msg->hsr.leader_host, recv_msg->hsr.leader_port);
                if (!nconn)
                {
                    nconn = __new_connection(sv);
                    printf("Redirecting to %s:%d...\n",
                        recv_msg->hsr.leader_host, recv_msg->hsr.leader_port);
                    __connect_to_peer_at_host(nconn, recv_msg->hsr.leader_host,
                                              recv_msg->hsr.leader_port);
                }
            }
        }
        else
        {
            printf("Connected to leader: %s:%d\n",
                 inet_ntoa(conn->addr.sin_addr), conn->raft_port);
            if (!conn->node)
            {
                conn->node = raft_get_node(sv->raft, recv_msg->hsr.node_id);
				printf("not find this node and get it from node list,node:%x\n",conn->node);
            }
        }
        break;
    case MSG_LEAVE:
        {
        if (!conn->node)
        {
            printf("ERROR: no node\n");
            return 0;
        }
        int e = __append_cfg_change(sv, RAFT_LOGTYPE_REMOVE_NODE,
                                inet_ntoa(conn->addr.sin_addr),
                                conn->raft_port,
                                conn->server_port,
                                raft_node_get_id(conn->node));
        if (0 != e)
            printf("ERROR: Leave request failed\n");
        }
        break;
    case MSG_LEAVE_RESPONSE:
       // __drop_db(sv);
		g_exit=1;
        printf("Shutdown complete. Quitting...\n");
        exit(0);
        break;
    case MSG_REQUESTVOTE:
    {
        msg_t msg = { .type = MSG_REQUESTVOTE_RESPONSE };
        e = raft_recv_requestvote(sv->raft, conn->node, &(recv_msg->rv), &msg.rvr);
        __peer_msg_send(conn->stream, &msg);
    }
    break;
    case MSG_REQUESTVOTE_RESPONSE:
        e = raft_recv_requestvote_response(sv->raft, conn->node, &(recv_msg->rvr));
        break;
    case MSG_APPENDENTRIES:
    	{
        msg_t msg = { .type = MSG_APPENDENTRIES_RESPONSE };
        e = raft_recv_appendentries(sv->raft, conn->node, &(recv_msg->ae), &msg.aer);

        /* send response */
        __peer_msg_send(conn->stream, &msg);

        return 0;
       
    	}
        break;
    case MSG_APPENDENTRIES_RESPONSE:
        e = raft_recv_appendentries_response(sv->raft, conn->node, &(recv_msg->aer));
        uv_cond_signal(&sv->appendentries_received);
        break;
    default:
        printf("unknown msg\n");
        exit(0);
    }
    return 0;
}

/** Read raft traffic using binary protocol */
static void __peer_read_cb(uv_stream_t* tcp, ssize_t nread, const uv_buf_t* buf)
{
    peer_connection_t* conn = tcp->data;

    if (nread < 0)
        switch (nread)
        {
        case UV__ECONNRESET:
        case UV__EOF:
            conn->connection_status = DISCONNECTED;
			if(buf->base)
				free(buf->base);
            return;
        default:
            uv_fatal(nread);
        }

    if (0 <= nread)
    {
        assert(conn);
		
        uv_mutex_lock(&sv->raft_lock);

		msg_t msg = {0};
		msg_entry_t entry = {0};
		struct pbc_slice slice;

		slice.buffer = buf->base;
		slice.len = nread;
		
		int ret =  parse_msg_read(g_pb_env, &slice,&msg,&entry);
		
		if(ret == 0)
		{
			__handle_msg(&msg, conn);
		}
		else
		{
			printf("error parse msg!\n");
		}

		if(msg.type == MSG_APPENDENTRIES && entry.data.len>0)
		{
			free(entry.data.buf);
		}
		
        uv_mutex_unlock(&sv->raft_lock);
    }

	if( buf->base)
		free(buf->base);
}

static void __send_leave(peer_connection_t* conn)
{
    msg_t msg = {};
    msg.type = MSG_LEAVE;
    __peer_msg_send(conn->stream,&msg);
}

static void __send_handshake(peer_connection_t* conn)
{
   
    msg_t msg = {};
    msg.type = MSG_HANDSHAKE;
    msg.hs.raft_port = opts.raft_port;
    msg.hs.server_port = opts.server_port;
    msg.hs.node_id = sv->node_id;
	printf("send handshake with id %d\n",sv->node_id);
    __peer_msg_send(conn->stream, &msg);
}

static int __send_leave_response(peer_connection_t* conn)
{
    if (!conn)
    {
        printf("no connection??\n");
        return -1;
    }
    if (!conn->stream)
        return -1;
   
    msg_t msg = {};
    msg.type = MSG_LEAVE_RESPONSE;
    __peer_msg_send(conn->stream, &msg);
    return 0;
}

static int __send_handshake_response(peer_connection_t* conn,
                                     handshake_state_e success,
                                     raft_node_t* leader)
{
    uv_buf_t bufs[1];
    char buf[RAFT_BUFLEN];

    msg_t msg = {};
    msg.type = MSG_HANDSHAKE_RESPONSE;
    msg.hsr.success = success;
    msg.hsr.leader_port = 0;
    msg.hsr.node_id = sv->node_id;

    /* allow the peer to redirect to the leader */
    if (leader)
    {
        peer_connection_t* leader_conn = raft_node_get_udata(leader);
        if (leader_conn)
        {
            msg.hsr.leader_port = leader_conn->raft_port;
            snprintf(msg.hsr.leader_host, IP_STR_LEN, "%s",
                     inet_ntoa(leader_conn->addr.sin_addr));
        }
    }

    msg.hsr.server_port = opts.server_port;

    __peer_msg_send(conn->stream, &msg);

    return 0;
}

/** Raft peer has connected to us.
* Add them to our list of nodes */
static void __on_peer_connection(uv_stream_t *listener, const int status)
{
    int e;

    if (0 != status)
        uv_fatal(status);

    uv_tcp_t *tcp = calloc(1, sizeof(uv_tcp_t));
    e = uv_tcp_init(listener->loop, tcp);
    if (0 != e)
        uv_fatal(e);

    e = uv_accept(listener, (uv_stream_t*)tcp);
    if (0 != e)
        uv_fatal(e);

    peer_connection_t* conn = __new_connection(sv);
    conn->node = NULL;
    conn->loop = listener->loop;
    conn->stream = (uv_stream_t*)tcp;
    tcp->data = conn;

    int namelen = sizeof(conn->addr);
    e = uv_tcp_getpeername(tcp, (struct sockaddr*)&conn->addr, &namelen);
    if (0 != e)
        uv_fatal(e);

    e = uv_read_start((uv_stream_t*)tcp, __peer_alloc_cb, __peer_read_cb);
    if (0 != e)
        uv_fatal(e);
}

/** Our connection attempt to raft peer has succeeded */
static void __on_connection_accepted_by_peer(uv_connect_t *req,
                                             const int status)
{
    peer_connection_t* conn = req->data;
    int e;

    switch (status)
    {
    case 0:
        break;
    case -ECONNREFUSED:
        return;
    default:
        uv_fatal(status);
    }

    __send_handshake(conn);

    int nlen = sizeof(conn->addr);
    e = uv_tcp_getpeername((uv_tcp_t*)req->handle, (struct sockaddr*)&conn->addr, &nlen);
    if (0 != e)
        uv_fatal(e);

    /* start reading from peer */
    conn->connection_status = CONNECTED;
    e = uv_read_start(conn->stream, __peer_alloc_cb, __peer_read_cb);
    if (0 != e)
        uv_fatal(e);
}

static peer_connection_t* __new_connection(server_t* sv)
{
    peer_connection_t* conn = calloc(1, sizeof(peer_connection_t));
    conn->loop = &sv->peer_loop;
    conn->next = sv->conns;
    sv->conns = conn;
    return conn;
}

/** Connect to raft peer */
static void __connect_to_peer(peer_connection_t* conn)
{
	printf("connect to peer\n\n");
    int e;

    uv_tcp_t *tcp = calloc(1, sizeof(uv_tcp_t));
    tcp->data = conn;
    e = uv_tcp_init(conn->loop, tcp);
    if (0 != e)
        uv_fatal(e);

    conn->stream = (uv_stream_t*)tcp;
    conn->connection_status = CONNECTING;

    uv_connect_t *c = calloc(1, sizeof(uv_connect_t));
    c->data = conn;
	conn->connect = c;

    e = uv_tcp_connect(c, (uv_tcp_t*)conn->stream,
                       (struct sockaddr*)&conn->addr,
                       __on_connection_accepted_by_peer);
    if (0 != e)
        uv_fatal(e);
}

static void __connection_set_peer(peer_connection_t* conn, char* host, int port)
{
    conn->raft_port = port;
    printf("Connecting to %s:%d\n", host, port);
    int e = uv_ip4_addr(host, port, &conn->addr);
    if (0 != e)
        uv_fatal(e);
}

static void __connect_to_peer_at_host(peer_connection_t* conn, char* host,
                                      int port)
{
    __connection_set_peer(conn, host, port);
    __connect_to_peer(conn);
}

/** Raft callback for displaying debugging information */
void __raft_log(raft_server_t* raft, raft_node_t* node, void *udata,
                const char *buf)
{
    if (opts.debug)
        printf("raft: %s\n", buf);
}

/** Raft callback for appending an item to the log */
static int __raft_logentry_offer(
    raft_server_t* raft,
    void *udata,
    raft_entry_t *ety,
    unsigned long ety_idx
    )
{
	unsigned long  total_count= ety_idx+1; //ety_idx start from 0
    MDB_txn *txn;

    if (raft_entry_is_cfg_change(ety))
    {
    	printf("offer cfg change!\n");
        __offer_cfg_change(sv, raft, ety->data.buf, ety->type);
    }
	
    int e = mdb_txn_begin(sv->db_env, NULL, 0, &txn);
    if (0 != e)
        mdb_fatal(e);

	struct pbc_wmessage* pb_ety = parse_entry_control_write(g_pb_env,ety);

	struct pbc_slice slice;
	
	pbc_wmessage_buffer(pb_ety, &slice);
		

    /* 1. put metadata */
    ety_idx <<= 1;
    MDB_val key = { .mv_size = sizeof(ety_idx), .mv_data = (void*)&ety_idx };
    MDB_val val = { .mv_size = slice.len, .mv_data = slice.buffer };

    e = mdb_put(txn, sv->entries, &key, &val, 0);

	pbc_wmessage_delete(pb_ety);
	
    switch (e)
    {
    case 0:
        break;
    case MDB_MAP_FULL:
    {
        mdb_txn_abort(txn);
        return -1;
    }
    default:
        mdb_fatal(e);
    }

    /* 2. put entry */
    ety_idx |= 1;
    key.mv_size = sizeof(ety_idx);
    key.mv_data = (void*)&ety_idx;
    val.mv_size = ety->data.len;
    val.mv_data = ety->data.buf;

    e = mdb_put(txn, sv->entries, &key, &val, 0);
    switch (e)
    {
    case 0:
        break;
    case MDB_MAP_FULL:
    {
        mdb_txn_abort(txn);
        return -1;
    }
    default:
        mdb_fatal(e);
    }

    e = mdb_txn_commit(txn);
    if (0 != e)
        mdb_fatal(e);

	/* 3. store log info*/ 

	key.mv_size = strlen("log_total_count");
	key.mv_data = "log_total_count";
	val.mv_size = sizeof(total_count);
	val.mv_data = &total_count;

    e = mdb_txn_begin(sv->db_env, NULL, 0, &txn);
    if (0 != e)
        mdb_fatal(e);

    
    e = mdb_put(txn, sv->state, &key, &val, 0);
    switch (e)
    {
    case 0:
        break;
    case MDB_MAP_FULL:
    {
        mdb_txn_abort(txn);
        return -1;
    }
    default:
        mdb_fatal(e);
    }

    e = mdb_txn_commit(txn);
    if (0 != e)
        mdb_fatal(e);

    return 0;
}

/** Raft callback for removing the first entry from the log
 * @note this is provided to support log compaction in the future */
static int __raft_logentry_poll(
    raft_server_t* raft,
    void *user_data,
    unsigned long entry_idx
    )
{
    MDB_val k, v;

    mdb_poll(sv->db_env, sv->entries, &k, &v);

    return 0;
}

/** Raft callback for deleting the most recent entry from the log.
 * This happens when an invalid leader finds a valid leader and has to delete
 * superseded log entries. */
static int __raft_logentry_pop(
    raft_server_t* raft,
    void *user_data,
    unsigned long entry_idx
    )
{
    MDB_val k, v;

    mdb_pop(sv->db_env, sv->entries, &k, &v);

    return 0;
}
int	__raft_logentry_get
(
   raft_server_t* raft,
   void *user_data,
   raft_entry_t *entry,
   unsigned long ety_idx
   )
{
	MDB_txn *txn;

	int e = mdb_txn_begin(sv->db_env, NULL, 0, &txn);
	   if (0 != e)
		   mdb_fatal(e);

	ety_idx <<= 1;
    MDB_val key = { .mv_size = sizeof(ety_idx), .mv_data = (void*)&ety_idx };
    MDB_val val ;

    e = mdb_get(txn, sv->entries, &key, &val);
    switch (e)
    {
    case 0:
        break;
    case MDB_MAP_FULL:
    {
        mdb_txn_abort(txn);
        return -1;
    }
    default:
        mdb_fatal(e);
    }
	struct pbc_slice  slice;
	slice.len = val.mv_size;
	slice.buffer = val.mv_data;
	parse_entry_control_read(g_pb_env,&slice, entry);

		

    /* 2. put entry */
    ety_idx |= 1;
    key.mv_size = sizeof(ety_idx);
    key.mv_data = (void*)&ety_idx;
   

  e = mdb_get(txn, sv->entries, &key, &val);
    switch (e)
    {
    case 0:
        break;
    case MDB_MAP_FULL:
    {
        mdb_txn_abort(txn);
        return -1;
    }
    default:
        mdb_fatal(e);
    }

	entry->data.buf = val.mv_data;
	entry->data.len = val.mv_size;

    e = mdb_txn_commit(txn);
    if (0 != e)
        mdb_fatal(e);

	return 0;
   
}

/** Non-voting node now has enough logs to be able to vote.
 * Append a finalization cfg log entry. */
static void __raft_node_has_sufficient_logs(
    raft_server_t* raft,
    void *user_data,
    raft_node_t* node)
{
    peer_connection_t* conn = raft_node_get_udata(node);
    __append_cfg_change(sv, RAFT_LOGTYPE_ADD_NODE,
                        inet_ntoa(conn->addr.sin_addr),
                        conn->raft_port,
                        conn->server_port,
                        raft_node_get_id(conn->node));
}

raft_cbs_t raft_funcs = {
    .send_requestvote            = __raft_send_requestvote,
    .send_appendentries          = __raft_send_appendentries,
    .applylog                    = __raft_applylog,
    .persist_vote                = __raft_persist_vote,
    .persist_term                = __raft_persist_term,
    .log_offer                   = __raft_logentry_offer,
    .log_get					 = __raft_logentry_get,
    .log_poll                    = __raft_logentry_poll,
    .log_pop                     = __raft_logentry_pop,
    .node_has_sufficient_logs    = __raft_node_has_sufficient_logs,
    .log                         = __raft_log,
};

/** Raft callback for handling periodic logic */
static void __periodic(uv_timer_t* handle)
{
    uv_mutex_lock(&sv->raft_lock);

    raft_periodic(sv->raft, PERIOD_MSEC);

    if (opts.leave)
    {
        raft_node_t* leader = raft_get_current_leader_node(sv->raft);
        if (leader)
        {
            peer_connection_t* leader_conn = raft_node_get_udata(leader);
            assert(raft_node_get_id(leader) != sv->node_id);
            __send_leave(leader_conn);
        }
    }

    raft_apply_all(sv->raft);

    uv_mutex_unlock(&sv->raft_lock);
}

/** Load all log entries we have persisted to disk */
static void __load_commit_log(server_t* sv)
{
   /*

	MDB_cursor* curs;
    MDB_txn *txn;
    MDB_val k, v;
    int e;


    e = mdb_txn_begin(sv->db_env, NULL, MDB_RDONLY, &txn);
    if (0 != e)
        mdb_fatal(e);

    e = mdb_cursor_open(txn, sv->entries, &curs);
    if (0 != e)
        mdb_fatal(e);

    e = mdb_cursor_get(curs, &k, &v, MDB_FIRST);
    switch (e)
    {
    case 0:
        break;
    case MDB_NOTFOUND:
        return;
    default:
        mdb_fatal(e);
    }

    raft_entry_t ety;
    ety.id = 0;

    int n_entries = 0;

    do
    {
        if (!(*(int*)k.mv_data & 1))
        {
            // entry metadata
            tpl_node *tn =
                tpl_map(tpl_peek(TPL_MEM, v.mv_data, v.mv_size), &ety);
            tpl_load(tn, TPL_MEM, v.mv_data, v.mv_size);
            tpl_unpack(tn, 0);
        }
        else
        {
            // entry data for FSM 
            ety.data.buf = v.mv_data;
            ety.data.len = v.mv_size;
            raft_append_entry(sv->raft, &ety);
            n_entries++;
        }

        e = mdb_cursor_get(curs, &k, &v, MDB_NEXT);
    }
    while (0 == e);

    mdb_cursor_close(curs);

    e = mdb_txn_commit(txn);
    if (0 != e)
        mdb_fatal(e);
*/
   MDB_val val;
   unsigned long total_count;
   unsigned long hold_count;
  

   mdb_gets(sv->db_env, sv->state, "log_total_count", &val);
   if(val.mv_data)
  		total_count = *(unsigned long*)val.mv_data;
   mdb_gets(sv->db_env, sv->state, "log_hold_count", &val);
   if(val.mv_data)
  		hold_count = *(unsigned long*)val.mv_data;
   printf("log count:%lu\n",total_count);
   raft_set_log_info(sv->raft,total_count,hold_count);

   
   mdb_gets(sv->db_env, sv->state, "last_applied_idx", &val);
   if (val.mv_data)
	{
		printf("last_applied_idx:%lu\n",*(unsigned long*)val.mv_data);
		raft_set_last_applied_idx(sv->raft, *(unsigned long*)val.mv_data);
	}


   mdb_gets(sv->db_env, sv->state, "commit_idx", &val);
   
    if (val.mv_data)
    {
    	printf("commit_idx:%lu\n",*(unsigned long*)val.mv_data);
    	raft_set_commit_idx(sv->raft, *(unsigned long*)val.mv_data);
    }
	
    raft_apply_all(sv->raft);
}

/** Load voted_for and term raft fields */
static void __load_persistent_state(server_t* sv)
{
    int val = -1;

    mdb_gets_int(sv->db_env, sv->state, "voted_for", &val);
    raft_vote_for_nodeid(sv->raft, val);
	unsigned long term=0;
    int ret = mdb_gets_ulong(sv->db_env, sv->state, "term", &term);
	if(ret != 0)
		term =0;
	printf("load term %lu\n",term);
    raft_set_current_term(sv->raft, term);
}

static int __load_opts(server_t* sv, options_t* option)
{
	MDB_val val;  
	mdb_gets(sv->db_env, sv->state, "option", &val);
	if(val.mv_data)
		memcpy(option,val.mv_data,sizeof(options_t));   
    return 0;
}



static void __drop_db(server_t* sv)
{
    MDB_dbi dbs[] = { sv->entries, sv->tickets, sv->state };
    mdb_drop_dbs(sv->db_env, dbs, len(dbs));
}

static void __start_raft_periodic_timer(server_t* sv)
{
    uv_timer_t *periodic_req = calloc(1, sizeof(uv_timer_t));
    periodic_req->data = sv;
    uv_timer_init(&sv->peer_loop, periodic_req);
    uv_timer_start(periodic_req, __periodic, 0, PERIOD_MSEC);
    raft_set_election_timeout(sv->raft, 2000);
}

static void __int_handler(int dummy)
{

	//exit(0);
    uv_mutex_lock(&sv->raft_lock);
    raft_node_t* leader = raft_get_current_leader_node(sv->raft);
	printf("leader:%X\n",leader);
    if (leader)
    {
        if (raft_node_get_id(leader) == sv->node_id)
        {
            printf("I'm the leader, I can't leave the cluster...\n");
            goto done;
        }

        peer_connection_t* leader_conn = raft_node_get_udata(leader);
        if (leader_conn)
        {
            printf("Leaving cluster...\n");
            __send_leave(leader_conn);
            goto done;
        }
    }
    printf("Try again no leader at the moment...\n");
done:
    uv_mutex_unlock(&sv->raft_lock);
}

static void __new_db(server_t* sv)
{
    mdb_db_env_create(&sv->db_env, 0, opts.path, opts.db_size);
    mdb_db_create(&sv->entries, sv->db_env, "entries");
    mdb_db_create(&sv->tickets, sv->db_env, "docs");
    mdb_db_create(&sv->state, sv->db_env, "state");
}


static void __start_peer_socket(server_t* sv, const char* host, int port, uv_tcp_t* listen)
{
    memset(&sv->peer_loop, 0, sizeof(uv_loop_t));
    int e = uv_loop_init(&sv->peer_loop);
    if (0 != e)
        uv_fatal(e);

    uv_bind_listen_socket(listen, host, port, &sv->peer_loop);
    e = uv_listen((uv_stream_t*)listen, MAX_PEER_CONNECTIONS,
                  __on_peer_connection);
    if (0 != e)
        uv_fatal(e);
}

static void __save_opts(server_t* sv, options_t* option)
{
	/*
    mdb_puts_int_commit(sv->db_env, sv->state, "id", sv->node_id);
    mdb_puts_int_commit(sv->db_env, sv->state, "raft_port", opts->raft_port);
    mdb_puts_int_commit(sv->db_env, sv->state, "http_port", opts->server_port);
	*/
	option->id = sv->node_id;


	MDB_val key,val;
	key.mv_size = strlen("option");
	key.mv_data = "option";
	val.mv_size = sizeof(options_t);
	val.mv_data = option;

	MDB_txn *txn;

    int e = mdb_txn_begin(sv->db_env, NULL, 0, &txn);
    if (0 != e)
        mdb_fatal(e);

    
    e = mdb_put(txn, sv->state, &key, &val, 0);
    switch (e)
    {
    case 0:
        break;
    case MDB_MAP_FULL:
    {
        mdb_txn_abort(txn);
        return ;
    }
    default:
        mdb_fatal(e);
    }


    e = mdb_txn_commit(txn);
    if (0 != e)
        mdb_fatal(e);

}

int main(int argc, char **argv)
{
    memset(sv, 0, sizeof(server_t));

    int e = parse_options(argc, argv, &opts);
    if (-1 == e)
        exit(-1);
    else if (opts.help)
    {
        show_usage();
        exit(0);
    }
    else if (opts.version)
    {
        fprintf(stdout, "%s\n", VERSION);
        exit(0);
    }

    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT, __int_handler);

	parse_msg_init();
	parse_zmq_msg_init();
    sv->raft = raft_new();
    raft_set_callbacks(sv->raft, &raft_funcs, sv);

    srand(time(NULL));

    __new_db(sv);

    if (opts.drop)
    {
        __drop_db(sv);
        exit(0);
    }


    /* lock and condition to support HTTP client blocking */
    uv_mutex_init(&sv->raft_lock);
    uv_cond_init(&sv->appendentries_received);

    uv_tcp_t  peer_listen;
    uv_multiplex_t m;

    /* get ID */
    if (opts.start || opts.join)
    {
        if (opts.id)
            sv->node_id = opts.id;
    }
    else
    {
        e = __load_opts(sv, &re_opts);
		printf_option(&re_opts);
        if (0 != e)
        {
            printf("ERROR: No database available.\n"
                   "Please start or join a cluster.\n");
            abort();
        }
		sv->node_id = re_opts.id;
    }

    /* add self */
    raft_add_node(sv->raft, NULL, sv->node_id, 1);

	zmq_server_info_t info;
    if (opts.start || opts.join)
    {
        __drop_db(sv);
        __new_db(sv);
        __save_opts(sv, &opts);
		printf("save option !!!!!!!!!!!!!!!!!!!!!!\n");
		printf_option(&opts);

		snprintf(info.ip,32,"%s",opts.host);
		info.port = opts.server_port;
		info.thread_count = HTTP_WORKERS;

		start_zmq_server(&info);
		

        __start_peer_socket(sv, opts.host, opts.raft_port, &peer_listen);

        if (opts.start)
        {
            raft_become_leader(sv->raft);
            /* We store membership configuration inside the Raft log.
             * This configuration change is going to be the initial membership
             * configuration (ie. original node) inside the Raft log. The
             * first configuration is for a cluster of 1 node. */
            __append_cfg_change(sv, RAFT_LOGTYPE_ADD_NODE,
                                opts.host,
                                opts.raft_port,
                                opts.server_port,
                                sv->node_id);
        }
        else
        {
            peer_connection_t* conn = __new_connection(sv);
            __connect_to_peer_at_host(conn, opts.PEER_IP, opts.PEER_PORT);
        }
    }
    /* Reload cluster information and rejoin cluster */
    else if(opts.restart)
    {
     	__save_opts(sv, &opts);
    	sv->node_id = opts.id;
		snprintf(info.ip,32,"%s",opts.host);
		info.port = opts.server_port;
		info.thread_count = HTTP_WORKERS;
		start_zmq_server(&info);
        __start_peer_socket(sv, opts.host, opts.raft_port, &peer_listen);
        __load_commit_log(sv);
        __load_persistent_state(sv);

		printf("num nodes:%d\n",raft_get_num_nodes(sv->raft));

		 raft_become_leader(sv->raft);
            /* We store membership configuration inside the Raft log.
             * This configuration change is going to be the initial membership
             * configuration (ie. original node) inside the Raft log. The
             * first configuration is for a cluster of 1 node. */
         __append_cfg_change(sv, RAFT_LOGTYPE_ADD_NODE,
                                opts.host,
                                opts.raft_port,
                                opts.server_port,
                                sv->node_id);
			
    }
	else
	{
		printf_option(&opts);
		change_with_the_old(&opts,&re_opts);
		printf_option(&opts);

		sv->node_id = opts.id;
		snprintf(info.ip,32,"%s",opts.host);
		info.port = opts.server_port;
		info.thread_count = HTTP_WORKERS;
		
		
    	start_zmq_server(&info);
        __start_peer_socket(sv, opts.host, opts.raft_port, &peer_listen);
        __load_commit_log(sv);
        __load_persistent_state(sv);

		 peer_connection_t* conn = __new_connection(sv);
         __connect_to_peer_at_host(conn, opts.PEER_IP, opts.PEER_PORT);
/*
        if (1 == raft_get_num_nodes(sv->raft))
        {
            raft_become_leader(sv->raft);
        }
        else
        {
            for (int i=0; i<raft_get_num_nodes(sv->raft); i++)
            {
                raft_node_t* node = raft_get_node_from_idx(sv->raft, i);
                if (raft_node_get_id(node) == sv->node_id) continue;
                __connect_to_peer(raft_node_get_udata(node));
            }
        }
        */
	}

    __start_raft_periodic_timer(sv);

    uv_run(&sv->peer_loop, UV_RUN_DEFAULT);
	parse_msg_free();
	parse_zmq_msg_free();
	printf("end!!!!!!!\n");
}
