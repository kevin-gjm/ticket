#ifndef DEF_H_
#define DEF_H_

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>

#include "h2o.h"
#include "h2o/http1.h"
#include "h2o_helpers.h"
#include "lmdb.h"
#include "lmdb_helpers.h"
#include "raft.h"
#include "uv_helpers.h"
#include "uv_multiplex.h"
#include "arraytools.h"

#include "parse_msg.h"



#define VERSION "0.1.0"
#define ANYPORT 65535
#define MAX_HTTP_CONNECTIONS 128
#define MAX_PEER_CONNECTIONS 128
#define IPV4_STR_LEN 3 * 4 + 3 + 1
#define PERIOD_MSEC 500
#define RAFT_BUFLEN 512
#define LEADER_URL_LEN 512
#define IPC_PIPE_NAME "ticketd_ipc"
#define HTTP_WORKERS 4
//strlen("111.111.111.111")



/** Add/remove Raft peer */
typedef struct
{
    int raft_port;
    int server_port;
    int node_id;
    char host[IP_STR_LEN];
} entry_cfg_change_t;



typedef enum
{
    DISCONNECTED,
    CONNECTING,
    CONNECTED,
} conn_status_e;

typedef struct peer_connection_s peer_connection_t;

struct peer_connection_s
{
    /* peer's address */
    struct sockaddr_in addr;

    int server_port, raft_port;


    /* tell if we need to connect or not */
    conn_status_e connection_status;

    /* peer's raft node_idx */
    raft_node_t* node;

    /* remember most recent append entries msg, we refer to this msg when we
     * finish reading the log entries.
     * used in tandem with n_expected_entries */
    msg_t ae;

    uv_stream_t* stream;
	uv_connect_t *connect;

    uv_loop_t* loop;

    peer_connection_t *next;
};

typedef struct
{
    /* the server's node ID */
    int node_id;

    raft_server_t* raft;

    /* Set of tickets that have been issued
     * We store unsigned ints in here */
    MDB_dbi tickets;

    /* Persistent state for voted_for and term
     * We store string keys (eg. "term") with int values */
    MDB_dbi state;

    /* Entries that have been appended to our log
     * For each log entry we store two things next to each other:
     *  - TPL serialized raft_entry_t
     *  - raft_entry_data_t */
    MDB_dbi entries;

    /* LMDB database environment */
    MDB_env *db_env;

    h2o_globalconf_t cfg;
    h2o_context_t ctx;

    /* Raft isn't multi-threaded, therefore we use a global lock */
    uv_mutex_t raft_lock;

    /* When we receive an entry from the client we need to block until the
     * entry has been committed. This condition is used to wake us up. */
    uv_cond_t appendentries_received;

    uv_loop_t peer_loop, http_loop;

    /* Link list of peer connections */
    peer_connection_t* conns;
} server_t;


#endif 
