#ifndef PARSE_MSG_H_
#define PARSE_MSG_H_

#include "raft.h"
#include "pbc.h"
#include "readfile.h"

#define IP_STR_LEN 16
#define RAFTPB_FILE "./raftpb.pb"

typedef enum {
    HANDSHAKE_FAILURE,
    HANDSHAKE_SUCCESS,
} handshake_state_e; 

/** Message types used for peer to peer traffic
 * These values are used to identify message types during deserialization */
typedef enum
{
    /** Handshake is a special non-raft message type
     * We send a handshake so that we can identify ourselves to our peers */
    MSG_HANDSHAKE,
    /** Successful responses mean we can start the Raft periodic callback */
    MSG_HANDSHAKE_RESPONSE,
    /** Tell leader we want to leave the cluster */
    /* When instance is ctrl-c'd we have to gracefuly disconnect */
    MSG_LEAVE,
    /* Receiving a leave response means we can shutdown */
    MSG_LEAVE_RESPONSE,
    MSG_REQUESTVOTE,
    MSG_REQUESTVOTE_RESPONSE,
    MSG_APPENDENTRIES,
    MSG_APPENDENTRIES_RESPONSE,
} peer_message_type_e;

/** Peer protocol handshake
 * Send handshake after connecting so that our peer can identify us */
typedef struct
{
    int raft_port;
    int server_port;
    int node_id;
} msg_handshake_t;

typedef struct
{
    int success;

    /* leader's Raft port */
    int leader_port;

    /* the responding node's HTTP port */
    int server_port;

    /* my Raft node ID.
     * Sometimes we don't know who we did the handshake with */
    int node_id;

    char leader_host[IP_STR_LEN];
} msg_handshake_response_t;

typedef struct
{
    int type;
    union
    {
        msg_handshake_t hs;
        msg_handshake_response_t hsr;
        msg_requestvote_t rv;
        msg_requestvote_response_t rvr;
        msg_appendentries_t ae;
        msg_appendentries_response_t aer;
    };
} msg_t;
struct pbc_env * g_pb_env;
int parse_msg_init();
int parse_msg_free();
/*you have to release the pbc_wmessage self*/

struct pbc_wmessage * parse_msg_write(struct pbc_env * env,msg_t* msg);

/*
	the data in entry.data.buf or msg.ae.entries[0].data.buf you have to free youself
*/
int parse_msg_read(struct pbc_env* env, struct pbc_slice* slice,msg_t* msg,msg_entry_t* entry);

/*
	parse entry control info to pb not include the data feild.
	you have to release the pbc_wmessage youself
*/
struct pbc_wmessage * parse_entry_control_write(struct pbc_env * env,msg_entry_t* ety);
/*
	decode the entry control info
*/
int parse_entry_control_read(struct pbc_env* env, struct pbc_slice* slice,msg_entry_t* entry);

int test();

#endif //PARSE_MSG_H_