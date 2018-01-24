#ifndef ZMQ_SERVER_H_
#define ZMQ_SERVER_H_
#include "raft.h"
#include "def.h"
typedef struct 
{
	int thread_count;
	int port;
	char ip[32];
}zmq_server_info_t;


#define ZMQPB_FILE "../store.pb"

#define ERROR_MSG_LENGTH	512
#define ERROR_NO_LEADER 	1
#define ERROR_REDIRECT		2
#define ERROR_MSG_PARSE		3
#define ERROR_INTERNAL		4	//server code error
#define ERROR_TRY_AGAIN		5
#define ERROR_NOT_COMMIT	6
#define ERROR_NO_DEFINE		7
#define ERROR_SQL_PREPARE	8

struct pbc_env * g_pb_zmq_env;
extern server_t *sv ;


typedef enum  {
    MsgControl =0,
    MsgControlResp,
    MsgNoQuery,
    MsgNoQueryResp,
    MsgQuery,
    MsgQueryResp,
    MsgHeartbeat,
    MsgHeartbeatResp,
    MsgNoDefine,
}MessageType;

typedef struct
{
	char * sql;
}query_t;
typedef struct
{
	int use_tran;
	char* sql;
	int bin_count;
	void* bin;
}no_query_t;


typedef struct 
{
union{
	query_t qry;
	no_query_t nqry;
};
}request_t;


typedef struct
{
}command_t;

typedef struct 
{
	int result;
	int error_code;
	char error_description[512];
}response_t;

typedef struct 
{
	MessageType type;
	unsigned long sequence;
	union{
		request_t req;
		response_t rep;
		command_t cmd;
		};
}zmq_server_msg_t;


void start_zmq_server(zmq_server_info_t* info);

int parse_zmq_msg_init();
int parse_zmq_msg_free();

int parse_zmq_msg_read(struct pbc_env* env, struct pbc_slice* slice,zmq_server_msg_t* msg);

int free_zmq_msg_mem_if_needed(zmq_server_msg_t* msg);

#endif //ZMQ_SERVER_H_