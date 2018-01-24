#include "zmq_server.h"
#include <pthread.h>
#include <czmq.h>
#include <zmsg.h>
#include "pbc.h"
#include "raft.h"
#include "sql_store.h"

struct pbc_wmessage * parse_zmq_noquery_msg_write(struct pbc_env * env,zmq_server_msg_t* msg);
int send_noquery_result(zframe_t* id,zsock_t* server,MessageType type,unsigned long seq,int result,int err_code,char* errmsg);
int parse_msg_read_type_and_seq(struct pbc_env* env, struct pbc_slice* slice,zmq_server_msg_t* msg);


/*
	return 0 success. Else return error num
*/
int handle_zmq_msg_noquery(zmsg_t* msg,char* error_msg)
{
    raft_node_t* leader = raft_get_current_leader_node(sv->raft);
    if (!leader)
    {
    	snprintf(error_msg, ERROR_MSG_LENGTH, "not have a leader!");	
        return ERROR_NO_LEADER;
    }
    else if (raft_node_get_id(leader) != sv->node_id)
    {
        peer_connection_t* leader_conn = raft_node_get_udata(leader);
        snprintf(error_msg, ERROR_MSG_LENGTH, "tcp://%s:%d",
                 inet_ntoa(leader_conn->addr.sin_addr),
                 leader_conn->server_port);
        return ERROR_REDIRECT;
    }

    int e;

   
	zframe_t* fr_data = zmsg_last(msg);
	int frame_size = zframe_size(fr_data);
	
	if (frame_size <= 0 )
	{
		snprintf(error_msg, ERROR_MSG_LENGTH, "message parse error!");	
		return ERROR_MSG_PARSE;
	}
	
    msg_entry_t entry = {};
    entry.id = rand();
    entry.data.buf = (void*)zframe_data(fr_data);
    entry.data.len = frame_size;

    uv_mutex_lock(&sv->raft_lock);

    msg_entry_response_t r;
    e = raft_recv_entry(sv->raft, &entry, &r);
    if (0 != e)
    {
		uv_mutex_unlock(&sv->raft_lock);
		snprintf(error_msg, ERROR_MSG_LENGTH, "internal server error!");	
		return ERROR_INTERNAL;
    }

    /* block until the entry is committed */
    int done = 0, tries = 0;
    do
    {
        if (3 < tries)
        {
            uv_mutex_unlock(&sv->raft_lock);
			snprintf(error_msg, ERROR_MSG_LENGTH, "failed to commit entry");
            return ERROR_NOT_COMMIT;
        }

        uv_cond_wait(&sv->appendentries_received, &sv->raft_lock);
        e = raft_msg_entry_response_committed(sv->raft, &r);
        tries += 1;
        switch (e)
        {
        case 0:
            /* not committed yet */
            break;
        case 1:
            done = 1;
            uv_mutex_unlock(&sv->raft_lock);
            break;
        case -1:
            uv_mutex_unlock(&sv->raft_lock);
			snprintf(error_msg, ERROR_MSG_LENGTH, "try again");	
            return ERROR_TRY_AGAIN;
        }
    }
    while (!done);

	return 0;
}
int handle_zmq_msg_query(zframe_t* id,zsock_t* server,zmsg_t* msg,unsigned long seq)
{
	raft_node_t* leader = raft_get_current_leader_node(sv->raft);
	if (!leader)
	{
		send_noquery_result(id,server,MsgQueryResp,seq,0,ERROR_NO_LEADER,"not have a leader!");
		return -1;
	}
	else if (raft_node_get_id(leader) != sv->node_id)
	{
		char tmp_buf[ERROR_MSG_LENGTH]={0};
		peer_connection_t* leader_conn = raft_node_get_udata(leader);
		snprintf(tmp_buf, ERROR_MSG_LENGTH, "tcp://%s:%d",
					inet_ntoa(leader_conn->addr.sin_addr),
					leader_conn->server_port);
		send_noquery_result(id,server,MsgQueryResp,seq,0,ERROR_REDIRECT,tmp_buf);
		return -1;
	}
	
	int e;
	
	   
	zframe_t* fr_data = zmsg_last(msg);
	int frame_size = zframe_size(fr_data);
		
	if (frame_size <= 0 )
	{
		send_noquery_result(id,server,MsgQueryResp,seq,0,ERROR_MSG_LENGTH,"message parse error!");
		return ERROR_MSG_PARSE;
	}
	struct pbc_slice slice;
	slice.buffer = (void*)zframe_data(fr_data);
	slice.len = frame_size;

	zmq_server_msg_t zmsg={0};
	parse_zmq_msg_read(g_pb_zmq_env,&slice,&zmsg);
	assert(zmsg.type == MsgQuery);
	printf("<><><><><><><><>!!!!!! sql:%s\n",zmsg.req.qry.sql);

	//TODO:simple this process!!!!
	//return pb message. So So SO complex,so write here
	int error_code=0;
	int b_ret=0;
	char err_buf[ERROR_MSG_LENGTH]={0};
	int row_count=0;
	int column_count=0;
	
	struct pbc_wmessage * pb_msg = pbc_wmessage_new(g_pb_zmq_env, "storepb.Message");
	
	pbc_wmessage_string(pb_msg , "type" , "MsgQueryResp" , -1);
	pbc_wmessage_integer(pb_msg, "sequence", seq, 0);
	struct pbc_wmessage * pb_rep = pbc_wmessage_message(pb_msg, "response");
	struct pbc_wmessage * pb_qrydata = pbc_wmessage_message(pb_rep, "querydata");


	uv_mutex_lock(&sv->raft_lock);

	sqlite3 *db =  store_open();

	
	sqlite3_stmt *stmt;
	int rc = sqlite3_prepare_v2(db,zmsg.req.qry.sql, strlen(zmsg.req.qry.sql), &stmt, NULL);
	if(SQLITE_OK != rc)
	{
		if(stmt)
			sqlite3_finalize(stmt);
		snprintf(err_buf,ERROR_MSG_LENGTH,"Query prepare error return value:%d!",rc);
		b_ret = 0;
		error_code= ERROR_SQL_PREPARE;
		goto end;
	}
	
	column_count = sqlite3_column_count(stmt);
	
	char ret_tmp_buf[128] ={0};
	while (SQLITE_ROW == sqlite3_step(stmt))
	{
		row_count++;
		for(int i=0;i<column_count;i++)
		{
			int vtype = sqlite3_column_type(stmt,i);
			if (vtype == SQLITE_INTEGER)
			{
				int v = sqlite3_column_int(stmt,i);
				snprintf(ret_tmp_buf,128,"%d",v);
				pbc_wmessage_string(pb_qrydata,"data",ret_tmp_buf,-1);
			}else if (vtype == SQLITE_FLOAT)
			{
				double v = sqlite3_column_double(stmt,i);
				snprintf(ret_tmp_buf,128,"%f",v);
				pbc_wmessage_string(pb_qrydata,"data",ret_tmp_buf,-1);
			}else if (vtype == SQLITE_TEXT)
			{
				const char* v = (const char*)sqlite3_column_text(stmt,i);
				pbc_wmessage_string(pb_qrydata,"data",v,-1);
	
			}else if (vtype == SQLITE_BLOB)
			{
				pbc_wmessage_string(pb_qrydata,"data",sqlite3_column_blob(stmt,i),sqlite3_column_bytes(stmt,i));
			}
			else if (vtype == SQLITE_NULL)
			{
				pbc_wmessage_string(pb_qrydata,"data","null",-1);
			}
		}
	
	}
		
	
	pbc_wmessage_integer(pb_qrydata, "row_count", row_count, 0);
	pbc_wmessage_integer(pb_qrydata, "column_count", column_count, 0);
	sqlite3_finalize(stmt);

end:
	
	store_close(db);
	pbc_wmessage_integer(pb_rep, "result", b_ret, 0);
	if(b_ret==0)
	{
		pbc_wmessage_integer(pb_rep, "error_code", error_code, 0);
		pbc_wmessage_string(pb_rep , "error_description" , err_buf, -1);
	}
	pbc_wmessage_integer(pb_rep, "last_block", 1, 0);
	pbc_wmessage_buffer(pb_msg, &slice);

	
	zmsg_t *ret_msg = zmsg_new();
	zmsg_append(ret_msg,&id);
	zmsg_addmem(ret_msg, slice.buffer, slice.len);
	zmsg_send(&ret_msg, server);
	printf("send back result with data!\n");
	zmsg_destroy(&ret_msg);

	pbc_wmessage_delete(pb_msg);
	free_zmq_msg_mem_if_needed(&zmsg);
	uv_mutex_unlock(&sv->raft_lock);

	return 0;
}

void handle_zmq_msg(zmsg_t* msg,zsock_t* server,zframe_t* id)
{
	zframe_t* fr_data = zmsg_last(msg);
	int frame_size = zframe_size(fr_data);


	if (frame_size <= 0 )
	{
		send_noquery_result(id,server,MsgNoDefine,0,0,ERROR_MSG_PARSE,"not have data frame");
		return ;
	}
	
    zmq_server_msg_t zmsg ={0};
	struct pbc_slice slice;
	slice.buffer = (void*)zframe_data(fr_data);
	slice.len = frame_size;
	parse_msg_read_type_and_seq(g_pb_zmq_env,&slice,&zmsg);
	if(zmsg.type == MsgNoQuery)
	{
		printf("in handler msg no query\n");
		char error_msg[ERROR_MSG_LENGTH]={0};
		int ret_code = handle_zmq_msg_noquery(msg,error_msg);
		if(ret_code != 0)
		{
			send_noquery_result(id,server,MsgNoQueryResp,zmsg.sequence,0,ret_code,error_msg);
		}
		else
		{
			send_noquery_result(id,server,MsgNoQueryResp,zmsg.sequence,1,0,NULL);
		}
		return;
	}
	else if(zmsg.type == MsgQuery)
	{
		printf("in handler msg query\n");
		handle_zmq_msg_query(id,server,msg,zmsg.sequence);
		return;
	}
	else
	{
		send_noquery_result(id,server,MsgNoDefine,zmsg.sequence,0,ERROR_NO_DEFINE,"not define!");
		return ;
	}
}




int send_noquery_result(zframe_t* id,zsock_t* server,MessageType type,unsigned long seq,int result,int err_code,char* errmsg)
{
	zmq_server_msg_t msg_ret={0};
	msg_ret.type= type;
	msg_ret.sequence=seq;
	msg_ret.rep.result = result;
	if(result == 0)
	{
		msg_ret.rep.error_code = err_code;
		if(errmsg)
			snprintf(msg_ret.rep.error_description,ERROR_MSG_LENGTH,"%s",errmsg);
	}
	struct pbc_wmessage * pb_msg =  parse_zmq_noquery_msg_write(g_pb_zmq_env,&msg_ret);
			
	struct pbc_slice slice;
	pbc_wmessage_buffer(pb_msg, &slice);

	zmsg_t *zmsg = zmsg_new();
	zmsg_append(zmsg,&id);

	zmsg_addmem(zmsg, slice.buffer, slice.len);
	zmsg_send(&zmsg, server);
	printf("send back result no data!\n");
	pbc_wmessage_delete(pb_msg);
	zmsg_destroy(&zmsg);
}



void* proxy_routine(void* arg)
{
	zmq_server_info_t * info = (zmq_server_info_t*)arg;

	pthread_detach(pthread_self());
	zsock_t* server = zsock_new(ZMQ_ROUTER);
	zsock_bind(server, "tcp://%s:%d",info->ip,info->port);

	/*
	for (int i = 0; i < info->thread_count; i++)
	{
		pthread_t proxy_tid;
		pthread_create(&proxy_tid, NULL, worker_routine, NULL);
	}
	*/

	//srand((int)time(0));

	printf("start zmq server listening on %s:%d\n",info->ip,info->port);

	zpoller_t *poller = zpoller_new(server,NULL);

	
	while (!zctx_interrupted)
	{
		
		zsock_t *which = (zsock_t *)zpoller_wait(poller, -1);

		printf("receive a msg\n");
		if (which==server)
		{
			zmsg_t *msg = zmsg_recv(server);
			if (!msg)
				break;
			if (zmsg_size(msg) > 1)
			{
			printf("msg:\n");
				zmsg_print(msg);
				zframe_t* id_frame_tmp = zmsg_first(msg);
				zframe_t* id_frame = zframe_dup(id_frame_tmp);
				handle_zmq_msg(msg,server,id_frame);
				
				zmsg_destroy(&msg);
				if (zctx_interrupted)
					break;
			}

		}
	}
	

}


void start_zmq_server(zmq_server_info_t* info)
{
	pthread_t proxy_tid;
	pthread_create(&proxy_tid, NULL, proxy_routine, info);
	//pthread_join(proxy_tid, NULL);
}


int parse_zmq_msg_init()
{
	struct pbc_slice slice;
	read_file(ZMQPB_FILE, &slice);
	if (slice.buffer == NULL)
		return -1;
	g_pb_zmq_env = pbc_new();
	int r = pbc_register(g_pb_zmq_env, &slice);
	if (r) {
		printf("Error : %s", pbc_error(g_pb_zmq_env));
		exit(1);
	}
	
	free(slice.buffer);

}
int parse_zmq_msg_free()
{
	pbc_delete(g_pb_zmq_env);
}



/*
	noquery resp or query resp with error
*/
struct pbc_wmessage * parse_zmq_noquery_msg_write(struct pbc_env * env,zmq_server_msg_t* msg)
{

	struct pbc_wmessage * pb_msg = pbc_wmessage_new(env, "storepb.Message");
	switch(msg->type)
	{
		case MsgNoQueryResp:
			pbc_wmessage_string(pb_msg , "type" , "MsgNoQueryResp" , -1);
			break;
		case MsgQueryResp:
			pbc_wmessage_string(pb_msg , "type" , "MsgQueryResp" , -1);
			break;
		default:
			printf("this func not support this type\n");		
	}
	
	pbc_wmessage_integer(pb_msg, "sequence", msg->sequence, 0);
	struct pbc_wmessage * pb_rep = pbc_wmessage_message(pb_msg, "response");
	pbc_wmessage_integer(pb_rep, "result", msg->rep.result, 0);
	if(msg->rep.result==0)
	{
		pbc_wmessage_integer(pb_rep, "error_code", msg->rep.error_code, 0);
		pbc_wmessage_string(pb_rep , "error_description" , msg->rep.error_description, -1);
	}
	pbc_wmessage_integer(pb_rep, "last_block", 1, 0);
	
	return pb_msg;
}


/*
	should free the memory calloc in msg->req.nqry.bin and msg->req.nqry.sql
*/
int free_zmq_msg_mem_if_needed(zmq_server_msg_t* msg)
{
	if(msg->type == MsgNoQuery )
	{
		if(msg->req.nqry.sql)
		{
			printf("free nqry sql!\n");
			free(msg->req.nqry.sql);
			msg->req.nqry.sql=NULL;
		}
		/*
		int bin_count =	msg->req.nqry.bin_count;
		if(bin_count>0)
		{
			if(msg->req.nqry.bin==NULL)
			{
				for(int i=0;i<bin_count;i++)
				{
					if(msg->req.nqry.bin[i])
					{
						free(msg->req.nqry.bin[i]);
						msg->req.nqry.bin[i]=NULL;
					}
				}
				free(msg->req.nqry.bin);
				msg->req.nqry.bin=NULL;
			}			
		}
		*/
	}
	if(msg->type == MsgQuery)
	{
		if(msg->req.qry.sql)
		{
			printf("free qry sql!\n");
			free(msg->req.qry.sql);
			msg->req.qry.sql=NULL;
		}
	}
	return 0;
}

int parse_msg_read_type_and_seq(struct pbc_env* env, struct pbc_slice* slice,zmq_server_msg_t* msg)
{
	if(msg==NULL) return -1;
	struct pbc_rmessage * m = pbc_rmessage_new(env, "storepb.Message", slice);
	if (m==NULL) {
		printf("Error : %s\n",pbc_error(env));
		return -1;
	}
	msg->sequence = pbc_rmessage_integer(m , "sequence" , 0 , NULL);
	
	const char* type = pbc_rmessage_string(m , "type" , 0 , NULL);
		
	if(strncmp(type,"MsgControl",32) ==0)
	{
		msg->type = MsgControl;
	}
	else if(strncmp(type,"MsgControlResp",32) ==0)
	{
		msg->type = MsgControlResp;
	}
	else if(strncmp(type,"MsgNoQuery",32) ==0)
	{
			msg->type = MsgNoQuery;
	
	}
	else if(strncmp(type,"MsgNoQueryResp",32) ==0)
	{
		msg->type = MsgNoQueryResp;
	}
	else if(strncmp(type,"MsgQuery",32) ==0)
	{
		msg->type = MsgQuery;
			
	}
	else if(strncmp(type,"MsgQueryResp",32) ==0)
	{
		msg->type = MsgQueryResp;
	}
	else if(strncmp(type,"MsgHeartbeat",32) ==0)
	{
		msg->type = MsgHeartbeat;
	}
	else if(strncmp(type,"MsgHeartbeatResp",32) ==0)
	{
		msg->type = MsgHeartbeatResp;
	}
	else 
	{
		goto fail;
	}
		pbc_rmessage_delete(m);
		return 0;
	fail:
		pbc_rmessage_delete(m);
		return -1;

}




int parse_zmq_msg_read(struct pbc_env* env, struct pbc_slice* slice,zmq_server_msg_t* msg)
{
	if(msg==NULL) return -1;
	struct pbc_rmessage * m = pbc_rmessage_new(env, "storepb.Message", slice);
	if (m==NULL) {
		printf("Error : %s",pbc_error(env));
		return -1;
	}
	
	msg->sequence = pbc_rmessage_integer(m , "sequence" , 0 , NULL);

	const char* type = pbc_rmessage_string(m , "type" , 0 , NULL);
	
	if(strncmp(type,"MsgControl",32) ==0)
	{
		msg->type = MsgControl;
	}
	else if(strncmp(type,"MsgControlResp",32) ==0)
	{
		msg->type = MsgControlResp;
	}
	else if(strncmp(type,"MsgNoQuery",32) ==0)
	{
		msg->type = MsgNoQuery;
		struct pbc_rmessage * req = pbc_rmessage_message(m , "request", 0);
		struct pbc_rmessage *nqry  = pbc_rmessage_message(req, "no_query", 0);

		msg->req.nqry.use_tran = pbc_rmessage_integer(nqry, "use_tran", 0, NULL) ;
		
		int sz;
		const char* sql = pbc_rmessage_string(nqry,"sql",0,&sz);
		if(sz > 0)
		{
			msg->req.nqry.sql = calloc(1,sz+1);
			memcpy(msg->req.nqry.sql,sql,sz);
			
		}else
		{
			goto fail;
		}
		/*
		int n = pbc_rmessage_size(nqry, "bin");
		msg->req.nqry.bin_count = n;
		if(n>0)
		{
			msg->req.nqry.bin = calloc(sizeof(void*),sz);
			const char* bin;
			int bin_size;
			for (int i=0;i<n;i++) {
				bin = pbc_rmessage_string(nqry,"bin",i,&bin_size);
				msg->req.nqry.bin[i] = calloc(1,bin_size);
				memcpy(msg->req.nqry.bin[i],bin,bin_size);
			}
		}
		*/
		
	}
	else if(strncmp(type,"MsgNoQueryResp",32) ==0)
	{
		msg->type = MsgNoQueryResp;
	}
	else if(strncmp(type,"MsgQuery",32) ==0)
	{
		msg->type = MsgQuery;
		struct pbc_rmessage * req = pbc_rmessage_message(m , "request", 0);
		struct pbc_rmessage *qry  = pbc_rmessage_message(req, "query", 0);
		int sz;
		const char* sql = pbc_rmessage_string(qry,"sql",0,&sz);
		if(sz > 0)
		{
					msg->req.qry.sql = calloc(1,sz+1);
					memcpy(msg->req.qry.sql,sql,sz);
		}else
		{
			goto fail;
		}
	}
	else if(strncmp(type,"MsgQueryResp",32) ==0)
	{
		msg->type = MsgQueryResp;
	}
	else if(strncmp(type,"MsgHeartbeat",32) ==0)
	{
		msg->type = MsgHeartbeat;
	}
	else if(strncmp(type,"MsgHeartbeatResp",32) ==0)
	{
		msg->type = MsgHeartbeatResp;
	}
	else 
	{
		goto fail;
	}
	pbc_rmessage_delete(m);
	return 0;
fail:
	pbc_rmessage_delete(m);
	return -1;
	

}



