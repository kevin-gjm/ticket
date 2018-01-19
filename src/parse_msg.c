#include "parse_msg.h"
#include <stdlib.h>
#include <string.h>
#include <assert.h>



int parse_msg_init()
{
	struct pbc_slice slice;
	read_file(RAFTPB_FILE, &slice);
	if (slice.buffer == NULL)
		return 1;
	g_pb_env = pbc_new();
	int r = pbc_register(g_pb_env, &slice);
	if (r) {
		printf("Error : %s", pbc_error(g_pb_env));
		exit(1);
	}

	free(slice.buffer);
}
int parse_msg_free()
{
	pbc_delete(g_pb_env);
}

struct pbc_wmessage * parse_msg_write(struct pbc_env * env,msg_t* msg)
{
	struct pbc_wmessage * pb_msg = pbc_wmessage_new(env, "raftpb.RaftMessage");
	
	
	pbc_wmessage_integer(pb_msg, "type", msg->type,0);
	switch(msg->type)
	{
	case MSG_HANDSHAKE:
		{
			struct pbc_wmessage * hs = pbc_wmessage_message(pb_msg , "hs");
			pbc_wmessage_integer(hs, "raft_port", msg->hs.raft_port,0);
			pbc_wmessage_integer(hs, "server_port", msg->hs.server_port,0);
			pbc_wmessage_integer(hs, "node_id", msg->hs.node_id,0);
		}
		break;
	case MSG_HANDSHAKE_RESPONSE:
		{
			struct pbc_wmessage * hsr = pbc_wmessage_message(pb_msg , "hsr");
			pbc_wmessage_integer(hsr, "success", msg->hsr.success,0);
			pbc_wmessage_integer(hsr, "leader_port", msg->hsr.leader_port,0);
			pbc_wmessage_integer(hsr, "server_port", msg->hsr.server_port,0);
			pbc_wmessage_integer(hsr, "node_id", msg->hsr.node_id,0);
			pbc_wmessage_string(hsr , "leader_host" , msg->hsr.leader_host , strlen(msg->hsr.leader_host));
		}
		break;
	case MSG_LEAVE:
		break;
	case MSG_LEAVE_RESPONSE:	
		break;
		
	case MSG_REQUESTVOTE:
		{
			struct pbc_wmessage * rv = pbc_wmessage_message(pb_msg , "rv");
			pbc_wmessage_integer(rv, "term", msg->rv.term,0);
			pbc_wmessage_integer(rv, "candidate_id", msg->rv.candidate_id,0);
			pbc_wmessage_integer(rv, "last_log_index", msg->rv.last_log_idx,0);
			pbc_wmessage_integer(rv, "last_log_term", msg->rv.last_log_term,0);
		}
		break;
	case MSG_REQUESTVOTE_RESPONSE:
		{
			struct pbc_wmessage * rvr = pbc_wmessage_message(pb_msg , "rvr");
			pbc_wmessage_integer(rvr, "term", msg->rvr.term,0);
			pbc_wmessage_integer(rvr, "vote_granted", msg->rvr.vote_granted,0);
		}
		break;
	case MSG_APPENDENTRIES:
		{
			struct pbc_wmessage * ae = pbc_wmessage_message(pb_msg , "ae");
			pbc_wmessage_integer(ae, "term", msg->ae.term,0);
			pbc_wmessage_integer(ae, "prev_log_index", msg->ae.prev_log_idx,0);
			pbc_wmessage_integer(ae, "prev_log_term", msg->ae.prev_log_term,0);
			pbc_wmessage_integer(ae, "leader_commit", msg->ae.leader_commit,0);
			pbc_wmessage_integer(ae, "n_entries", msg->ae.n_entries,0);
			struct pbc_wmessage * entry;
			struct pbc_wmessage * data;
			for(int i=0;i<msg->ae.n_entries;i++)
			{
				entry = pbc_wmessage_message(ae , "entries");
				pbc_wmessage_integer(entry,"term", msg->ae.entries[i].term,0);
				pbc_wmessage_integer(entry,"id", msg->ae.entries[i].id,0);
				pbc_wmessage_integer(entry,"type", msg->ae.entries[i].type,0);
				data = pbc_wmessage_message(entry , "data");
				pbc_wmessage_string(data,"buf",msg->ae.entries[i].data.buf,msg->ae.entries[i].data.len);
				pbc_wmessage_integer(data,"len", msg->ae.entries[i].data.len,0);
			}
			
		}
		break;
	case MSG_APPENDENTRIES_RESPONSE:
		{
			struct pbc_wmessage * aer = pbc_wmessage_message(pb_msg , "aer");
			pbc_wmessage_integer(aer, "term", msg->aer.term,0);
			pbc_wmessage_integer(aer, "success", msg->aer.success,0);
			pbc_wmessage_integer(aer, "current_index", msg->aer.current_idx,0);
			pbc_wmessage_integer(aer, "first_index", msg->aer.first_idx,0);
		}
		break;
	default:
		printf("error not have this msg type:%d\n",msg->type);
		pbc_wmessage_delete(pb_msg);
		exit(1);
	
	}
	
	return pb_msg;
}



int parse_msg_read(struct pbc_env* env, struct pbc_slice* slice,msg_t* msg,msg_entry_t* entry)
{
	if(msg==NULL) return -1;
	struct pbc_rmessage * m = pbc_rmessage_new(env, "raftpb.RaftMessage", slice);
	if (m==NULL) {
		printf("Error : %s",pbc_error(env));
		return -1;
	}

	msg->type = pbc_rmessage_integer(m , "type" , 0 , NULL);
	
	switch(msg->type)
	{
	case MSG_HANDSHAKE:
		{
			struct pbc_rmessage * hs = pbc_rmessage_message(m , "hs", 0);
			msg->hs.raft_port =  pbc_rmessage_integer(hs , "raft_port" , 0 , NULL);
			msg->hs.server_port =  pbc_rmessage_integer(hs , "server_port" , 0 , NULL);
			msg->hs.node_id =  pbc_rmessage_integer(hs , "node_id" , 0 , NULL);
		}
		break;
	case MSG_HANDSHAKE_RESPONSE:
		{
			struct pbc_rmessage * hsr = pbc_rmessage_message(m , "hsr", 0);
			msg->hsr.success =  pbc_rmessage_integer(hsr , "success" , 0 , NULL);
			msg->hsr.leader_port =  pbc_rmessage_integer(hsr , "leader_port" , 0 , NULL);
			msg->hsr.server_port =  pbc_rmessage_integer(hsr , "server_port" , 0 , NULL);
			msg->hsr.node_id =  pbc_rmessage_integer(hsr , "node_id" , 0 , NULL);
			const char * host = pbc_rmessage_string(hsr , "leader_host" , 0 , NULL);
			if(strlen(host)>0)
			{
				sprintf(msg->hsr.leader_host,"%s",host);
			}
		}
		break;
	case MSG_LEAVE:
		break;
	case MSG_LEAVE_RESPONSE:
		break;
	case MSG_REQUESTVOTE:
		{
			struct pbc_rmessage * rv = pbc_rmessage_message(m , "rv", 0);
			msg->rv.candidate_id = pbc_rmessage_integer(rv , "candidate_id" , 0 , NULL);
			msg->rv.last_log_idx = pbc_rmessage_integer(rv , "last_log_index" , 0 , NULL);
			msg->rv.last_log_term = pbc_rmessage_integer(rv , "last_log_term" , 0 , NULL);
			msg->rv.term = pbc_rmessage_integer(rv , "term" , 0 , NULL);
		}
		break;
	case MSG_REQUESTVOTE_RESPONSE:
		{
			struct pbc_rmessage * rvr = pbc_rmessage_message(m , "rvr", 0);
			msg->rvr.term = pbc_rmessage_integer(rvr , "term" , 0 , NULL);
			msg->rvr.vote_granted = pbc_rmessage_integer(rvr , "vote_granted" , 0 , NULL);
		}
		break;
	case MSG_APPENDENTRIES:
		{
			struct pbc_rmessage * ae = pbc_rmessage_message(m , "ae", 0);
			msg->ae.leader_commit = pbc_rmessage_integer(ae , "leader_commit" , 0 , NULL);
			msg->ae.prev_log_idx = pbc_rmessage_integer(ae , "prev_log_index" , 0 , NULL);
			msg->ae.prev_log_term = pbc_rmessage_integer(ae , "prev_log_term" , 0 , NULL);
			msg->ae.term = pbc_rmessage_integer(ae , "term" , 0 , NULL);
			msg->ae.n_entries = pbc_rmessage_integer(ae , "n_entries" , 0 , NULL);	

			if(msg->ae.n_entries==1)
			{
				if(entry==NULL)
					goto fail;
				struct pbc_rmessage * entries = pbc_rmessage_message(ae, "entries", 0);
				entry->id = pbc_rmessage_integer(entries , "id" , 0 , NULL);
				entry->term = pbc_rmessage_integer(entries , "term" , 0 , NULL);
				entry->type = pbc_rmessage_integer(entries , "type" , 0 , NULL);
			
				struct pbc_rmessage * data = pbc_rmessage_message(entries, "data", 0);
				entry->data.len = pbc_rmessage_integer(data , "len" , 0 , NULL);

			
				unsigned int sz;
				const char* buff = pbc_rmessage_string(data,"buf",0,&sz);
				if(sz != entry->data.len)
				{
					printf("error,receive buf len not fix the buf\n");
					goto fail;
				}
				else
				{
					entry->data.buf = calloc(1,entry->data.len);
					memcpy(entry->data.buf,buff,sz);
				}
				msg->ae.entries = entry;
			
			}
			else if(msg->ae.n_entries > 1)
			{
				printf("WARNNING, not support now! Just send one entry one time!\n");
			}
		}
		break;
	case MSG_APPENDENTRIES_RESPONSE:
		{
			struct pbc_rmessage * aer = pbc_rmessage_message(m , "aer", 0);
			msg->aer.current_idx = pbc_rmessage_integer(aer , "current_index" , 0 , NULL);
			msg->aer.first_idx = pbc_rmessage_integer(aer , "first_index" , 0 , NULL);
			msg->aer.success = pbc_rmessage_integer(aer , "success" , 0 , NULL);
			msg->aer.term = pbc_rmessage_integer(aer , "term" , 0 , NULL);
		}
		break;
	default:
		printf("Error not have this type: %d",msg->type);
		goto fail;
	}
	
	
	pbc_rmessage_delete(m);
	return 0;
fail:
	pbc_rmessage_delete(m);
	return -1;
}

struct pbc_wmessage * parse_entry_control_write(struct pbc_env * env,msg_entry_t* ety)
{
	struct pbc_wmessage * entry = pbc_wmessage_new(env, "raftpb.Entry");

	pbc_wmessage_integer(entry,"term",ety->term,0);
	pbc_wmessage_integer(entry,"id", ety->id,0);
	pbc_wmessage_integer(entry,"type",ety->type,0);
	return entry;
}

int parse_entry_control_read(struct pbc_env* env, struct pbc_slice* slice,msg_entry_t* entry)
{
	if(entry==NULL) return -1;
	struct pbc_rmessage * m = pbc_rmessage_new(env, "raftpb.Entry", slice);
	if (m==NULL) {
		printf("Error : %s",pbc_error(env));
		return -1;
	}
	entry->id = pbc_rmessage_integer(m , "id" , 0 , NULL);
	entry->term = pbc_rmessage_integer(m , "term" , 0 , NULL);
	entry->type = pbc_rmessage_integer(m , "type" , 0 , NULL);	
	pbc_rmessage_delete(m);
	return 0;
}

int test()
{
	parse_msg_init();
	
	msg_t msg;
	msg_entry_t entry;
	
	msg.type = MSG_APPENDENTRIES;
	msg.ae.leader_commit =10;
	msg.ae.n_entries = 1;
	msg.ae.prev_log_idx = 11;
	msg.ae.prev_log_term = 12;
	msg.ae.term = 13;
	
	entry.id = 14;
	entry.term = 15;
	entry.type = RAFT_LOGTYPE_ADD_NODE;
	char buff[64]= "world";
	entry.data.buf = buff;
	entry.data.len = strlen(buff);
	msg.ae.entries = &entry;

	
	struct pbc_wmessage* msg_pb = parse_msg_write(g_pb_env, &msg);

	msg_t msg_r;
	msg_entry_t entry_r;

	struct pbc_slice slice;

	pbc_wmessage_buffer(msg_pb, &slice);
	
	int ret =  parse_msg_read(g_pb_env, &slice,&msg_r,&entry_r);

	if(ret != 0)
	{
		printf("error!\n");
	}

	assert(msg_r.type == MSG_APPENDENTRIES);
	assert(msg_r.ae.leader_commit ==10);
	assert(msg_r.ae.n_entries == 1);
	assert(msg_r.ae.prev_log_idx == 11);
	assert(msg_r.ae.prev_log_term == 12);
	assert(msg_r.ae.term == 13);

	assert(msg_r.ae.entries[0].id==14);
	assert(msg_r.ae.entries[0].term==15);
	assert(msg_r.ae.entries[0].type==RAFT_LOGTYPE_ADD_NODE);
	assert(msg_r.ae.entries[0].data.len==strlen(buff));
	assert(strncmp(msg_r.ae.entries[0].data.buf,buff,msg_r.ae.entries[0].data.len)==0);
		
			
	free(msg_r.ae.entries[0].data.buf);
	
	pbc_wmessage_delete(msg_pb);
	
	parse_msg_free();
}




