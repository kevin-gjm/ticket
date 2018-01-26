#include "usage.h"
#include <stdio.h>
#include <getopt.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>







void show_usage()
{
    fprintf(stdout, "\n");
    fprintf(stdout, "Usage:\n");
    fprintf(stdout, "  sqlstore [-d | -P DB_PATH | -H HOST | -s SIZE | -i PID_FILE | -g]\n");
    fprintf(stdout, "  sqlstore start --id ID [-d | -P DB_PATH | -H HOST | -p PORT | -t PORT | -s SIZE | -i PID_FILE | -g]\n");
    fprintf(stdout, "  sqlstore join PEER --id ID [-d | -P DB_PATH | -H HOST | -p PORT | -t PORT | -s SIZE | -i PID_FILE | -g]\n");
    fprintf(stdout, "  sqlstore leave [-P DB_PATH | -g]\n");
    fprintf(stdout, "  sqlstore drop [-P DB_PATH | -g]\n");
    fprintf(stdout, "  sqlstore --version\n");
    fprintf(stdout, "  sqlstore --help\n");
    fprintf(stdout, "\n");
	fprintf(stdout, "Commands:\n");
    fprintf(stdout, "  --start                  Destroy database and create a new cluster\n");
    fprintf(stdout, "  --join                   Destroy database and join cluster via peer\n");
    fprintf(stdout, "  --leave                  Destroy database and leave cluster\n");
    fprintf(stdout, "  --drop                   Destroy database\n");
	fprintf(stdout, "  --restart                Use old database, log and create a new cluster\n");
	fprintf(stdout, "  --rejoin                 Rejoin the cluster\n");
    fprintf(stdout, "\n");
    fprintf(stdout, "Options:\n");
    fprintf(stdout, "  -d --daemonize           Run as a daemon.\n");
	fprintf(stdout, "  -c --cluster_ip C_IP     Cluster ip.\n");
	fprintf(stdout, "  -r --cluster_port C_PORT Cluster port.\n");
    fprintf(stdout, "  -I --id ID               This server's manually set Raft ID\n");
    fprintf(stdout, "  -P --path DB_PATH        Path where database files will be kept [default: store]\n");
    fprintf(stdout, "  -H --host HOST           Host to listen on [default: 127.0.0.1]\n");
    fprintf(stdout, "  -p --raft_port PORT      Port for Raft peer traffic [default: 9000]\n");
    fprintf(stdout, "  -z --zmq_port PORT       Port for ZMQ traffic [default: 8000]\n");
    fprintf(stdout, "  -s --db_size SIZE        Size of database in megabytes [default: 1000]\n");
    fprintf(stdout, "  -i --pid_file PID_FILE   Pid file [default: /var/run/pearl.pid]\n");
    fprintf(stdout, "  -g --debug               Switch on debugging mode\n");
    fprintf(stdout, "  -v --version             Display version.\n");
    fprintf(stdout, "  -h --help                Prints a short usage summary.\n");
    fprintf(stdout, "\n");


}

int parse_options(int argc, char **argv, options_t* options)
{
	memset(options,0,sizeof(options_t));
	options->db_size = 1000;
	snprintf(options->host,32,"127.0.0.1");
	//options->server_port = 8000;
	snprintf(options->path,64,"store");
   	snprintf(options->pid_file,64,"/var/run/pearl.pid");
	//options->raft_port = 9000;
	
	const char*  short_options = "dc:r:I:P:H:p:z:s:i:gvh";
	struct option long_options[] = {
			{ "daemonize", 		0,	NULL,	'd' },
			{ "cluster_ip",		1,	NULL,	'c' },
			{ "cluster_port",	1,	NULL,	'r' },
			{ "id",				1,	NULL,	'I' },
			{ "path", 			1,	NULL,	'P' },
			{ "host",			1,	NULL,	'H' },
			{ "raft_port",		1,	NULL,	'p' },
			{ "zmq_port",		1,	NULL,	'z' },
			{ "db_size",		1,	NULL,	's' },
			{ "pid_file", 		1,	NULL,	'i' },
			{ "debug",			0,	NULL,	'g' },
			{ "version",		0,	NULL,	'v' },
			{ "help",			0,	NULL,	'h' },
			{ "start", 			0,	NULL,	'A' },
			{ "join",			0,	NULL,	'B' },
			{ "leave",			0,	NULL,	'C' },
			{ "drop",			0,	NULL,	'D' },
			{ "restart",		0,	NULL,	'E' },
			{ "rejoin",			0,	NULL,	'F' },
			{ 0,	0,	0,	0 },
	};
	int c;
		while ((c = getopt_long(argc, argv, short_options, long_options, NULL)) != -1)
		{
			switch (c)
			{
			case 'd':
				options->daemonize=1;
				if (daemon(1, 1) < 0)
				{
					fprintf(stderr," daemon error!%d\n", errno);
					return -1;
				}
				break;
			case 'c':
				if (strlen(optarg) < 32)
				{
					snprintf(options->PEER_IP, 32, "%s", optarg);
				}
				else
				{
					fprintf(stderr, " PERR IP too long!\n");
					return -1;
				}
				break;
			case 'r':
				options->PEER_PORT = atoi(optarg);
				break;
			case 'I':
				options->id = atoi(optarg);
				break;
			case 'P':
				if (strlen(optarg) < 64)
				{
					snprintf(options->path,64, "%s", optarg);
				}
				else
				{
					fprintf(stderr, "path arg too long!\n");
					return -1;
				}
				break;
			case 'H':
				if (strlen(optarg) < 32)
				{
					snprintf(options->host,32, "%s", optarg);
				}
				else
				{
					fprintf(stderr, "Host IP too long!\n");
					return -1;
				}
				break;
			case 'p':
				options->raft_port = atoi(optarg);
				break;
			case 'z':
				options->server_port = atoi(optarg);
				break;
			case 's':
				options->db_size = atoi(optarg);
				break;
			case 'i':
				if (strlen(optarg) < 64)
				{
					snprintf(options->pid_file,64, "%s", optarg);
				}
				else
				{
					fprintf(stderr, "Pid file path too long!\n");
					return -1;
				}
				break;
			case 'g':
				options->debug = 1;
				break;
			case 'v':
				options->version = 1;
				break;
			case 'h':
				options->help=1;
				break;
			case 'A':
				options->start = 1;
				break;
			case 'B':
				options->join = 1;
				break;
			case 'C':
				options->leave = 1;
				break;
			case 'D':
				options->drop = 1;
				break;
			case 'E':
				options->restart = 1;
				break;
			case 'F':
				options->rejoin = 1;
				break;
			default:
				fprintf(stdout, "paramer error!\n"); 
				show_usage();
				return -1;
			}
		}
	
    return 0;
}

void change_with_the_old(options_t* option, options_t* old)
{
	if(strlen(option->host)==0)
	{
		sprintf(option->host,"%s",old->host);
	}
	if(option->raft_port==0)
		option->raft_port = old->raft_port;
	if(option->PEER_PORT==0)
		option->PEER_PORT=old->PEER_PORT;
	if(strlen(option->PEER_IP)==0)
		sprintf(option->PEER_IP,"%s",old->PEER_IP);
	if(option->server_port==0)
		option->server_port = old->server_port;
	if(option->id==0)
		option->id = old->id;
}


void printf_option(options_t* option)
{
	printf("-----------------------------------------------\n");
	printf("id:%d\n",option->id);
	printf("peer ip:%s,port:%d\n",option->PEER_IP,option->PEER_PORT);
	printf("raft ip:%s,port:%d\n",option->host,option->raft_port);
	printf("server ip:%s,port:%d\n",option->host,option->server_port);
	printf("-----------------------------------------------\n");
	
}

