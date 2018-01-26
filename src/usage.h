#ifndef USAGE_H_
#define  USAGE_H_

typedef struct
{
    /* commands */
    int drop;
    int join;
    int leave;
    int start;
	int restart;
	int rejoin;

    /* flags */
    int daemonize;
    int debug;
    int help;
    int version;

    /* options */
    int  db_size;
    char host[32];
    int  server_port;
    int  id;
    char path[64];
    char pid_file[64];
    int  raft_port;

    /* arguments */
    char PEER_IP[32];
	int  PEER_PORT;

} options_t;

int parse_options(int argc, char **argv, options_t* options);
void show_usage();
void change_with_the_old(options_t* option, options_t* old);
void printf_option(options_t* option);

#endif //USAGE_H_