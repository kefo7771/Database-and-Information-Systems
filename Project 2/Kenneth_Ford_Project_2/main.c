#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "config.h"
#include <cassandra.h>
#include "lex.h"

char *readline(char *prompt);

static int tty = 0;

static void cli_about()
{
	printf("You executed a command!\n");
}

static void cli_help()
{
	printf("Commands: \n");
	printf("    show\n");
    printf("    list\n");
	printf("    use\n");
    printf("        Enter <keyspace> <table>\n");
	printf("    get\n");
    printf("        Enter <key>\n");
	printf("    insert\n");
    printf("        Enter <key> <value>\n");
	return;
}

CassError handleQuery(CassSession* session, CassStatement* query)
{
    printf("\n");
    //printf("Inside handleQuery\n");
    //initialize error status and future here
    CassError possibleError = CASS_OK;
    CassFuture* future = NULL;

    //printf("Initialized error, future\n");
    //reassign future as attempted execution of the query statement
    future = cass_session_execute(session,query);

    //printf("Query executed\n");
    //wait for the current future to work or return an error
    cass_future_wait(future);

    possibleError = cass_future_error_code(future);

    if(possibleError != CASS_OK)
    {
        printf("Error occurred\n");
        const char* errorMessage;
        size_t message_length;
        cass_future_error_message(future,&errorMessage,&message_length);
        fprintf(stderr, "Error connecting '%.*s'\n",(int)message_length,errorMessage);
    }
    else
    {

        const CassResult* result = cass_future_get_result(future);
        //printf("Got Result\n");

        CassIterator* row_iterator = cass_iterator_from_result(result);
        while(cass_iterator_next(row_iterator))
        {
            const CassRow* row = cass_iterator_get_row(row_iterator);
            const CassValue* value = cass_row_get_column(row,0);

            const char* resultString;
            size_t result_STRLEN;

            cass_value_get_string(value,&resultString,&result_STRLEN);
            printf("%s\n",resultString);
        }

        cass_result_free(result);

    }
    cass_future_free(future);
    cass_statement_free(query);
    printf("\n");
}

static void cli_show(CassSession* session)
{
    //printf("In cli_show \n");
    CassStatement* query = cass_statement_new("SELECT keyspace_name FROM system_schema.keyspaces;",0);
    handleQuery(session,query);
    cass_statement_free(query);
    return;
}

static void cli_list(CassSession* session)
{
    printf("In cli_list \n");
    CassStatement* query = cass_statement_new("SELECT table_name FROM system_schema.tables WHERE keyspace_name = 'demodb';",0);
    handleQuery(session,query);
    return;
}

static void cli_use(CassSession* session,CassCluster* cluster,char* arg1, char* arg2)
{
    //printf("In cli_use\n");
    CassError possibleError = CASS_OK;
    CassFuture* future = NULL;
    //printf("arg1: %s\n",arg1);
    //printf("arg2: %s\n",arg2);

    char query[BUFSIZE];
    snprintf(query,BUFSIZE,"USE %s;",arg1);
    CassStatement* statement = cass_statement_new(query,0);

    future = cass_session_execute(session,statement);

    CassError error = cass_future_error_code(future);
    cass_future_wait(future);
    if(error != CASS_OK)
    {
        printf("Error occurred in use\n");
        const char* errorMessage;
        size_t message_length;
        cass_future_error_message(future,&errorMessage,&message_length);
        fprintf(stderr, "Error connecting '%.*s'\n",(int)message_length,errorMessage);
    }
    else
    {
        printf("Using keyspace %s and table %s\n",arg1,arg2);
        CassFuture* linkFuture = cass_session_connect_keyspace(session,cluster,arg1);
        CassError linkError = cass_future_error_code(linkFuture);
        cass_future_wait(linkFuture);
        if(error != CASS_OK)
        {
            printf("Error occurred in use\n");
            const char* errorMessage2;
            size_t message_length2;
            cass_future_error_message(future,&errorMessage2,&message_length2);
            fprintf(stderr, "Error connecting '%.*s'\n",(int)message_length2,errorMessage2);
        }
    }

    cass_statement_free(statement);
    cass_future_free(future);
    return;
}

static void cli_get(CassSession* session,char* arg1)
{
    printf("In cli_get \n");
    return;
}

static void cli_insert(CassSession* session,char* arg1,char* arg2)
{
    printf("In cli_insert \n");

    //CassStatement* statement = cass_statement_new("INSERT INTO ?")
    return;
}

void
cli()
{
	CassCluster* cluster;
	CassSession* session;

    cluster = cass_cluster_new();
	session = cass_session_new();
	cass_cluster_set_contact_points(cluster,"127.0.0.1");

	CassFuture* connection = cass_session_connect(session,cluster);


	char *cmdline = NULL;
	char cmd[BUFSIZE], prompt[BUFSIZE],arg1[BUFSIZE],arg2[BUFSIZE];
	int pos;

	tty = isatty(STDIN_FILENO);
	if (tty)
		cli_about();

	/* Main command line loop */
	for (;;) {
		if (cmdline != NULL) {
			free(cmdline);
			cmdline = NULL;
		}
		memset(prompt, 0, BUFSIZE);
		sprintf(prompt, "cassandra> ");

		if (tty)
			cmdline = readline(prompt);
		else
			cmdline = readline("");

		if (cmdline == NULL)
			continue;

		if (strlen(cmdline) == 0)
			continue;

		if (!tty)
			printf("%s\n", cmdline);

		if ((strcmp(cmdline, "?") == 0) || (strcmp(cmdline, "help") == 0)) {
			cli_help();
			continue;
		}
		if (strcmp(cmdline, "quit") == 0 ||
		    strcmp(cmdline, "q") == 0)
			break;

        if (strcmp(cmdline, "show") == 0) {
			cli_show(session);
			continue;
		}

        if (strcmp(cmdline, "list") == 0) {
			cli_list(session);
			continue;
		}

		memset(cmd, 0, BUFSIZE);
		pos = 0;
		nextarg(cmdline, &pos, " ", cmd);

		if (strcmp(cmd, "about") == 0 || strcmp(cmd, "a") == 0) {
			cli_about();
			continue;
		}
        if (strcmp(cmd, "use") == 0) {
            //printf("pos1: %d\n",pos);
			nextarg(cmdline,&pos,".",cmd);
			strncpy(arg1,cmd,BUFSIZE);
			//printf("arg1: %s\n",arg1);
			//printf("pos2: %d\n",pos);

			pos++;
			nextarg(cmdline,&pos,"\n",cmd);
			strncpy(arg2,cmd,BUFSIZE);
			//printf("arg2: %s\n",arg2);
            //printf("pos3: %d\n",pos);


			cli_use(session,cluster,arg1,arg2);
			continue;
		}

		if (strcmp(cmd, "get") == 0) {
			//printf("pos1: %d\n",pos);
			nextarg(cmdline,&pos,".",cmd);
			strncpy(arg1,cmd,BUFSIZE);
			//printf("arg1: %s\n",arg1);
			//printf("pos2: %d\n",pos);

			cli_get(session,arg1);
			continue;
		}

		if (strcmp(cmd, "insert") == 0) {
            //printf("pos1: %d\n",pos);
			nextarg(cmdline,&pos,".",cmd);
			strncpy(arg1,cmd,BUFSIZE);
			//printf("arg1: %s\n",arg1);
			//printf("pos2: %d\n",pos);

			pos++;
			nextarg(cmdline,&pos,"\n",cmd);
			strncpy(arg2,cmd,BUFSIZE);
			//printf("arg2: %s\n",arg2);
            //printf("pos3: %d\n",pos);

			cli_insert(session,arg1,arg2);
			continue;
		}
	}
    cass_session_free(session);
    cass_future_free(connection);
    cass_cluster_free(cluster);
}

int
main(int argc, char**argv)
{
	cli();
	exit(0);
}
