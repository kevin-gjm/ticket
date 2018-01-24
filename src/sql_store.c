#include "sql_store.h"
#include <stddef.h>
#include <string.h>
#include <stdio.h>


typedef enum
{
	TRANSACTION_BEGIN = 0,
	TRANSACTION_COMMIT,
	TRANSACTION_ROLLBACK,
}tran_type_e;

sqlite3 * store_open()
{
	sqlite3 *db;

	if(SQLITE_OK != sqlite3_open(DB_PATH, &db))
	{
		printf("open sqlite error! %s\n",sqlite3_errmsg(db));
		return NULL;
	}
	return db;
}

void store_close(sqlite3 *db)
{
	if(db)
	{
		sqlite3_close(db);
		db = NULL;
	}
}

int Transaction(sqlite3 *db,tran_type_e type)
{
	const char* sql;
	if(type==TRANSACTION_BEGIN)
	{
		sql ="begin transaction;";
	}
	else if (type == TRANSACTION_COMMIT)
	{
		sql ="commit transaction;";
	}
	else if(type == TRANSACTION_ROLLBACK)
	{
		sql ="rollback transaction;";
	}
	else
	{
		//LOG_ERR<<"Unidentification transaction type ! type = " << type;
		return -1;
	}
	char * errmsg;
	if (sqlite3_exec(db, sql, NULL, NULL, &errmsg)!=SQLITE_OK)
	{
		//LOG_ERR<<"transaction type:"<<type <<" exec failure! Reason:"<<errmsg;
		return -1;
	}
	sqlite3_free(errmsg);


	//LOG_INFO<<"transaction type:"<<type <<" exec ok!";
	return 0;
}



int store_exec(const char* sql,int use_tran)
{
	sqlite3 * db = store_open();
	if(db==NULL)
		return -1;
	if(use_tran)
	{
		if(Transaction(db,TRANSACTION_BEGIN)<0)
			goto fail;
	}
	sqlite3_stmt *stmt;
	int rc = sqlite3_prepare_v2(db,sql, strlen(sql), &stmt, NULL);
	if(SQLITE_OK != rc)
	{
		if(stmt)
			sqlite3_finalize(stmt);
		//LOG_ERR<<"Exec prepare failure! "<< rc;
		goto fail;
	}
	if((rc =sqlite3_step(stmt)) != SQLITE_DONE)
	{
		if(stmt)
			sqlite3_finalize(stmt);
		//LOG_ERR<<"Exec failure! "<< rc;
		goto fail;
	}
	sqlite3_finalize(stmt);
	
	if(use_tran)
	{
		if(Transaction(db,TRANSACTION_COMMIT)<0)
		{
			Transaction(db,TRANSACTION_ROLLBACK);
			//LOG_ERR<<"Not committed error occurred";
			goto fail;
		}
	}
	store_close(db);
	return 0;
fail:
		store_close(db);
		return -1;

}

