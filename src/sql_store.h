#ifndef SQL_STORE_H_
#define SQL_STORE_H_

#include <sqlite3.h>

#define DB_PATH "./htdata.db"

sqlite3 * store_open();
void store_close(sqlite3 *db);
int store_exec(const char* sql,int use_tran);

#endif //SQL_STORE_H_