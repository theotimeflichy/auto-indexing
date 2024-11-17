#include "postgres.h"
#include "fmgr.h"
#include "executor/executor.h"
#include "utils/elog.h"
#include "utils/timestamp.h"
#include "access/xact.h"
#include "commands/trigger.h"
#include "utils/builtins.h"
#include "string.h"
#include "pthread.h"
#include <time.h>
#include <unistd.h>
#include "executor/spi.h"
#include "commands/defrem.h"
#include "utils/elog.h"

PG_MODULE_MAGIC;

static ExecutorStart_hook_type prev_ExecutorStart = NULL;

void _PG_init(void);
void _PG_fini(void);
static void startExec(QueryDesc *queryDesc, int eflags);
static void log_query(const char *query_text, CmdType type);
int recording = 0;

/**
 * Function called to the init of plugin.
 * We setup the log hook.
 */
void _PG_init(void) {
    prev_ExecutorStart = ExecutorStart_hook;
    ExecutorStart_hook = startExec;
}

/**
 * Function called when the plugin is unloading.
 * We unsetup the log hook.
 */
void _PG_fini(void) {
    ExecutorStart_hook = prev_ExecutorStart;
}

/**
 * Function called to every query (hook).
 *
 * @param queryDesc
 * @param eflags
 */
static void startExec(QueryDesc *queryDesc, int eflags) {

    if (strstr(queryDesc->sourceText, "query_log") == NULL) {
        log_query(queryDesc->sourceText, queryDesc->operation);
    }

    if (prev_ExecutorStart) {
        prev_ExecutorStart(queryDesc, eflags);
    } else {
        standard_ExecutorStart(queryDesc, eflags);
    }

}

/**
 * Function which permit to remove specific char.
 *
 * @param str char* the string where we want to remove char
 * @param toRemove char the char we want to remove
 */
void removeChar(char *str, char c) {

    int i, j = 0;
    int len = strlen(str);

    for (i = 0; i < len; i++) {
        if (str[i] != c) {
            str[j++] = str[i];
        }
    }

    str[j] = '\0';
}

/**
 * Add a log in the database.
 *
 * @param query_type the type of query (e.g. INSERT, UPDATE, ...)
 * @param table_name the table name
 * @param clause_type the type of clause (e.g. GROUPE BY, ..)
 * @param clause_name the name of clause (e.g. name, ..)
 */
static void add_query_log(const char *query_type, const char *table_name, const char *clause_type, const char *clause_name) {
    StringInfoData buf;
    initStringInfo(&buf);
    appendStringInfo(&buf, "INSERT INTO query_log (query_type, table_name, clause_type, clause_name, log_time) VALUES ('%s', '%s', '%s', '%s', now());",
                     query_type ? query_type : "UNKNOWN",
                     table_name ? table_name : "UNKNOWN",
                     clause_type ? clause_type : "UNKNOWN",
                     clause_name ? clause_name : "UNKNOWN");

    if (query_type) {
        SPI_connect();
        SPI_execute(buf.data, false, 0);
        SPI_finish();
    }
}

/**
 * Process query to extract data from the delimiter
 * (WHERE, GROUP BY, ORDER BY)
 *
 * @param query_type the type of query (e.g. INSERT, UPDATE, ...)
 * @param table_name the table name
 * @param clause the part of the query
 * @param delimiter WHERE | GROUP BY | ORDER BY | other is possible
 */
static void process_clause(const char *query_type, char *table_name, char *clause, char *delimiter) {

    clause += strlen(delimiter);
    char *token = strtok((char *)clause, ",");
    while (token != NULL) {
        while (*token == ' ') token++;
        char *end = token;
        while (*end != ' ' && *end != '=' && *end != '<' && *end != ';' && *end != '>' && *end != '\0') end++;
        *end = '\0';

        add_query_log(query_type, table_name, delimiter, token);

        token = strtok(NULL, ",");
    }
}

/**
 * This function is called whenever a new query is executed.
 *
 * @param query_text the query.
 */
static void log_query(const char *query_text, CmdType type) {

    const char *query_type = NULL;
    const char *table_name = NULL;
    const char *where_clause = strstr(query_text, "WHERE");
    const char *order_by_clause = strstr(query_text, "ORDER BY");
    const char *group_by_clause = strstr(query_text, "GROUP BY");

    // We select the query type.
    if (type == CMD_SELECT) {
        query_type = "SELECT";
        table_name = strstr(query_text, "FROM");
    } else if (type == CMD_UPDATE) {
        query_type = "UPDATE";
        table_name = query_text + 7;
    } else if (type == CMD_DELETE) {
        query_type = "DELETE";
        table_name = strstr(query_text, "FROM");
    } else if (type == CMD_INSERT) {
        query_type = "INSERT";
        table_name = strstr(query_text, "INTO");
    } else {
        return;
    }

    // We get the name of the table.
    if (table_name) {
        table_name += (strncasecmp(query_type, "INSERT", 6) == 0) ? 5 : 5;
        while (*table_name == ' ') table_name++;
        char table_name_buf[256];
        int i = 0;
        while (table_name[i] != ' ' && table_name[i] != '\0' && table_name[i] != ';' && table_name[i] != ',' && i < 255) {
            if ((unsigned char)table_name[i] >= 32 && (unsigned char)table_name[i] <= 126) {
                table_name_buf[i] = table_name[i];
                i++;
            } else {
                break;
            }
        }
        table_name_buf[i] = '\0';

        int len = strlen(table_name_buf);
        if (len >= 2 && table_name_buf[len - 1] == '\x1C' && table_name_buf[len - 2] == 'V') {
            table_name_buf[len - 2] = '\0';
        }

        char *backslash_pos = strchr(table_name_buf, '\\');
        if (backslash_pos != NULL) {
            *backslash_pos = '\0';
        }

        table_name = table_name_buf;
    } else {
        return;
    }

    if (where_clause) process_clause(query_type, table_name, where_clause, "WHERE");
    if (order_by_clause) process_clause(query_type, table_name, order_by_clause, "ORDER BY");
    if (group_by_clause) process_clause(query_type, table_name, group_by_clause, "GROUP BY");

}

/**
 * Initial function of the auto indexing plugin.
 *
 * @param fcinfo
 * @return
 */
Datum auto_indexing(PG_FUNCTION_ARGS) {
    ereport(INFO, (errmsg("Auto Indexing plugin is ready to be use !")));
    PG_RETURN_VOID();
}

/**
 * Start an audit for a precise amount of time.
 *
 * @param fcinfo time in second that the audit will last.
 * @return NULL
 */
Datum audit(PG_FUNCTION_ARGS) {

    recording = 1;

    int time_arg = PG_GETARG_INT32(0);

    StringInfoData buf;
    initStringInfo(&buf);
    appendStringInfo(&buf,
                     "SELECT cron.schedule("
                     " 'audit_end_task', "
                     " '%d seconds', "
                     " $$SELECT audit_end(); DELETE FROM cron.job WHERE jobname = 'audit_end_task';$$ "
                     ");",
                     time_arg);

    SPI_connect();
    SPI_execute(buf.data, false, 0);
    SPI_finish();

    PG_RETURN_VOID();
}

/**
 * This function is calculating and removing every unuseful index.
 *
 * Is called an unuseful index every which respect the following :
 *  - a non unique index
 *  - the index is used in less than 10% of requests.
 */
void remove_index() {

    StringInfoData buf;
    initStringInfo(&buf);
    appendStringInfo(&buf,
                     "SELECT i.schemaname, i.relname, i.indexrelname "
                     "FROM pg_stat_user_indexes i "
                     "JOIN pg_stat_user_tables t ON i.relname = t.relname AND i.schemaname = t.schemaname "
                     "JOIN pg_index x ON i.indexrelid = x.indexrelid "
                     "WHERE x.indisunique = false "
                     "AND (i.idx_scan::float / NULLIF(t.seq_scan + t.idx_scan, 0)) < 0.1;"
    );
    SPI_connect();
    SPI_execute(buf.data, true, 0);

    for (int i = 0; i < SPI_processed; i++) {
        HeapTuple tuple = SPI_tuptable->vals[i];
        TupleDesc tupdesc = SPI_tuptable->tupdesc;

        char *schemaname = SPI_getvalue(tuple, tupdesc, 1);
        char *tablename = SPI_getvalue(tuple, tupdesc, 2);
        char *indexname = SPI_getvalue(tuple, tupdesc, 3);

        StringInfoData drop_cmd;
        initStringInfo(&drop_cmd);
        appendStringInfo(&drop_cmd, "DROP INDEX IF EXISTS %s.%s;", schemaname, indexname);

        int drop_ret = SPI_execute(drop_cmd.data, false, 0);
        if (drop_ret != SPI_OK_UTILITY) {
            ereport(WARNING, (errmsg("Failed to drop index : %s.%s", schemaname, indexname)));
        } else {
            ereport(INFO, (errmsg("Dropped index : %s.%s", schemaname, indexname)));
        }
    }

    // Terminer SPI
    SPI_finish();
}



/**
 * The function is launched at the end an audit.
 *
 * @param arg NULL
 * @return NULL
 */
Datum audit_end(PG_FUNCTION_ARGS) {

    recording = 0;

    ereport(INFO, (errmsg("Auto_Indexing : Starting analyzing to remove index.")));
    remove_index();
    ereport(INFO, (errmsg("Auto_Indexing : Ending analyzing to remove index.")));

    ereport(INFO, (errmsg("Auto_Indexing : Starting analyzing to add index.")));
    ereport(INFO, (errmsg("Auto_Indexing : Ending analyzing to add index.")));


    // We delete all logs.
    StringInfoData buf;
    initStringInfo(&buf);
    appendStringInfo(&buf, "DELETE FROM query_log;");
    SPI_connect();
    SPI_execute(buf.data, false, 0);
    SPI_finish();

    PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(auto_indexing);
PG_FUNCTION_INFO_V1(audit);
PG_FUNCTION_INFO_V1(audit_end);
