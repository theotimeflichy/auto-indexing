#include "postgres.h"
#include "fmgr.h"
#include "executor/executor.h"
#include "utils/elog.h"
#include "utils/timestamp.h"
#include "access/xact.h"
#include "commands/trigger.h"
#include "utils/builtins.h"
#include "string.h"

PG_MODULE_MAGIC;

static ExecutorStart_hook_type prev_ExecutorStart = NULL;

void _PG_init(void);
void _PG_fini(void);
static void startExec(QueryDesc *queryDesc, int eflags);
static void log_query(const char *query_text, CmdType type);

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
 *
 *
 * @param query_type
 * @param table_name
 * @param clause_type
 * @param clause
 */
static void add_query_log(const char *query_type, const char *table_name, const char *clause_type, const char *clause_name)
{
    StringInfoData buf;
    initStringInfo(&buf);
    appendStringInfo(&buf, "INSERT INTO query_log (query_type, table_name, clause_type, clause_name, log_time) VALUES ('%s', '%s', '%s', '%s', now());",
                     query_type ? query_type : "UNKNOWN",
                     table_name ? table_name : "UNKNOWN",
                     clause_type ? table_name : "UNKNOWN",
                     clause_name ? table_name : "UNKNOWN");

    if (query_type) {
        SPI_connect();
        SPI_execute(buf.data, false, 0);
        SPI_finish();
    }
}

/**
 * This function is called whenever a new query is executed.
 *
 * @param query_text the query.
 */
static void log_query(const char *query_text, CmdType type) {

    const char *query_type = NULL;
    const char *where_clause = strstr(query_text, "WHERE");
    const char *table_name = NULL;

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

    // For each condition, we add a new tuple.
    if (where_clause) {
        where_clause += 6;

        char *condition = strtok((char *)where_clause, "AND");

        while (condition != NULL) {
            while (*condition == ' ') condition++;
            char *end = condition;
            while (*end != ' ' && *end != '=' && *end != '<' && *end != '>' && *end != '\0') end++;
            *end = '\0';

            // We extract the attribute name.
            char extract[50];
            strcpy(extract, condition);
            char *clause_name = strtok(extract, " ");
            removeChar(clause_name, '\'');
            removeChar(clause_name, ';');

            add_query_log(query_type, table_name, "WHERE", clause_name);

            condition = strtok(NULL, "AND");
        }
    }
}

/**
 * Initial function of the auto indexing plugin.
 *
 * @param fcinfo
 * @return
 */
Datum auto_indexing(PG_FUNCTION_ARGS)
{
    ereport(INFO, (errmsg("Auto Indexing plugin is ready to be use !")));
    PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(auto_indexing);
