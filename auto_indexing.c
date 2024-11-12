#include "postgres.h"
#include "fmgr.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(auto_indexing);

/**
 * Initial function of the auto indexing plugin.
 * @param fcinfo
 * @return
 */
Datum auto_indexing(PG_FUNCTION_ARGS)
{
    ereport(INFO, (errmsg("Auto Indexing plugin is ready to be use !")));
    PG_RETURN_VOID();
}
