#include "DatabaseEngine.h"
#include "ParseTree.h"
#include "Statistics.h"
#include "QueryPlanner.h"

extern "C" {
int yyparse(void); //defined in y.tab.c
}

extern struct FuncOperator *finalFunction;
extern struct TableList *tables;
extern struct AndList *boolean;
extern struct NameList *groupingAtts;
extern struct NameList *attsToSelect;
extern int distinctAtts;
extern int distinctFunc;

extern char *newtable;
extern char *oldtable;
extern char *newfile;
extern char *outputmode;
extern struct AttributeList *newAtts;

void DatabaseEngine::run()
{

    char *statsFile = "Statistics.txt";

    Statistics stats;
    QueryPlanner queryPlan(&stats);

    while (true)
    {

        if (yyparse() != 0)
        {
            cout << "Error in parsing CNF" << endl;
            continue;
        }

        stats.Read(statsFile);
    }
}