#include "DatabaseEngine.h"
#include "ParseTree.h"
#include "Statistics.h"
#include "QueryPlanner.h"
#include "CRUDHelper.h"

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
    CRUDHelper crud;

    QueryPlanner queryPlanner(&stats);

    while (true)
    {

        if (yyparse() != 0)
        {
            cout << "Error in parsing CNF" << endl;
            continue;
        }

        stats.Read(statsFile);

        if (newtable)
        {
            if (crud.createTable())
            {
                cout << "Created table " << newtable << endl;
            }
            else
            {
                cout << "Cannot create table " << endl;
            }
        }
        else if (oldtable && newfile)
        {
            if (crud.insertInto())
            {
                cout << "Insert into " << oldtable << endl;
            }
            else
            {
                cout << "Insert can't be performed" << endl;
            }
        }
        else if (oldtable && !newfile)
        {
            if (crud.dropTable())
            {
                cout << oldtable << " dropped" << endl;
            }
            else
            {
                cout << "Drop can't be performed on " << oldtable << endl;
            }
        }
        else if (outputmode)
        {
            queryPlanner.setOutputMode(outputmode);
        }
        else if (attsToSelect || finalFunction)
        {
            queryPlanner.planOperationOrder();
            queryPlanner.printOperationOrder();
            queryPlanner.executeQueryPlanner();
        }
    }
}