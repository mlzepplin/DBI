
#include <iostream>
#include "ParseTree.h"
#include "Statistics.h"
#include "QueryPlanner.h"

using namespace std;

extern "C" {
int yyparse(void); // defined in y.tab.c
}

int main()
{
	//cout << finalFunction->code;
	extern struct FuncOperator *finalFunction;
	extern struct TableList *tables;
	extern struct AndList *boolean;
	extern struct NameList *groupingAtts;
	extern struct NameList *attsToSelect;
	extern int distinctAtts;
	extern int distinctFunc;

	// extern char *newtable;
	// extern char *oldtable;
	// extern char *newfile;
	// extern char *outputmode;
	// extern struct AttrList *newattrs;

	yyparse();
	// cout << "tables" << endl;
	// while (tables != NULL)
	// {
	// 	cout << tables->tableName;
	// 	cout << tables->aliasAs;
	// 	tables = tables->next;
	// 	cout << endl;
	// }
	// cout << "groupingatts" << endl;
	// while (groupingAtts != NULL)
	// {
	// 	cout << groupingAtts->name;
	// 	groupingAtts = groupingAtts->next;
	// 	cout << endl;
	// }
	// cout << "atts to select" << endl;
	// while (attsToSelect != NULL)
	// {
	// 	cout << attsToSelect->name;
	// 	attsToSelect = attsToSelect->next;
	// 	cout << endl;
	// }

	// cout << "distfunc" << distinctFunc << endl;
	// cout << "distatts" << distinctAtts << endl;

	Statistics *st = new Statistics();
	//st.Read("Statistics.txt");
	char *relName[] = {"part", "supplier", "partsupp"};

	st->AddRel(relName[0], 1500000);
	st->AddAtt(relName[0], "p_partkey", 150000);

	st->AddRel(relName[1], 150000);
	st->AddAtt(relName[1], "s_suppkey", 150000);
	st->AddAtt(relName[1], "s_acctbal", 25);

	st->AddRel(relName[2], 25);
	st->AddAtt(relName[2], "ps_partkey", 25);
	st->AddAtt(relName[2], "ps_suppkey", 150000);
	st->AddAtt(relName[2], "ps_supplycost", 150000);

	char *outFilePath = "./QueryPlannerOutput.txt";

	QueryPlanner queryPlanner(st, outFilePath, boolean);

	queryPlanner.planOperationOrder();
}
