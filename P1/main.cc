
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
	extern FuncOperator *finalFunction;
	extern TableList *tables;
	extern AndList *boolean;
	extern NameList *groupingAtts;
	extern NameList *attsToSelect;
	extern int distinctAtts;
	extern int distinctFunc;

	yyparse();
	cout << "tables" << endl;
	while (tables != NULL)
	{
		cout << tables->tableName;
		cout << tables->aliasAs;
		tables = tables->next;
		cout << endl;
	}
	cout << "groupingatts" << endl;
	while (groupingAtts != NULL)
	{
		cout << groupingAtts->name;
		groupingAtts = groupingAtts->next;
		cout << endl;
	}
	cout << "atts to select" << endl;
	while (attsToSelect != NULL)
	{
		cout << attsToSelect->name;
		attsToSelect = attsToSelect->next;
		cout << endl;
	}

	cout << "distfunc" << distinctFunc << endl;
	cout << "distatts" << distinctAtts << endl;

	Statistics st;
	st.Read("Statistics.txt");
	char *outFilePath = "./QueryPlannerOutput.txt";
	QueryPlanner queryPlanner(&st, outFilePath, boolean);
}
