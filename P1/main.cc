
#include <iostream>
#include "ParseTree.h"

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
	while (finalFunction->right != NULL)
	{
		// cout << finalFunction->code;
		// cout << finalFunction->leftOperand;
	}
}
