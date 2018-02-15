#include <iostream>
#include "Record.h"
#include <stdlib.h>
#include "HeapFile.h"
using namespace std;

extern "C" {
	int yyparse(void);   // defined in y.tab.c
}

extern struct AndList *final;

int main () {
	HeapFile *h = new HeapFile();
    	Schema mySchema ("catalog", "lineitem");
	h->Load(mySchema,"./lineitem.tbl");

}

