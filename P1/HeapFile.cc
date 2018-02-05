#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "HeapFile.h"
#include "Defs.h"

#include <iostream>


HeapFile::HeapFile (){

}

int HeapFile::Create (const char *f_path, fType f_type, void *startup) {
}

void HeapFile::Load (Schema &f_schema, const char *loadpath) {
   Record temp;
   //loadpath to std::FILE pointer
    FILE *tableFile = fopen (loadpath, "r");
    int pageCount =0;
   
    //fillup buffer page
    
   
    while(temp.SuckNextRecord (&f_schema,tableFile)==1){
        //append already checks limits for page size    
        if(bufferPage.Append(&temp)==0)break;   
    }
    cout<<pageCount;
    pageCount++;
    
    //dFile->AddPage(&bufferPage,pageCount);
    //bufferPage.EmptyItOut();
    
  
    

    


    
}
int HeapFile::Open (const char *f_path) {
}

void HeapFile::MoveFirst () {
}

int HeapFile::Close () {
}

void HeapFile::Add (Record &rec) {
}

int HeapFile::GetNext (Record &fetchme) {
}

int HeapFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
}
