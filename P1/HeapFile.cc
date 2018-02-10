#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "HeapFile.h"
#include "Defs.h"

#include <iostream>


HeapFile::HeapFile():DB(){
      
}
int HeapFile::Create (const char *fpath, void *startup){
    //zero parameter makes sure that the file is created
    //and not opened
    dFile.Open(0,(char *)fpath);


}

void HeapFile::Load (Schema &f_schema, const char *loadpath) {
   
   Record tempRecord;
   //loadpath of the .tbl file
    FILE *tableFile = fopen (loadpath, "r");

    //init pageCount
    off_t pageCount =0;
   
    //fillup buffer page
    while(tempRecord.SuckNextRecord (&f_schema,tableFile)==1){
        //if page full add it to memory(dFile) empty out bufferPage
        if(bufferPage.Append(&tempRecord)==0){
            dFile.AddPage(&bufferPage,pageCount);
            bufferPage.EmptyItOut();
        }  
    }
          
}
int HeapFile::Open (const char *f_path) {
}

void HeapFile::MoveFirst () {

    
}

int HeapFile::Close () {

    dFile.Close();
}

void HeapFile::Add (Record &rec) {

    //Append record to end of current page. 
    //If there is not enough memory, write page to file, empty it out and add record
    if (!bufferPage.Append(&rec)){
        dFile.AddPage(&bufferPage, dFile.GetLength()); 
        bufferPage.EmptyItOut();
        bufferPage.Append(&rec);
    }
}

int HeapFile::GetNext (Record &fetchme) {
}

int HeapFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
}
