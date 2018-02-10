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
    currentPageOffset=0;
    
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
            pageCount++;
            cout<<dFile.GetLength()<<endl;
        }

    }
    //need a check to see if buffer page is empty
    if(bufferPage.GetFirst(&tempRecord)){
        bufferPage.Append(&tempRecord);
        dFile.AddPage(&bufferPage,pageCount);
    }
    
          
}
int HeapFile::Open (const char *f_path) {
    dFile.Open(1,(char *)f_path);
}

void HeapFile::MoveFirst () {
  currentPageOffset = 0;
  bufferPage.EmptyItOut();
  dFile.GetPage(&bufferPage,currentPageOffset);
   
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
    
    //get current record
    //this while loop only entered when we aren't able to
    //get the nextrecord
    while(!bufferPage.GetFirst(&fetchme)){
        
        currentPageOffset++;
        if(currentPageOffset>dFile.GetLength()-2){
            return 0;
        }
        dFile.GetPage(&bufferPage,currentPageOffset);
        

    }

    return 1;
    
}

int HeapFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) { 

    ComparisonEngine comp;
     
    while (GetNext(fetchme)) {
        if (comp.Compare (&fetchme, &literal, &cnf)){
            //fetchme.Consume(&fetchme);
            return 1;
            }
        }
    return 0;
   
}

int HeapFile::initReadMode(){
    //populates the bufferPage with the currentPage 
    if(currentPageOffset>dFile.GetLength()) return 0;
    dFile.AddPage(&bufferPage,currentPageOffset);
    return 1;

}

