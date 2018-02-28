#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "HeapFile.h"
#include "Defs.h"

#include <iostream>
#include <fstream>

HeapFile::HeapFile() : DB()
{
    currentPageOffset = 0;
}
HeapFile::~HeapFile()
{   
    delete(database);
    delete(tblFile);
    delete(currentRecord);
}
int HeapFile::Create(const char *fpath, void *startup)
{
    //generate auxfile name using f_path
    //the auxFiles are specific to each table
    string auxFilePath = getTableName(fpath);
    auxFilePath += ".meta";
    ofstream auxFile;
    auxFile.open (auxFilePath);
    
    //write to output file
    auxFile <<"heap"<< "\n";
    auxFile.close();

    //zero parameter makes sure that the file is created and not opened
    dFile.Open(0, (char *)fpath);

    return 1;
}

void HeapFile::Load(Schema &f_schema, const char *loadpath)
{
    Record tempRecord;
    //loadpath of the .tbl file
    FILE *tableFile = fopen(loadpath, "r");

    bufferPage.EmptyItOut();
    
    //fillup buffer page
    while(tempRecord.SuckNextRecord(&f_schema,tableFile)==1){   
       Add(tempRecord);
    }
    dFile.AddPage(&bufferPage,currentPageOffset);
    //don't increment currentPageOffset at load's end, as it would
    //need to point to the last record, not to the empty space after it
    bufferPage.EmptyItOut();
    
}

int HeapFile::Open(const char *f_path)
{
    dFile.Open(1, (char *)f_path);

    return 1;
}

void HeapFile::MoveFirst()
{
    currentPageOffset = 0;
    bufferPage.EmptyItOut();
    dFile.GetPage(&bufferPage, 0);
    currentPageOffset++;
}

int HeapFile::Close()
{   
    //write buffer page to dFile if any records still left
    if(bufferPage.getNumRecords()!=0){
        dFile.AddPage(&bufferPage,currentPageOffset);
        //don't increment currentPageOffset at close's end, as it would
        //need to point to the last record, not to the empty space after it
        bufferPage.EmptyItOut();
    }
    return dFile.Close();
}

void HeapFile::Add(Record &rec)
{

    //Append record to end of current page.
    //If there is not enough memory, write page to file, empty it out and add record
    if (!bufferPage.Append(&rec))
    {   

#ifdef printBufferLast
        //*************************************************************************
        //HELPER BLOCK TO PRINT THE LAST RECORD OF BUFFERPAGE BEFORE WRITING IT OUT
        //*************************************************************************
        Schema mySchema ("catalog", "customer");
        cout<<"num records in buffer page before it's written out"<<endl;
        cout<<bufferPage.getNumRecords()<<endl;
        Record *c = bufferPage.peekLastRecord();
        c->Print(&mySchema);
        cout<<"--------------------------------"<<endl;      
#endif
        //which_page is incremented, then checked against curLength
        //Using currentPageOffset to index and increment, and not just 
        //dFile.getLength()-1 because the appendages can be anywhere and not just
        //at the immediate end of the dFile, also this will keep currentPageOffset updated
        //and simultaneously keep updating the dFile.curLength through Append as well
        dFile.AddPage(&bufferPage, currentPageOffset); //or do dFile.getLength()-1{if only want to append to immediate end}
        currentPageOffset++;
        bufferPage.EmptyItOut();
        bufferPage.Append(&rec);

        
    }
}

int HeapFile::GetNext(Record &fetchme)
{

    //get current record
    //this while loop only entered when we aren't able to
    //get the nextrecord
    if(!bufferPage.GetFirst(&fetchme))
    {

        
        if (currentPageOffset+1 >= dFile.GetLength())
        {
            return 0;
        }
        /*
        GetPage already increases the offset before apending
        so no need to first increment currentpageOffset and
        then before passing it in GetPage
        */
        dFile.GetPage(&bufferPage, currentPageOffset);
        currentPageOffset++;
        bufferPage.GetFirst(&fetchme);
        
    }

    return 1;
}

int HeapFile::GetNext(Record &fetchme, CNF &cnf, Record &literal)
{

    ComparisonEngine comp;

    while (GetNext(fetchme))
    {
        //Compare already populates fetchme
        if (comp.Compare(&fetchme, &literal, &cnf))
        {
            return 1;
        }
    }
    return 0;
}

int HeapFile::initReadMode()
{
    //populates the bufferPage with the currentPage
    if (currentPageOffset > dFile.GetLength())
        return 0;
    dFile.AddPage(&bufferPage, currentPageOffset);
    return 1;
}
