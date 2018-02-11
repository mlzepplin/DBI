#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "HeapFile.h"
#include "Defs.h"

#include <iostream>

HeapFile::HeapFile() : DB()
{
    currentPageOffset = 0;
}

int HeapFile::Create(const char *fpath, void *startup)
{
    //zero parameter makes sure that the file is created
    //and not opened
    dFile.Open(0, (char *)fpath);
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
    currentPageOffset++;
    bufferPage.EmptyItOut();
    cout<<"-------------"<<dFile.GetLength();
    
}

int HeapFile::Open(const char *f_path)
{
    dFile.Open(1, (char *)f_path);
}

void HeapFile::MoveFirst()
{
    currentPageOffset = 0;
    bufferPage.EmptyItOut();
    dFile.GetPage(&bufferPage, currentPageOffset);
    currentPageOffset++;
}

int HeapFile::Close()
{
    return dFile.Close();
}

void HeapFile::Add(Record &rec)
{

    //Append record to end of current page.
    //If there is not enough memory, write page to file, empty it out and add record
    if (!bufferPage.Append(&rec))
    {   
        //which page is incremented, then checked against curLength
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
    while (!bufferPage.GetFirst(&fetchme))
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
        bufferPage.GetFirst(&fetchme);
        currentPageOffset++;
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
