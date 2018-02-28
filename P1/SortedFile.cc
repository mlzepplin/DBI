#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "SortedFile.h"
#include "Defs.h"

#include <iostream>

SortedFile::SortedFile() : DB()
{
    inPipe = new Pipe(PIPE_SIZE);
    outPipe = new Pipe(PIPE_SIZE);

}

SortedFile::~SortedFile()
{
    delete (database);
    delete (tblFile);
    delete (currentRecord);
}

int SortedFile::Create(const char *fpath, void *startup)
{
    
}

void SortedFile::Load(Schema &f_schema, const char *loadpath)
{

}

int SortedFile::GetNext(Record &fetchme, CNF &cnf, Record &literal)
{

    ComparisonEngine comp;

    while (GetNext(fetchme))
    {
        //Implement 
    }
    return 0;
}

int SortedFile::Close()
{
    if(bufferPage.getNumRecords()!=0){
        dFile.AddPage(&bufferPage,currentPageOffset);
        bufferPage.EmptyItOut();
    }
    //TODO 
    //Store sortOrder, filetype to re-open file
    //If file is sorted, merge Bigq with data in file

    return 1; 
}


int SortedFile::initReadMode()
{
    //populates the bufferPage with the currentPage
    if (currentPageOffset > dFile.GetLength())
        return 0;
    dFile.AddPage(&bufferPage, currentPageOffset);
    return 1;
}