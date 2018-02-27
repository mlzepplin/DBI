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
    currentPageOffset = 0;
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

int SortedFile::Open(const char *f_path)
{
    dFile.Open(1, (char *)f_path);

    return 1;
}

void SortedFile::MoveFirst()
{
    currentPageOffset = 0;
    bufferPage.EmptyItOut();
    dFile.GetPage(&bufferPage, 0);
    currentPageOffset++;
}

int SortedFile::GetNext(Record &fetchme)
{

    if (!bufferPage.GetFirst(&fetchme))
    {
        if (currentPageOffset + 1 >= dFile.GetLength())
        {
            return 0;
        }

        dFile.GetPage(&bufferPage, currentPageOffset);
        currentPageOffset++;
        bufferPage.GetFirst(&fetchme);
    }

    return 1;
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