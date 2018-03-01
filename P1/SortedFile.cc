#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "SortedFile.h"
#include "Defs.h"
#include "DB.h"

#include <iostream>
#include <fstream>

typedef struct
{
    OrderMaker *orderMaker;
    int runlength;
} SortInfo;

std::istream &operator>>(std::istream &is, OrderMaker &order)
{
    is >> order.numAtts;
    for (int i = 0; i < order.numAtts; ++i)
        is >> order.whichAtts[i];
    for (int i = 0; i < order.numAtts; ++i)
    {
        int t;
        is >> t;
        order.whichTypes[i] = static_cast<Type>(t);
    }
    return is;
}

SortedFile::SortedFile() : DB()
{
    currentPageOffset = 0;
    inPipe = NULL;
    outPipe = NULL;
    runLength = 0;
    mode = read;
}

SortedFile::~SortedFile()
{
    delete (database);
    delete (tblFile);
    delete (currentRecord);
}

int SortedFile::Create(const char *fpath, void *startup)
{

    SortInfo s = *(SortInfo *)startup;
    sortOrder = *s.orderMaker;
    runLength = s.runlength;

    //populate auxFilePath
    //the auxFiles are specific to each table
    auxFilePath = getTableName(fpath);
    auxFilePath += ".meta";

    //zero parameter makes sure that the file is created and not opened
    dFile.Open(0, (char *)fpath);

    return 1;
}

void SortedFile::Load(Schema &f_schema, const char *loadpath)
{
    Record tempRecord;
    FILE *tableFile = fopen(loadpath, "r");

    bufferPage.EmptyItOut();

    while (tempRecord.SuckNextRecord(&f_schema, tableFile) == 1)
    {
        Add(tempRecord);
    }
    dFile.AddPage(&bufferPage, currentPageOffset);
    bufferPage.EmptyItOut();
}

int SortedFile::Open(const char *f_path)
{
    string auxFilePath = getTableName(f_path);
    auxFilePath += ".meta";

    //read meta file and set sortOrder and runLength
    ifstream auxReadFile;
    //helper vars
    string f_type_string;
    auxReadFile.open(auxFilePath);
    if (auxReadFile.is_open())
    {
        //first line of every meta file is the f_type string
        auxReadFile >> f_type_string;
        auxReadFile >> sortOrder.numAtts;
        for (int i = 0; i < sortOrder.numAtts; ++i)
            auxReadFile >> sortOrder.whichAtts[i];
        for (int i = 0; i < sortOrder.numAtts; ++i)
        {
            int t;
            auxReadFile >> t;
            sortOrder.whichTypes[i] = static_cast<Type>(t);
        }
        auxReadFile >> runLength;
        //cout<<"READ STRING-----"<<f_type_string<<"..."<<endl<<endl;
        auxReadFile.close();
    }
    dFile.Open(1, (char *)f_path);

    return 1;
}

void SortedFile::MoveFirst()
{
    startReading();
    currentPageOffset = 0;
    bufferPage.EmptyItOut();
    dFile.GetPage(&bufferPage, 0);
    currentPageOffset++;
}

void SortedFile::Add(Record &addme)
{
    //If in write mode, simply add record to bigQ
    //If in read mode, initialize a bigQ instance, add record to it
    cout<<"sorted file add";

    startWriting();
    inPipe->Insert(&addme);
}

int SortedFile::GetNext(Record &fetchme)
{   
    startReading();
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
    startReading();
    OrderMaker queryOrder, cnfOrder;

    //Build queryOrder by comparing sortOrder and cnfOrder

    ComparisonEngine comp;

    //If query order is empty, return first record that is equal to literal
    //without performing a binary search
    if (queryOrder.numAtts == 0)
    {
        while (GetNext(fetchme))
        {
            if (comp.Compare(&fetchme, &literal, &cnf))
                return 1;
        }
        return 0;
    }

    // if (matchSortOrder())
    // {
    //     binarySearch();
    // }

    return 0;
}

int SortedFile::Close()
{
    if (bufferPage.getNumRecords() != 0)
    {
        dFile.AddPage(&bufferPage, currentPageOffset);
        bufferPage.EmptyItOut();
    }

    //write to output file
    auxFile.open(auxFilePath);
    //auxFile << "sorted"<< "\n"<<sortOrder<<"\n"<<runLength<<"\n";
    auxFile << "sorted"
            << "\n";
    auxFile << sortOrder.numAtts << ' ';
    for (int i = 0; i < sortOrder.numAtts; ++i)
        auxFile << sortOrder.whichAtts[i] << ' ';
    for (int i = 0; i < sortOrder.numAtts; ++i)
        auxFile << sortOrder.whichTypes[i] << ' ';
    auxFile << std::endl;
    auxFile << runLength << "\n";
    auxFile.close();

    //TODO
    //Store sortOrder, filetype to re-open file
    //If file is sorted, merge Bigq with data in file

    return dFile.Close();
}

void SortedFile::createBigQ()
{
    inPipe = new Pipe(PIPE_SIZE);
    outPipe = new Pipe(PIPE_SIZE);
}

void SortedFile::startReading()
{
    // if (mode == read)
    //     return;
    mode = read;
    //the main thread/process releases its lock on the inPipe
    //this allows BigQ to sort using inPipe and write to the outPipe
    inPipe->ShutDown();
    //wait for the outPipe to be released by BigQ and then use outPipe to write to dFile
    bigQ = new BigQ(*inPipe, *outPipe, sortOrder, runLength);
    Record temp;
    Schema mySchema("catalog", "nation");
    cout<<"POPULATING DFILE::"<<endl;
    while (outPipe->Remove(&temp))
    {   temp.Print(&mySchema);
        if (!bufferPage.Append(&temp))
        {

#ifdef printBufferLast
            //*************************************************************************
            //HELPER BLOCK TO PRINT THE LAST RECORD OF BUFFERPAGE BEFORE WRITING IT OUT
            //*************************************************************************
            Schema mySchema("catalog", "customer");
            cout << "num records in buffer page before it's written out" << endl;
            cout << bufferPage.getNumRecords() << endl;
            Record *c = bufferPage.peekLastRecord();
            c->Print(&mySchema);
            cout << "--------------------------------" << endl;
#endif
            //which_page is incremented, then checked against curLength
            //Using currentPageOffset to index and increment, and not just
            //dFile.getLength()-1 because the appendages can be anywhere and not just
            //at the immediate end of the dFile, also this will keep currentPageOffset updated
            //and simultaneously keep updating the dFile.curLength through Append as well
            dFile.AddPage(&bufferPage, currentPageOffset); //or do dFile.getLength()-1{if only want to append to immediate end}
            currentPageOffset++;
            bufferPage.EmptyItOut();
            bufferPage.Append(&temp);
        }
    }
}

void SortedFile::startWriting()
{
    if (mode == write)
        return;
    mode = write;
    createBigQ();
}