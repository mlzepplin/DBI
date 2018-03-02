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
    //auxFiles are specific to each table
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
        cout<<"READ STRING in open-----"<<f_type_string<<"..."<<endl<<endl;
        auxReadFile.close();
    }
    dFile.Open(1, (char *)f_path);

    return 1;
}

void SortedFile::MoveFirst()
{
    startReading();
    currentPageOffset = 0;
    // bufferPage.EmptyItOut();
    // dFile.GetPage(&bufferPage, 0);
    // currentPageOffset++;
}

void SortedFile::Add(Record &addme)
{
    //If in write mode, simply add record to bigQ
    //If in read mode, initialize a bigQ instance, add record to it
    cout<<"sorted file add";

    startWriting();
    cout <<"mode"<< mode<<endl;
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
    //write to output file
    auxFile.open(auxFilePath);
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
    if(mode==write) mergeBiQAndDfile();
    return dFile.Close();
}

void SortedFile::createBigQ()
{
    inPipe = new Pipe(PIPE_SIZE);
    outPipe = new Pipe(PIPE_SIZE);
    //trying bigQ here
    
}

void SortedFile::mergeBiQAndDfile(){
    //the main thread/process releases its lock on the inPipe
    //this allows BigQ to sort using inPipe and write to the outPipe
    inPipe->ShutDown();
    //cout<<"before bigQ"<<endl;
    bigQ = new BigQ(*inPipe, *outPipe, sortOrder, runLength);
    //cout<<"after bigQ"<<endl;
    // //wait for the outPipe to be released by BigQ and then use outPipe to write to dFile 
    
    Record temp;
    Schema mySchema("catalog", "nation");
    
    // 

    deleteBigQ();
}

void SortedFile::deleteBigQ(){
    delete inPipe;
    delete outPipe;
    delete bigQ;
    inPipe = outPipe = NULL;
    bigQ = NULL;
}

void SortedFile::startReading()
{
    if (mode == read)
         return;
    mode = read;
    mergeBiQAndDfile();
}

void SortedFile::startWriting()
{   cout<<"start writibg "<<endl;
    if (mode == write)
        return;
    mode = write;
    createBigQ();
}