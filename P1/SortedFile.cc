#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "SortedFile.h"
#include "Defs.h"
#include "DB.h"
#include "HeapFile.h"

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
    // delete (database);
    // delete (tblFile);
    // delete (currentRecord);
}

int SortedFile::Create(const char *fpath, void *startup)
{

    SortInfo s = *(SortInfo *)startup;
    sortOrder = *s.orderMaker;
    runLength = s.runlength;

    //populate auxFilePath
    //auxFiles are specific to each table
    tableName = getTableName(fpath);
    binPath = getBinPath(fpath);

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
    //firstly setting binPath and table name
    tableName = getTableName(f_path);
    binPath = getBinPath(f_path);
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
        //cout<<"READ STRING in open-----"<<f_type_string<<"..."<<endl<<endl;
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
    startWriting();
    inPipe->Insert(&addme);
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
    startReading();
    OrderMaker queryOrder, cnfOrder;

    //Build queryOrder by comparing sortOrder and cnfOrder
    OrderMaker::buildQueryOrder(sortOrder, cnf, queryOrder, cnfOrder);

    ComparisonEngine comp;

    if (!binarySearch(fetchme, queryOrder, literal, cnfOrder, comp))
        return 0;

    do
    {
        if (comp.Compare(&fetchme, &queryOrder, &literal, &cnfOrder))
            return 0;
        if (comp.Compare(&fetchme, &literal, &cnf))
            return 1;
    } while (GetNext(fetchme));
    return 0;
}

int SortedFile::Close()
{
    //write to output file
    auxFile.open(tableName + ".meta");
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
    if (mode == write)
        mergeBiQAndDfile();
    return dFile.Close();
}

int SortedFile::binarySearch(Record &fetchme, OrderMaker &queryOrder, Record &literal, OrderMaker &cnfOrder, ComparisonEngine &comp)
{
    if (!GetNext(fetchme))
        return 0;

    int result = comp.Compare(&fetchme, &queryOrder, &literal, &cnfOrder);
    if (result > 0)
        return 0;
    else if (result == 0)
        return 1;

    off_t low = 0;
    off_t high = dFile.lastIndex();
    off_t mid = (low + high) / 2;
    for (; low < mid; mid = (low + high) / 2)
    {
        dFile.GetPage(&bufferPage, mid);

        result = comp.Compare(&fetchme, &queryOrder, &literal, &cnfOrder);
        if (result < 0)
            low = mid;
        else if (result > 0)
            high = mid - 1;
        else
            high = mid;
    }

    dFile.GetPage(&bufferPage, low);
    do
    {
        if (!GetNext(fetchme))
            return 0;
result = comp.Compare(&fetchme, &queryOrder, &literal, &cnfOrder);
    } while (result < 0);
    return result == 0;
}


void SortedFile::initPipes()
{
    inPipe = new Pipe(PIPE_SIZE);
    outPipe = new Pipe(PIPE_SIZE);
}

void SortedFile::mergeBiQAndDfile()
{
    //the main thread/process releases its lock on the inPipe
    //this allows BigQ to sort using inPipe and write to the outPipe
    inPipe->ShutDown();

    //BigQ populates the outPipe and releases its lock on it
    bigQ = new BigQ(*inPipe, *outPipe, sortOrder, runLength);

    //--------MERGING-----------
    Record fileRecord, pipeRecord;
    bool fileEmpty = dFile.isEmpty(), pipeEmpty = !outPipe->Remove(&pipeRecord);

    //merge will be written to temp file
    string tempString = binPath + "/temp.bin";
    cout << "TEMPSTRING" << binPath << endl;
    const char *tempPath = tempString.c_str();
    HeapFile tempFile;
    tempFile.Create((char *)tempPath, NULL);

    ComparisonEngine compEngine;

    // initialize
    if (!fileEmpty)
    {
        moveFirstWithoutModeSwitch();
        //now we'll have to check if the page brought in was not empty
        fileEmpty = !GetNext(fileRecord);
    }

    //merging
    while (!fileEmpty || !pipeEmpty)
    {

        if (fileEmpty || (!pipeEmpty && compEngine.Compare(&pipeRecord, &fileRecord, &sortOrder) < 0))
        {
            tempFile.Add(pipeRecord);
            pipeEmpty = !outPipe->Remove(&pipeRecord);
        }
        else if (pipeEmpty || (!fileEmpty && compEngine.Compare(&pipeRecord, &fileRecord, &sortOrder) >= 0))
        {
            tempFile.Add(fileRecord);
            fileEmpty = !GetNext(fileRecord);
        }
        else
        {
            cerr << "merge failed" << endl;
            exit(1);
        }
    }
    tempFile.Close();
    //RENAMING FILE
    string newName = binPath + "/" + tableName + ".bin";
    if (rename(tempPath, newName.c_str()) != 0)
    {
        cout << "merge rename/write failed" << endl;
        exit(1);
    }

    //END MERGING
    deleteBigQ();
}

void SortedFile::deleteBigQ()
{

    inPipe = outPipe = NULL;
    bigQ = NULL;
    delete inPipe;
    delete outPipe;
    delete bigQ;
}

void SortedFile::startReading()
{
    if (mode == read)
        return;
    mode = read;
    mergeBiQAndDfile();
}

void SortedFile::startWriting()
{
    if (mode == write)
        return;
    mode = write;
    initPipes();
}