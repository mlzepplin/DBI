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
    auxFile.open(tableName+".meta");
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

void SortedFile::initPipes()
{
    inPipe = new Pipe(PIPE_SIZE);
    outPipe = new Pipe(PIPE_SIZE);
    
}

void SortedFile::mergeBiQAndDfile(){
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
    cout<<"TEMPSTRING"<<binPath<<endl;
    const char* tempPath = tempString.c_str();
    HeapFile tempFile;
    tempFile.Create((char*)tempPath, NULL);
    
    ComparisonEngine compEngine;

    // initialize
    if (!fileEmpty) {
        moveFirstWithoutModeSwitch();
        //now we'll have to check if the page brought in was not empty
        fileEmpty = !GetNext(fileRecord);
    }
     //Schema mySchema ("catalog", "nation");
    //cout <<"IN MERGE"<<endl<<endl<<endl;
    //pipeRecord.Print(&mySchema);
    
   //merging 
    while (!fileEmpty||!pipeEmpty){
       
        if(fileEmpty|| (!pipeEmpty && compEngine.Compare(&pipeRecord,&fileRecord, &sortOrder)<0)){
            tempFile.Add(pipeRecord);
            pipeEmpty = !outPipe->Remove(&pipeRecord);
            
        }
        else if(pipeEmpty|| (!fileEmpty && compEngine.Compare(&pipeRecord,&fileRecord, &sortOrder)>=0)){
            tempFile.Add(fileRecord);
            fileEmpty = !GetNext(fileRecord);
        }
        else{
            cerr<<"merge failed"<<endl;
            exit(1);
        }
    }
    tempFile.Close();    
    //RENAMING FILE
    string newName = binPath+"/"+tableName+".bin";
    if(rename(tempPath,newName.c_str() )!=0){
        cout<<"merge rename/write failed"<<endl;
        exit(1);
    } 

    //END MERGING
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
{   cout<<"start reading "<<endl;
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
    initPipes();
}