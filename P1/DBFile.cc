#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

#include "DBFile.h"
#include "Defs.h"
#include "HeapFile.h"
#include "fTypeEnum.h"
#include "SortedFile.h"

#include <iostream>
#include <fstream>
#include <unordered_map>


DBFile::DBFile()
{   
    auxMap.insert(std::make_pair(heap,"heap"));
    auxMap.insert(std::make_pair(sorted,"sorted"));
    auxMap.insert(std::make_pair(tree,"tree"));

}
DBFile::~DBFile(){
    delete(db);
}

//CREATE ALWAYS DOES CREATE A NEW FILE!! ALWAYS!!
int DBFile::Create(const char *f_path, fType f_type, void *startup)
{
    allocateMemToDB(f_type);
    return db->Create(f_path, startup);
    
}

void DBFile::Load(Schema &f_schema, const char *loadpath)
{
    return db->Load(f_schema, loadpath);
}

int DBFile::Open(const char *f_path)
{   
    /** THIS WE'LL TAKE CARE OF LATER, CURRENTLY DEFAULTING TO HEAPFILE*/

    //FIRST READ FTPYE FROM AUX FILE   
    ifstream auxReadFile;
    string auxFilePath = getTableName(f_path);
    auxFilePath += ".meta";
    string f_type_string;
    fType f_type;

    auxReadFile.open(auxFilePath);
    if(auxReadFile.is_open())
    {
        auxReadFile >> f_type_string;
        cout<<"READ STRING-----"<<f_type_string<<"..."<<endl<<endl;
        auxReadFile.close();
    }
    else{
        cerr << "Can't open auxiliary file: "<<auxFilePath<< endl;
        exit(1);
    }
    //get the key corresponding to our read value
    for (unordered_map<fType,string>::const_iterator it = auxMap.begin(); it != auxMap.end(); ++it) {
        if (it->second.compare(f_type_string) != 0) f_type = it->first;
    } 
    
    ifstream binFile(f_path);
    if (!binFile) {
        cerr<< "creating bin file without load as it didn't exist"<<endl;
        return Create(f_path, f_type, NULL);
    }
    else{
        allocateMemToDB(f_type);
        return db->Open(f_path);
    }
    
    
}

void DBFile::allocateMemToDB(fType f_type){
    switch (f_type)
    {
        case heap:
        {   db = new HeapFile();
            break;
        }
        case sorted:
        {
            db = new SortedFile();
            break;
        }
        default:
        { 
            cerr << "Unimplemented file type option given" << endl;
            cerr<< "Defaulting to HeapFile"<<endl;
            db = new HeapFile();
            
        }
       
    }
}

void DBFile::MoveFirst()
{
    db->MoveFirst();
}

int DBFile::Close()
{
    return db->Close();
}

void DBFile::Add(Record &rec)
{
    return db->Add(rec);
}

int DBFile::GetNext(Record &fetchme)
{
    db->GetNext(fetchme);
}

int DBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal)
{
    return db->GetNext(fetchme, cnf, literal);
}

