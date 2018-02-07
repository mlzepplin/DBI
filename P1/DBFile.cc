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


#include <iostream>

// stub file .. replace it with your own DBFile.cc

DBFile::DBFile () {
  
}

int DBFile::Create (const char *f_path, fType f_type, void *startup) {
    switch(f_type) {
        case heap: {
            db = new HeapFile();
            break;
        default:
            cerr<<"Invalid file type option"<<endl;
            exit(1);
        }
    }
}

void DBFile::Load (Schema &f_schema, const char *loadpath) {
    return db->Load(f_schema, loadpath);
}

int DBFile::Open (const char *f_path) {
    return db->Open(f_path);
}

void DBFile::MoveFirst () {
    db->MoveFirst();
}

int DBFile::Close () {
    return db->Close();
}

void DBFile::Add (Record &rec) {
    return db->Add(rec);
}

int DBFile::GetNext (Record &fetchme) {
    db->GetNext(fetchme);
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    return db->GetNext(fetchme, cnf, literal);
}
