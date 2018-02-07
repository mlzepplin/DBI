#ifndef DB_H
#define DB_H

#include "Record.h"
#include "Schema.h"
#include "fTypeEnum.h"

class DB {

protected:
    FILE *dbFile;
	FILE *tblFile;
	Page bufferPage;
	Record *currentRecord;

public:
    
    virtual int Create (const char* fpath, fType file_type, void* startup) = 0;
    virtual int Open (const char* fpath) = 0;
    virtual int Close() = 0;

    virtual void Add (Record& addme) = 0;
    virtual void Load (Schema& myschema, const char* loadpath) = 0;

    virtual void MoveFirst () = 0;
    virtual int GetNext (Record& fetchme) = 0;
    virtual int GetNext (Record& fetchme, CNF& cnf, Record& literal) = 0;
};

#endif