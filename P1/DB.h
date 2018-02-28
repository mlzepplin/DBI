#ifndef DB_H
#define DB_H

#include "Record.h"
#include "Schema.h"
#include "fTypeEnum.h"

class DB
{

  protected:
    FILE *database;
    FILE *tblFile;
    Page bufferPage;
    Record *currentRecord;
    File dFile;
    off_t currentPageOffset;

  public:
    inline DB(){currentPageOffset=0;}
    ~DB();
    //non virtual methods
    int Open(const char *fpath);
    void MoveFirst();
    int GetNext(Record &fetchme);

    //virtual methods
    virtual int Create(const char *fpath, void *startup) = 0;
    virtual int Close() = 0;
    virtual void Add(Record &addme) = 0;
    virtual void Load(Schema &myschema, const char *loadpath) = 0;
    virtual int GetNext(Record &fetchme, CNF &cnf, Record &literal) = 0;

    //Added new methods
    virtual int initReadMode() = 0;
};
#endif