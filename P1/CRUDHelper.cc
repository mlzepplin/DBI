#include <fstream>
#include <iostream>

#include "CRUDHelper.h"
#include "DBFile.h"
#include "ParseTree.h"
#include "Comparison.h"

extern struct FuncOperator *finalFunction;
extern struct TableList *tables;
extern struct AndList *boolean;
extern struct NameList *groupingAtts;
extern struct NameList *attsToSelect;
extern int distinctAtts;
extern int distinctFunc;

extern struct AttributeList *newAtts;
extern struct NameList *sortAtts;

extern char *newtable;
extern char *oldtable;
extern char *newfile;
extern char *outputmode;

extern char *catalog_path;
extern char *dbfile_dir;
extern char *tpch_dir;

bool CRUDHelper::createTable()
{

    if (isRelPresent(newtable))
    {
        cout << newtable << ' ' << "already exists" << endl;
        return false;
    }

    //TODO
    //create new table
}

bool CRUDHelper::insertInto()
{
    DBFile db;
    char *fpath = new char[strlen(oldtable)];
    strcpy(fpath, oldtable);
    strcat(fpath, ".bin");

    Schema schema(catalog_path, oldtable);

    if (db.Open(fpath))
    {
        db.Load(schema, newfile);
        db.Close();
        delete[] fpath;
        return true;
    }
    delete[] fpath;
    return false;
}

bool CRUDHelper::dropTable()
{
}

bool CRUDHelper::isRelPresent(const char *relName)
{

    ifstream catalog(catalog_path);
    string line;

    while (getline(catalog, line))
    {
        if (trim(line) == relName)
        {
            catalog.close();
            return true;
        }
    }
    catalog.close();
    return false;
}

string CRUDHelper::trim(const string &str)
{
    size_t first = str.find_first_not_of(' ');
    if (string::npos == first)
    {
        return str;
    }
    size_t last = str.find_last_not_of(' ');
    return str.substr(first, (last - first + 1));
}