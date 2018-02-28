#include "Record.h"
#include "Schema.h"
#include "fTypeEnum.h"
#include "DB.h"


DB::~DB()
{
    delete (database);
    delete (tblFile);
    delete (currentRecord);
}

int DB::Open(const char *f_path)
{
    dFile.Open(1, (char *)f_path);
    return 1;
}

void DB::MoveFirst()
{
    currentPageOffset = 0;
    bufferPage.EmptyItOut();
    dFile.GetPage(&bufferPage, 0);
    currentPageOffset++;
}

int DB::GetNext(Record &fetchme)
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

int DB::initReadMode()
{
    //populates the bufferPage with the currentPage
    if (currentPageOffset > dFile.GetLength())
        return 0;
    dFile.AddPage(&bufferPage, currentPageOffset);
    return 1;
}