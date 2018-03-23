#include "RelOp.h"
#include "Record.h"
#include "vector"

//SelectFile
SelectFile::SelectFile()
{
	numPages = 1;
}

void *SelectFile::selectFileHelper()
{
	//NOTE: No need of using the numPages memory cnostraint
	//as we are select is purely streaing and non-blocking
	while (inFile->GetNext(*(buffer), *(selOp), *(literal)) == 1)
	{
		outPipe->Insert(buffer);
	}
	outPipe->ShutDown();
}

void *SelectFile::selectFileStaticHelper(void *selectFile)
{
	//required because initial thread call has to be static
	//as if a thread helper function cannot have a this pointer pointing to it
	//and non static class member functions have a hidden this parameter passed in
	SelectFile *SF = (SelectFile *)selectFile;
	return SF->selectFileHelper();
}

void SelectFile::Run(DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal)
{

	buffer = new Record();
	this->inFile = &inFile;
	this->outPipe = &outPipe;
	this->selOp = &selOp;
	this->literal = &literal;

	//spawns worker thread and returns to the operation
	int w = pthread_create(&thread, NULL, selectFileStaticHelper, (void *)this);
	if (w)
	{
		printf("Error creating selectFile thread! Return %d\n", w);
		exit(-1);
	}
}

void SelectFile::WaitUntilDone()
{
	pthread_join(thread, NULL);
}

void SelectFile::Use_n_Pages(int runlen)
{
	numPages = runlen;
}

//SelectPipe
SelectPipe::SelectPipe()
{
	numPages = 1;
}

void *SelectPipe::selectPipeHelper()
{
	//NOTE: No need of using the numPages memory cnostraint
	//as we are select is purely streaing and non-blocking
	ComparisonEngine comp;
	while (inPipe->Remove(buffer))
	{

		if (comp.Compare(buffer, literal, selOp))
		{
			outPipe->Insert(buffer);
		}
	}
	outPipe->ShutDown();
}

void *SelectPipe::selectPipeStaticHelper(void *selectPipe)
{

	SelectPipe *SP = (SelectPipe *)selectPipe;
	SP->selectPipeHelper();
}

void SelectPipe::Run(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal)
{

	buffer = new Record();
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->selOp = &selOp;
	this->literal = &literal;

	//spawns worker thread and returns to the operation
	int w = pthread_create(&thread, NULL, selectPipeStaticHelper, (void *)this);
	if (w)
	{
		printf("Error creating selectPipe thread! Return %d\n", w);
		exit(-1);
	}
}

void SelectPipe::WaitUntilDone()
{
	pthread_join(thread, NULL);
}

void SelectPipe::Use_n_Pages(int runlen)
{
	numPages = runlen;
}

void *Sum::sumHelper()
{
	if (computeMe->returnsInt == 1)
	{
		calculateSum<int>(inPipe, outPipe, computeMe);
	}
	else
	{
		calculateSum<double>(inPipe, outPipe, computeMe);
	}
	outPipe->ShutDown();
}

void *Sum::sumStaticHelper(void *sum)
{

	Sum *s = (Sum *)sum;
	s->sumHelper();
}

void Sum::Run(Pipe &inPipe, Pipe &outPipe, Function &computeMe)
{

	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->computeMe = &computeMe;

	int w = pthread_create(&thread, NULL, sumStaticHelper, (void *)this);
	if (w)
	{
		printf("Error creating sum thread! Return %d\n", w);
		exit(-1);
	}
}

void Sum::WaitUntilDone()
{
	pthread_join(thread, NULL);
}

void Sum::Use_n_Pages(int runlen)
{
	numPages = runlen;
}
