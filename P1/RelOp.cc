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

void *DuplicateRemoval::duplicateRemovalHelper()
{
	OrderMaker sortOrder(schema);
	Pipe sortedPipe(PIPE_SIZE);
	BigQ bigQ(*inPipe, sortedPipe, sortOrder, numPages);

	Record buffer, next;
	ComparisonEngine compEngine;

	if (sortedPipe.Remove(&buffer))
	{
		while (sortedPipe.Remove(&next))
		{
			if (compEngine.Compare(&buffer, &next, &sortOrder))
			{
				outPipe->Insert(&buffer);
				buffer.Consume(&next);
			}
		}
		outPipe->Insert(&buffer);
	}
	outPipe->ShutDown();
}

void *DuplicateRemoval::duplicateRemovalStaticHelper(void *duplicateRemoval)
{

	DuplicateRemoval *dupRemoval = (DuplicateRemoval *)duplicateRemoval;
	dupRemoval->duplicateRemovalHelper();
}

void DuplicateRemoval::Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema)
{
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->schema = &mySchema;

	int w = pthread_create(&thread, NULL, duplicateRemovalStaticHelper, (void *)this);
	if (w)
	{
		printf("Error creating duplicateRemoval thread! Return %d\n", w);
		exit(-1);
	}
}

void DuplicateRemoval::WaitUntilDone()
{
	pthread_join(thread, NULL);
}

void DuplicateRemoval::Use_n_Pages(int runlen)
{
	numPages = runlen;
}

void Project::Run(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput)
{
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->keepMe = keepMe;
	this->numAttsInput = numAttsInput;
	this->numAttsOutput = numAttsOutput;
	int w = pthread_create(&thread, NULL, projectStaticHelper, (void *)this);
	if (w)
	{
		printf("Error creating project thread! Return %d\n", w);
		exit(-1);
	}
}

void *Project::projectStaticHelper(void *project)
{
	Project *P = (Project *)project;
	P->projectHelper();
}
void *Project::projectHelper()
{
	//NOTE: No need of using the numPages memory cnostraint
	//as we are project is purely streaing and non-blocking
	Record buildMe;
	Schema mySchema("catalog", "part");
	//Schema("catalog", numAttsOutput, keepMe);
	int a = 0;
	while (inPipe->Remove(&buildMe))
	{
		buildMe.Project(keepMe, numAttsOutput, numAttsInput);
		outPipe->Insert(&buildMe);
	}
	outPipe->ShutDown();
}
void Project::WaitUntilDone()
{
	pthread_join(thread, NULL);
}
void Project::Use_n_Pages(int n)
{
	numPages = n;
}