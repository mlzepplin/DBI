
#include "RelOp.h"
#include "Record.h"
#include "vector"
#include "HeapFile.h"

//SelectFile
SelectFile::SelectFile()
{
	numPages = 1;
}

void *SelectFile::selectFileHelper()
{
	//NOTE: No need of using the numPages memory constraint
	//as we are select is purely streaming and non-blocking
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

	//first shut input pipe, so bigQ can lock on it
	//inPipe->ShutDown();
	//call BigQ to put the sorted output into the outPipe/sortedPipe
	BigQ bigQ(*inPipe, sortedPipe, sortOrder, numPages);
	
	Record buffer, next;
	ComparisonEngine compEngine;

	if (sortedPipe.Remove(&buffer))
	{

		while (sortedPipe.Remove(&next))
		{
			if (compEngine.Compare(&buffer, &next, &sortOrder) != 0)
			{
				outPipe->Insert(&buffer);
				buffer.Consume(&next);
			}
		}
		outPipe->Insert(&buffer);
	}
	bigQ.WaitUntilDone();
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

void *WriteOut::writeOutHelper()
{
	Record buffer;

	while (inPipe->Remove(&buffer))
	{
		buffer.Write(outputFile, schema);
	}
}

void *WriteOut::writeOutStaticHelper(void *writeOut)
{

	WriteOut *wo = (WriteOut *)writeOut;
	wo->writeOutHelper();
}

void WriteOut::Run(Pipe &inPipe, FILE *outFile, Schema &mySchema)
{
	this->inPipe = &inPipe;
	this->outputFile = outFile;
	this->schema = &mySchema;

	int w = pthread_create(&thread, NULL, writeOutStaticHelper, (void *)this);
	if (w)
	{
		printf("Error creating writeOut thread! Return %d\n", w);
		exit(-1);
	}
}

void WriteOut::WaitUntilDone()
{
	pthread_join(thread, NULL);
}

void WriteOut::Use_n_Pages(int runlen)
{
	numPages = runlen;
}

void Join::sortMergeJoin(Pipe *leftPipe, OrderMaker *leftOrder, Pipe *rightPipe, OrderMaker *rightOrder, Pipe *outPipe, CNF *cnf, Record *literal, size_t runLength)
{
	ComparisonEngine compEngine;
	//output pipes for the two input pipes
	Pipe leftSortedPipe(PIPE_SIZE);
	Pipe rightSortedPipe(PIPE_SIZE);
	
	//BigQ's to store input from the two inout pipes
	BigQ leftBigQ(*leftPipe, leftSortedPipe, *leftOrder, runLength);
	BigQ rightBigQ(*rightPipe, rightSortedPipe, *rightOrder, runLength);

	Record leftRecord, rightRecord, mergedRecord;
	
	int l = leftSortedPipe.Remove(&leftRecord);
	int r = rightSortedPipe.Remove(&rightRecord);


	while(l && r){
		
		int c = compEngine.Compare(&leftRecord,leftOrder,&rightRecord,rightOrder);
		if(c==0){
			//create a merge record
			int leftNumAtts = leftRecord.getNumAtts();
			int rightNumAtts = rightRecord.getNumAtts();
			int mergedNumAtts = leftNumAtts + rightNumAtts;
			int *attsToKeep = new int[leftNumAtts + rightNumAtts];

			for (int i=0; i<leftNumAtts; ++i){
				attsToKeep[i] = i;
			}

			for (int i=0; i<rightNumAtts; ++i) {
				attsToKeep[i+ leftNumAtts] = i;
			}
		
			mergedRecord.MergeRecords(&leftRecord, &rightRecord, leftNumAtts, rightNumAtts, attsToKeep, mergedNumAtts, leftNumAtts);
			
			outPipe->Insert(&mergedRecord);

			l = leftSortedPipe.Remove(&leftRecord);
			r = rightSortedPipe.Remove(&rightRecord);
		}
		else if(c==-1){
			//left rec is less
			l = leftSortedPipe.Remove(&leftRecord);
		}
		else{
			//right rec is less
			r = rightSortedPipe.Remove(&rightRecord);
		}

	}

	leftBigQ.WaitUntilDone();
	rightBigQ.WaitUntilDone();

	outPipe->ShutDown();

}

void *Join::joinHelper()
{
	OrderMaker leftOrder;
	OrderMaker rightOrder;

	//use sortMergeJoin if the cnf has an equality check, otherwise use block-nested loops join
	//GetSortOrders returns a 0 if and only if it is impossible to determine
	//an acceptable ordering for the given comparison
	if (selOp->GetSortOrders(leftOrder, rightOrder))
	{	
		
		sortMergeJoin(leftInPipe, &leftOrder, rightInPipe, &rightOrder, outPipe, selOp, literal, numPages);
	}
	else
	{
		//nested loop join
	}
}

void *Join::joinStaticHelper(void *join)
{
	Join *j = (Join *)join;
	j->joinHelper();
}

void Join::Run(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal)
{
	this->leftInPipe = &inPipeL;
	this->rightInPipe = &inPipeR;
	this->outPipe = &outPipe;
	this->selOp = &selOp;
	this->literal = &literal;

	int w = pthread_create(&thread, NULL, joinStaticHelper, (void *)this);
	if (w)
	{
		printf("Error creating join thread! Return %d\n", w);
		exit(-1);
	}
}

void Join::WaitUntilDone()
{
	pthread_join(thread, NULL);
}

void Join::Use_n_Pages(int runlen)
{
	numPages = runlen;
}

void *GroupBy::groupbyHelper()
{
	
	Pipe sortedPipe(PIPE_SIZE);

	//call BigQ to put the sorted output into the outPipe/sortedPipe
	BigQ bigQ(*inPipe, sortedPipe, *groupAtts, numPages);

	HeapFile tempFile;
	tempFile.Open("llllaaaa.bin");
	

	Record buffer, next;
	ComparisonEngine compEngine;

	if (tempFile.GetNext(buffer))
	{

		while (tempFile.GetNext(next))
		{
			if (compEngine.Compare(&buffer, &next, groupAtts) != 0)
			{
				outPipe->Insert(&buffer);
				buffer.Consume(&next);
			}

	// 		if (computeMe->returnsInt == 1){
	// 	groupBySum<int>(inPipe, outPipe, computeMe);
	// }
	// else{
	// 	groupBySum<double>(inPipe, outPipe, computeMe);
	// }

	

		}
		outPipe->Insert(&buffer);
	}
	outPipe->ShutDown();
}



void *GroupBy::groupbyStaticHelper(void *groupBy)
{	
	GroupBy *GB = (GroupBy *)groupBy;
	GB->groupbyHelper();

}

void GroupBy::Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe)
{
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->groupAtts = &groupAtts;
	this->computeMe = &computeMe;

	int w = pthread_create(&thread, NULL, groupbyStaticHelper, (void *)this);
	if (w)
	{
		printf("Error creating groupby thread! Return %d\n", w);
		exit(-1);
	}
}

void GroupBy::WaitUntilDone()
{
	pthread_join(thread, NULL);
}

void GroupBy::Use_n_Pages(int runlen)
{
	numPages = runlen;
}
