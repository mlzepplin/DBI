#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"

class RelationalOp
{
public:
	// blocks the caller until the particular relational operator
	// has run to completion
	virtual void WaitUntilDone() = 0;

	// tell us how much internal memory the operation can use
	virtual void Use_n_Pages(int n) = 0;
};

class SelectFile : public RelationalOp
{

private:
	pthread_t thread;
	int numPages;
	DBFile *inFile;
	Pipe *outPipe;
	CNF *selOp;
	Record *literal;
	Record *buffer;

public:
	SelectFile();
	void Run(DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
	static void *selectFileStaticHelper(void *selectFile);
	void *selectFileHelper();
	void WaitUntilDone();
	void Use_n_Pages(int n);
};

class SelectPipe : public RelationalOp
{

private:
	pthread_t thread;
	Pipe *inPipe;
	Pipe *outPipe;
	CNF *selOp;
	Record *literal;
	Record *buffer;
	int numPages;

public:
	SelectPipe();
	void Run(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
	static void *selectPipeStaticHelper(void *selectFile);
	void *selectPipeHelper();
	void WaitUntilDone();
	void Use_n_Pages(int n);
};

class Project : public RelationalOp
{
public:
	void Run(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) {}
	void WaitUntilDone() {}
	void Use_n_Pages(int n) {}
};

class Join : public RelationalOp
{
public:
	void Run(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) {}
	void WaitUntilDone() {}
	void Use_n_Pages(int n) {}
};

class DuplicateRemoval : public RelationalOp
{
public:
	void Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema) {}
	void WaitUntilDone() {}
	void Use_n_Pages(int n) {}
};

class Sum : public RelationalOp
{

private:
	pthread_t thread;
	Pipe *inPipe;
	Pipe *outPipe;
	Function *computeMe;
	int numPages;

public:
	void Run(Pipe &inPipe, Pipe &outPipe, Function &computeMe);
	static void *sumStaticHelper(void *sum);
	void *sumHelper();
	void WaitUntilDone();
	void Use_n_Pages(int n);

	template <class sumType>
	static void calculateSum(Pipe *inPipe, Pipe *outPipe, Function *computeMe);
};

class GroupBy : public RelationalOp
{
public:
	void Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) {}
	void WaitUntilDone() {}
	void Use_n_Pages(int n) {}
};

class WriteOut : public RelationalOp
{
public:
	void Run(Pipe &inPipe, FILE *outFile, Schema &mySchema) {}
	void WaitUntilDone() {}
	void Use_n_Pages(int n) {}
};

template <class sumType>
void Sum::calculateSum(Pipe *inPipe, Pipe *outPipe, Function *computeMe)
{
	sumType sum = 0;
	Record buffer;

	while (inPipe->Remove(&buffer))
	{
		sum += computeMe->Apply<sumType>(buffer);
	}
	//create an instance of a Record that contains the calculated sum of the given function
	Record sumRecord(sum);
	outPipe->Insert(&sumRecord);
}

#endif