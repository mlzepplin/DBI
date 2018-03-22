#include "RelOp.h"
#include "Record.h"
#include "vector"

SelectFile::SelectFile(){
	numPages =1;
}
void* selectFileWorking(void *selectFile){
	//NOTE: No need of using the numPages memory cnostraint
	//as we are select is purely streaing and non-blocking
	SelectFile *SF = (SelectFile *)selectFile;
	
	while(SF->inFile->GetNext(*(SF->buffer),*(SF->selOp),*(SF->literal))==1){
		 SF->outPipe->Insert(SF->buffer);
	}
	SF->outPipe->ShutDown();
}
void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
	

	buffer  = new Record();
	this->inFile = &inFile;
	this->outPipe = &outPipe;
	this->selOp = &selOp;
	this->literal = &literal;

	//spawns worker thread and returns to the operation 
	int w = pthread_create(&thread, NULL, selectFileWorking,(void *)this);
	if (w)
	{
		printf("Error creating worker thread! Return %d\n", w);
		exit(-1);
	}

}

void SelectFile::WaitUntilDone () {
	pthread_join (thread, NULL);
}

void SelectFile::Use_n_Pages (int runlen) {
	numPages = runlen;
}

SelectPipe::SelectPipe(){
	numPages =1;
}
void* selectPipeWorking(void *selectPipe){
	//NOTE: No need of using the numPages memory cnostraint
	//as we are select is purely streaing and non-blocking
	SelectPipe *SP = (SelectPipe *)selectPipe;
	ComparisonEngine comp;
	
	while(SP->inPipe->Remove(SP->buffer)){
		 //if comparison engine's output matches
		 if (comp.Compare(SP->buffer, SP->literal, SP->selOp))
        {
           SP->outPipe->Insert(SP->buffer);
        }
		 
	}
	SP->outPipe->ShutDown();
}
void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {
	

	buffer  = new Record();
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->selOp = &selOp;
	this->literal = &literal;

	//spawns worker thread and returns to the operation 
	int w = pthread_create(&thread, NULL, selectPipeWorking,(void *)this);
	if (w)
	{
		printf("Error creating worker thread! Return %d\n", w);
		exit(-1);
	}

}

void SelectPipe::WaitUntilDone () {
	pthread_join (thread, NULL);
}

void SelectPipe::Use_n_Pages (int runlen) {
	numPages = runlen;
}
