#include "BigQ.h"
#include "Record.h"

void *working(void *big){
	BigQ *bigQ = (BigQ *)big;

	Record temp;
	Page bufferPage;

	int numPages = 0;

	Schema mySchema ("catalog", "region");

	cout<<"worker"<<endl;
	while(bigQ->inPipe->Remove(&temp)){
		bigQ->outPipe->Insert(&temp);
	}

	// while (numPages < bigQ->runLength){
	// 	//Remove returns 0 if there is no record
	// 	if ((bigQ->inPipe)->Remove(&temp)){

	// 		//Write record to Page. 
			
	// 		//If page is full, add to next page. numPages++

	// 	}
	// }

	//sort run

}

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
	// read data from in pipe sort them into runlen pages
	

	inPipe = &in;
	outPipe = &out;
	sortOrder = sortorder;
	runLength = runlen;

	pthread_t worker;

	//create worker thread
	int w = pthread_create(&worker, NULL, working, (void *)this);
	if(w){
		printf("Error creating worker thread! Return %d\n", w);
		exit(-1);

	}
	
    // construct priority queue over sorted runs and dump sorted data


 	// into the out pipe

    // finally shut down the out pipe
	
	//wait for thread to finish, then release resources
	pthread_join (worker, NULL);

	//release lock on the out pipe, only when completely done
	out.ShutDown ();
}



BigQ::~BigQ (){
}
