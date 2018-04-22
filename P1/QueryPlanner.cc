#include "QueryPlanner.h"

void QueryPlanner::planOperationOrder()
{
    //steps to do
    //the singletons should already be populated as leaf representation of the (pseudo)tree
    //first we'll call for join's optimal estimate
    //second, to it, we'll append the other estimates, which are unary in nature
    //and then what we print it out using the orint helper methid to the outFile
    vector<OperationNode *> optimalNodesVector;
    vector<OperationNode *>::iterator nodesVecIter;
    //get all the permutations of the list and break at the optimal one
    //RMEEMBER - DON'T ALTER THE ACTUAL LEAF LIST!!
    while (std::next_permutation(nodesVector.begin(), nodesVector.end()))
    {
        vector<OperationNode *> tempNodesVector = nodesVector;
        //for this permutation, calc the total cost
        //estimatedTuples += statistics->Estimate();
    }
}

void QueryPlanner::printOperationOrder()
{
}