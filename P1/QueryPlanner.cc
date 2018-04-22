#include "QueryPlanner.h"

extern char *catalogPath;

void QueryPlanner::planOperationOrder()
{
    initLeaves();
    planJoins();
}

void QueryPlanner::printOperationOrder()
{
}
void QueryPlanner::planJoins()
{
    //steps to do
    //the singletons should already be populated as leaf representation of the (pseudo)tree
    //first we'll call for join's optimal estimate
    //second, to it, we'll append the other estimates, which are unary in nature
    //and then what we print it out using the orint helper methid to the outFile
    vector<OperationNode *> optimalNodesVector;
    vector<OperationNode *>::iterator nodesVecIter;
    int optimalNumTuples = 0;
    //get all the permutations of the list and break at the optimal one
    //RMEEMBER - DON'T ALTER THE ACTUAL LEAF LIST!!

    /*
Schema *outSchema;
  string operationName;
  char *relationNames[MAX_NUM_RELS];
  int estimatedTuples;
  int optimalTuples;
  Statistics *statistics;
  int outPipeID;
*/
    while (std::next_permutation(nodesVector.begin(), nodesVector.end()))
    {
        vector<OperationNode *> tempNodesVector = nodesVector;
        Statistics stats = *statistics;

        while (tempNodesVector.size() > 1)
        {
            //pop off the last two
            OperationNode *node1 = tempNodesVector.back();
            tempNodesVector.pop_back();
            OperationNode *node2 = tempNodesVector.back();
            tempNodesVector.pop_back();

            //constructing new relNames
            OperationNode *joinNode = new JoinOperationNode(node1, node2);
            //join em and
            stats.Estimate();
            stats.Apply();
        }

        //for this permutation, calc the total cost
        //estimatedTuples += statistics->Estimate();
    }
}
void JoinOperationNode::combineRelNames()
{
    vector<char *> combination;
    for (int i = 0; i < leftOperationNode->numRelations; i++)
    {
        leftOperations
    }
}
void QueryPlanner::initLeaves()
{
    // statistics->Read(inFilePath);
    while (tables != NULL)
    {
        statistics->CopyRel(tables->tableName, tables->aliasAs);
        aliasMappings.insert(std::make_pair(tables->tableName, tables->aliasAs));
        Schema *outSchema = new Schema(catalogPath, tables->tableName);
        OperationNode *currentNode = new SingletonLeafNode(tables->tableName, tables->aliasAs, outSchema, statistics);
        nodesVector.push_back(currentNode);
    }
}
