#include "QueryPlanner.h"
#include "float.h"

extern char *catalogPath;

//QueryPlanner
void QueryPlanner::initLeaves()
{
    // statistics->Read(inFilePath);
    int outPipeId = 1;
    while (tables != NULL)
    {
        statistics->CopyRel(tables->tableName, tables->aliasAs);
        Schema *outSchema = new Schema(catalogPath, tables->tableName);
        OperationNode *currentNode = new SingletonLeafNode("leaf", statistics, outSchema, outPipeId, tables->tableName, tables->aliasAs);
        outPipeId++;
        nodesVector.push_back(currentNode);
        tables = tables->next;
    }
}
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
    //and then what we print it out using the print helper method to the outFile
    vector<OperationNode *> optimalNodesVector;
    vector<OperationNode *>::iterator nodesVecIter;
    double optimalEstimate = DBL_MAX;
    //get all the permutations of the list and break at the optimal one
    //RMEEMBER - DON'T ALTER THE ACTUAL LEAF LIST!!
    while (std::next_permutation(nodesVector.begin(), nodesVector.end()))
    {
        vector<OperationNode *> currentNodesVector = nodesVector;
        Statistics stats = *statistics;
        double currentEstimate = 0.0;
        while (currentNodesVector.size() > 1)
        {

            //pop off the last two
            OperationNode *node1 = currentNodesVector.back();
            currentNodesVector.pop_back();
            OperationNode *node2 = currentNodesVector.back();
            currentNodesVector.pop_back();

            //constructing new JoinNode which inherently updates relNames,numRelations as well
            int outPipeId = std::max(node1->outPipeID, node2->outPipeID) + 1;
            OperationNode *joinNode = new JoinOperationNode("join", node1->statistics, outPipeId, node1, node2);

            //join em and
            currentEstimate += joinNode->statistics->Estimate(boolean, joinNode->relationNames, joinNode->numRelations);
            joinNode->statistics->Apply(boolean, joinNode->relationNames, joinNode->numRelations);

            currentNodesVector.push_back(joinNode);
        }
        if (currentEstimate < optimalEstimate)
        {
            optimalEstimate = currentEstimate;
            optimalNodesVector = currentNodesVector;
        }
    }
    //note, at the end when all the joins are done, we'll have only one
    //node (JoinOperationNode) in the optimalNodesVector, and its outschema
    //will be all the prepended alisaed attributes
    //constructing the outSchema for the final join
    ((JoinOperationNode *)optimalNodesVector[0])->populateOutSchema();
}

//OperationNode
OperationNode::OperationNode(string operationName, Statistics *statistics, int outPipeID)
{
    this->operationName = operationName;
    this->statistics = statistics;
    this->outPipeID = outPipeID;
}
OperationNode::OperationNode(string operationName, Statistics *statistics, Schema *outSchema, int outPipeID)
{
    this->operationName = operationName;
    this->statistics = statistics;
    this->outPipeID = outPipeID;
    this->outSchema = outSchema;
}

//SingletonLeafNode
SingletonLeafNode::SingletonLeafNode(string operationName, Statistics *statistics, Schema *outSchema, int outPipeId, char *relationName, char *aliasName) : OperationNode(operationName, statistics, outSchema, outPipeId)
{
    this->relationNames[0] = relationName;
    this->aliasName = aliasName;
    this->outSchema = outSchema;
    numRelations = 1;
    this->statistics = statistics;
}

//JoinOperationNode
void JoinOperationNode::populateOutSchema()
{
    //go through all the table names, get their alias from aliasmappings
    //find their attributes from the relation map and build outschema
    int numTotalAtts = 0;
    //loop gets the total NUM of Atts there will be
    while (tables != NULL)
    {
        Attribute *tempAttList;
        Schema aliasSchema(catalogPath, tables->tableName, tables->aliasAs);
        numTotalAtts += aliasSchema.GetNumAtts();
        tables = tables->next;
    }

    Attribute *joinAttList = new Attribute[numTotalAtts];
    int i = 0;
    while (tables != NULL)
    {
        Attribute *tempAttList;
        //generate alias's schema
        Schema aliasSchema(catalogPath, tables->tableName, tables->aliasAs);
        //get all attributes of the schema
        tempAttList = aliasSchema.GetAtts();
        for (int j = 0; j < aliasSchema.GetNumAtts(); j++, i++)
        {
            joinAttList[i] = tempAttList[j];
        }
        tables = tables->next;
    }
    Schema outSchema(catalogPath, numTotalAtts, joinAttList);
    this->outSchema = &outSchema;
}
JoinOperationNode::JoinOperationNode(string operationName, Statistics *statistics, int outPipeID, OperationNode *node1, OperationNode *node2)
    : OperationNode(operationName, statistics, outPipeID)
{
    leftOperationNode = node1;
    rightOperationNode = node2;
    combineRelNames();
}
void JoinOperationNode::combineRelNames()
{
    int numLeft = leftOperationNode->numRelations;
    int numRight = rightOperationNode->numRelations;
    //update numRelations of new joinnode
    numRelations = numLeft + numRight;
    //update relNames[] for new join node
    for (int i = 0; i < numLeft; i++)
    {
        relationNames[i] = leftOperationNode->relationNames[i];
    }
    for (int i = numLeft; i < numRelations; i++)
    {
        relationNames[i] = rightOperationNode->relationNames[i];
    }
}
