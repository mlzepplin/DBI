#include "QueryPlanner.h"
#include <float.h>

extern char *catalogPath;

//QueryPlanner
void QueryPlanner::initLeaves()
{
    // statistics->Read(inFilePath);
    int outPipeId = 1;
    while (tables != NULL)
    {
        statistics->CopyRel(tables->tableName, tables->aliasAs);
        Schema *outSchema = new Schema(catalogPath, tables->tableName, tables->aliasAs);
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
    //performSum();
}
void QueryPlanner::printOperationOrder()
{
    for (int i = 0; i < nodesVector.size(); i++)
    {
        string nodeType = nodesVector[i]->getOperationName();
        if (nodeType == "join")
        {
            ((JoinOperationNode *)nodesVector[i])->printNodeInfo(outStream, 0);
        }
        else if (nodeType == "leaf")
        {

            ((SingletonLeafNode *)nodesVector[i])->printNodeInfo(outStream, 0);
        }
        else
            ;
    }
}

void QueryPlanner::planJoins()
{
    //steps to do
    //the singletons should already be populated as leaf representation of the (pseudo)tree
    //first we'll call for join's optimal estimate
    //second, to it, we'll append the other estimates, which are unary in nature
    //and then what we print it out using the print helper method to the outFile
    vector<OperationNode *> optimalNodesVector;
    //required for printing purposes
    vector<JoinOperationNode *> optimalJoinVector;
    vector<OperationNode *>::iterator nodesVecIter;
    double optimalEstimate = DBL_MAX;
    AndList *tempAndList = boolean;
    //get all the permutations of the list and break at the optimal one
    //RMEEMBER - DON'T ALTER THE ACTUAL LEAF LIST!!
    while (std::next_permutation(nodesVector.begin(), nodesVector.end()))
    {
        vector<OperationNode *> currentNodesVector = nodesVector;
        vector<JoinOperationNode *> currentJoinVector;
        Statistics stats = *statistics;
        double currentEstimate = 0.0;
        while (currentNodesVector.size() > 1)
        {

            //pop off the last two
            OperationNode *node1 = currentNodesVector.back();
            currentNodesVector.pop_back();
            OperationNode *node2 = currentNodesVector.back();
            currentNodesVector.pop_back();

            int outPipeId = std::max(node1->outPipeID, node2->outPipeID) + 1;
            //constructing new JoinNode which inherently updates relNames,numRelations as well
            //populates combined outschema as well
            JoinOperationNode *joinNode = new JoinOperationNode("join", node1->statistics, outPipeId, node1, node2);

            //populate subAndlist
            AndList *subAndList = joinNode->buildSubAndList(boolean, joinNode->outSchema);
            //join and estimate
            currentEstimate += joinNode->statistics->Estimate(subAndList, joinNode->relationNames, joinNode->numRelations);
            joinNode->statistics->Apply(subAndList, joinNode->relationNames, joinNode->numRelations);

            currentNodesVector.push_back((OperationNode *)joinNode);
            currentJoinVector.push_back(joinNode);
        }
        if (currentEstimate < optimalEstimate)
        {
            optimalEstimate = currentEstimate;
            optimalNodesVector = nodesVector;
            optimalJoinVector = currentJoinVector;
        }
    }
    for (int i = optimalJoinVector.size() - 1; i >= 0; i--)
    {
        optimalJoinVector[i]->printNodeInfo();
    }
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
std::string OperationNode::getOperationName()
{
    return operationName;
}

AndList *OperationNode::buildSubAndList(AndList *boolean, Schema *schema)
{
    AndList *subAndList = NULL;

    AndList header;
    header.rightAnd = boolean;

    AndList *prev = &header;
    AndList *current = boolean;

    //GOAL:: get the subAndList
    OrList *orList;
    for (; current; current = prev->rightAnd)
    {
        orList = current->left;
        if (isValidCondition(orList, schema))
        {
            prev->rightAnd = current->rightAnd;
            current->rightAnd = subAndList;
            subAndList = current;
        }
        else
        {
            prev = current;
        }
    }

    subAndList = header.rightAnd;
    return subAndList;
}

bool OperationNode::isValidCondition(OrList *orList, Schema *joinSchema)
{
    for (; orList; orList = orList->rightOr)
    {
        if (!isValidCondition(orList->left, joinSchema))
        {
            return false;
        }
    }
    return true;
}

bool OperationNode::isValidCondition(ComparisonOp *compOp, Schema *joinSchema)
{
    Operand *leftOperand = compOp->left;
    Operand *rightOperand = compOp->right;

    bool leftAttInSchema = (joinSchema->Find(leftOperand->value) != -1) ? true : false;
    bool rightAttInSchema = (joinSchema->Find(rightOperand->value) != -1) ? true : false;

    return (leftOperand->code != NAME || leftAttInSchema) && (rightOperand->code != NAME || rightAttInSchema);
}

//SingletonLeafNode
SingletonLeafNode::SingletonLeafNode(string operationName, Statistics *statistics, Schema *outSchema, int outPipeId, char *relationName, char *aliasName) : OperationNode(operationName, statistics, outSchema, outPipeId)
{
    this->relationNames[0] = relationName;
    this->aliasName = aliasName;
    this->outSchema = outSchema;
    numRelations = 1;
    this->statistics = statistics;
    estimatedTuples = statistics->getNumTuples(relationNames[0]);
}
void SingletonLeafNode::printNodeInfo(std::ostream &os, size_t level) const
{
    os << "singletonLeaf CNF:" << endl;
    //cnf.Print();
    for (int i = 0; i < numRelations; i++)
        os << relationNames[i] << ", ";
    os << endl;
    os << "Estimate = " << estimatedTuples << endl;
}

void JoinOperationNode::populateJoinOutSchema()
{
    //go through all the table names, get their alias from aliasmappings
    //find their attributes from the relation map and build outschema
    int leftNumAtts = leftOperationNode->outSchema->GetNumAtts();
    int rightNumAtts = rightOperationNode->outSchema->GetNumAtts();
    int numTotalAtts = leftNumAtts + rightNumAtts;

    Attribute *joinAttList = new Attribute[numTotalAtts];
    Attribute *leftAttList = leftOperationNode->outSchema->GetAtts();
    Attribute *rightAttList = rightOperationNode->outSchema->GetAtts();
    int i = 0;
    for (; i < leftNumAtts; i++)
        joinAttList[i] = leftAttList[i];

    for (int j = 0; i < numTotalAtts; i++, j++)
        joinAttList[i] = rightAttList[j];

    Schema outSchema(catalogPath, numTotalAtts, joinAttList);
    this->outSchema = &outSchema;
}

JoinOperationNode::JoinOperationNode(string operationName, Statistics *Statistics, int outPipeID, OperationNode *node1, OperationNode *node2) : OperationNode(operationName, statistics, outPipeID)
{
    leftOperationNode = node1;
    rightOperationNode = node2;
    combineRelNames();
    populateJoinOutSchema();
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

void JoinOperationNode::printNodeInfo(std::ostream &os, size_t level) const
{
    os << "Join CNF:" << endl;
    //cnf.Print();
    for (int i = 0; i < numRelations; i++)
        os << relationNames[i] << ", ";
    os << endl;
    os << "Estimate = " << estimatedTuples << endl;
}

void QueryPlanner::performSum()
{
    //Perform groupby if grouping attributes are provided
    if (groupingAtts)
    {
        if (!finalFunction)
        {
            cout << "Group by can't be performed without aggregate function";
            exit(-1);
        }
        if (distinctFunc)
        {
            root = new DupRemovalOperationNode(root);
        }

        root = new GroupByOperationNode(groupingAtts, finalFunction, root);
    }
    else if (finalFunction)
    {
        root = new SumOperationNode(finalFunction, root);
    }
}

void QueryPlanner::performProject()
{
    if (attsToSelect && !finalFunction && !groupingAtts)
        root = new ProjectOperationNode(attsToSelect, root);
}

GroupByOperationNode::GroupByOperationNode(NameList *groupingAtts, FuncOperator *parseTree, OperationNode *node) : OperationNode("GroupBy", NULL, resultSchema(groupingAtts, parseTree, node), 0)
{
    // groupOrder.growFromParseTree(groupingAtts, node->outSchema);
    // func.GrowFromParseTree(parseTree, *node->outSchema);
}
