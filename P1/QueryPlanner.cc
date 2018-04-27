#include "QueryPlanner.h"
#include <float.h>

char *catalogPath = "./catalog";

//QueryPlanner
void QueryPlanner::initLeaves()
{
    // statistics->Read(inFilePath);

    //driver for testing: used when the stats object is already populated from text file
    while (tables != NULL)
    {
        statistics->CopyRel(tables->tableName, tables->aliasAs);
        Schema *outSchema = new Schema(catalogPath, tables->tableName, tables->aliasAs);
        OperationNode *currentNode = new SingletonLeafNode(statistics, outSchema, tables->tableName, tables->aliasAs);
        nodesVector.push_back(currentNode);
        currentNode->printNodeInfo();
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

void QueryPlanner::planJoins()
{

    //steps
    vector<OperationNode *> optimalNodesVector;
    //required for printing purposes
    vector<JoinOperationNode *> optimalJoinVector;
    vector<OperationNode *>::iterator nodesVecIter;
    double optimalEstimate = DBL_MAX;
    AndList *tempAndList = boolean;
    //get all the permutations of the list and break at the optimal one
    //RMEEMBER - DON'T ALTER THE ACTUAL LEAF LIST!!
    std::sort(nodesVector.begin(), nodesVector.end());
    while (next_permutation(nodesVector.begin(), nodesVector.end()))
    {
        cout << "join0" << endl;
        vector<OperationNode *> currentNodesVector = nodesVector;
        vector<JoinOperationNode *> currentJoinVector;
        cout << "join1.5" << endl;
        cout << "join1.6" << endl;
        double currentEstimate = 0.0;
        while (currentNodesVector.size() > 1)
        {
            cout << "join1" << endl;
            //pop off the last two
            OperationNode *node1 = currentNodesVector.back();
            currentNodesVector.pop_back();
            OperationNode *node2 = currentNodesVector.back();
            currentNodesVector.pop_back();

            //constructing new JoinNode which inherently updates relNames,numRelations as well
            //populates combined outschema as well

            JoinOperationNode *joinNode = new JoinOperationNode(node1->statistics, node1, node2);
            cout << "join2" << endl;
            //populate subAndlist
            AndList *subAndList = joinNode->buildSubAndList(boolean, joinNode->outSchema);
            //join and estimate

            currentEstimate += joinNode->statistics->Estimate(subAndList, joinNode->relationNames, joinNode->numRelations);
            joinNode->statistics->Apply(subAndList, joinNode->relationNames, joinNode->numRelations);
            cout << "join3" << endl;
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

void QueryPlanner::performProject()
{
    if (attsToSelect && !finalFunction && !groupingAtts)
        root = new ProjectOperationNode(attsToSelect, root);
}

//OperationNode
int OperationNode::pipeId = 0;

OperationNode::OperationNode(string operationName)
{
    this->operationName = operationName;
    this->outPipeID = pipeId++;
}

OperationNode::OperationNode(string operationName, Statistics *statistics)
{
    this->operationName = operationName;
    this->statistics = statistics;
    this->outPipeID = pipeId++;
}
OperationNode::OperationNode(string operationName, Statistics *statistics, Schema *outSchema)
{
    this->operationName = operationName;
    this->statistics = statistics;
    this->outSchema = outSchema;
    this->outPipeID = pipeId++;
}
OperationNode::OperationNode(string operationName, Schema *outSchema)
{
    this->operationName = operationName;
    this->outSchema = outSchema;
    this->outPipeID = pipeId++;
}
std::string OperationNode::getOperationName()
{
    return operationName;
}

//AndListBasedOperationNode
AndListBasedOperationNode::AndListBasedOperationNode(string operationName) : OperationNode(operationName)
{
}

AndList *AndListBasedOperationNode::buildSubAndList(AndList *&boolean, Schema *schema)
{
    AndList *subAndList = NULL;
    //adding a dummy node as header to keep
    //previous one step above current from the start
    AndList head;
    head.rightAnd = boolean;
    AndList *previous = &head;
    AndList *current = head.rightAnd;

    //GOAL:: parse the boolean and trim all rrelevance out
    //and append that relevance to subndList
    while (current != NULL)
    {
        if (isValidOr(current->left, schema))
        { //if matched, trim it out and add it to the subAndList
            previous->rightAnd = current->rightAnd;
            current->rightAnd = subAndList;
            subAndList = current;
        }
        else
        { //skip current and let it remain in boolean
            previous = current;
            current = previous->rightAnd;
        }
    }
    //removing the initial dummy node that was added just to init previous
    boolean = head.rightAnd;
    return subAndList;
}
bool AndListBasedOperationNode::isValidOr(OrList *booleanOrList, Schema *schema)
{
    ComparisonOp *compOp = booleanOrList->left;

    while (booleanOrList)
    {
        if (isValidComparisonOp(compOp, schema))
            return true;
        else
            booleanOrList = booleanOrList->rightOr;
    }
    return false;
}

// bool AndListBasedOperationNode::buildSubOrList(OrList *orList, Schema *schema)
// {
//     OrList *subOrList;
//     subOrList->rightOr = orList;
//     OrList *previous = subOrList;
//     OrList *current = subOrList->rightOr;

//     //GOAL:: parse the subOrList and trim all irrelevance
//     bool matchStatus = false;
//     while (current != NULL)
//     {
//         //current->left is the comparisonOp
//         if (isValidOr(current->left, schema))
//         { //let this compOp stay
//             matchStatus = true;
//             previous = current;
//             current = current->rightOr;
//         }
//         else
//         { //skip current and trim it out
//             previous->rightOr = current->rightOr;
//             current = previous->rightOr;
//         }
//     }
//     //removing the initial dummy node that was added just to init previous
//     orList = subOrList->rightOr;
//     return matchStatus;
// }

//(leftOperand->code != NAME || leftAttInSchema) && (rightOperand->code != NAME || rightAttInSchema)
//SelectOperationNode

SelectOperationNode::SelectOperationNode(Statistics *statistics) : AndListBasedOperationNode("select")
{
    this->statistics = statistics;
}
bool SelectOperationNode::isValidComparisonOp(ComparisonOp *compOp, Schema *schema)
{

    Operand *leftOperand = compOp->left;
    Operand *rightOperand = compOp->right;
    //return (leftOperand->code == NAME && rightOperand->code != NAME && (schema->Find(leftOperand->value) != -1));
    bool leftAttInSchema = (schema->Find(leftOperand->value) != -1) ? true : false;
    bool rightAttInSchema = (schema->Find(rightOperand->value) != -1) ? true : false;
    if (rightOperand->code != NAME)
        return leftAttInSchema;

    return compOp->code != EQUALS;
}
void SelectOperationNode::printNodeInfo(std::ostream &os, size_t level) const
{
    // os << "singletonLeaf CNF:" << endl;
    // //cnf.Print();
    // for (int i = 0; i < numRelations; i++)
    //     os << relationNames[i] << ", ";
    // os << endl;
    // os << "Estimate = " << estimatedTuples << endl;
}

//SelectAfterJoinNode
// SelectAfterJoinOperationNode::SelectAfterJoinOperationNode(Statistics *statistics) : AndListBasedOperationNode("selectAfterJoin")
// {
//     this->statistics = statistics;
// }
// bool SelectAfterJoinOperationNode::isValidCondition(ComparisonOp *compOp, Schema *schema)
// {
//     Operand *leftOperand = compOp->left;
//     Operand *rightOperand = compOp->right;
//     bool leftAttInSchema = (schema->Find(leftOperand->value) != -1) ? true : false;
//     bool rightAttInSchema = (schema->Find(rightOperand->value) != -1) ? true : false;
//     return (leftOperand->code == NAME && rightOperand->code == NAME && (compOp->code != EQUALS) && leftAttInSchema && rightAttInSchema);
// }
// void SelectAfterJoinOperationNode::printNodeInfo(std::ostream &os, size_t level) const
// {

//     // os << "singletonLeaf CNF:" << endl;
//     // //cnf.Print();
//     // for (int i = 0; i < numRelations; i++)
//     //     os << relationNames[i] << ", ";
//     // os << endl;
//     // os << "Estimate = " << estimatedTuples << endl;
// }

//SingletonLeafNode
SingletonLeafNode::SingletonLeafNode(Statistics *statistics, Schema *outSchema, char *relationName, char *aliasName) : OperationNode("leaf")
{
    this->outSchema = outSchema;
    this->statistics = statistics;
    this->relationNames[0] = relationName;
    this->aliasName = aliasName;
    this->outSchema = outSchema;
    numRelations = 1;
    estimatedTuples = statistics->getNumTuples(relationNames[0]);
}
void SingletonLeafNode::printNodeInfo(std::ostream &os, size_t level) const
{
    cout << "ahbdfkajba" << endl;
    os << "singletonLeaf CNF:" << endl;
    //cnf.Print();
    for (int i = 0; i < numRelations; i++)
        os << relationNames[i] << ", ";
    os << endl;
    os << "Estimate = " << estimatedTuples << endl;
}

//Join Operation Node
JoinOperationNode::JoinOperationNode(Statistics *statistics, OperationNode *node1, OperationNode *node2) : AndListBasedOperationNode("join")
{
    this->statistics = statistics;
    leftOperationNode = node1;
    rightOperationNode = node2;
    combineRelNames();
    populateJoinOutSchema();
}
bool JoinOperationNode::isValidComparisonOp(ComparisonOp *compOp, Schema *schema)
{
    Operand *leftOperand = compOp->left;
    Operand *rightOperand = compOp->right;

    bool leftAttInSchema = (schema->Find(leftOperand->value) != -1) ? true : false;
    bool rightAttInSchema = (schema->Find(rightOperand->value) != -1) ? true : false;

    return (leftOperand->code == NAME && rightOperand->code == NAME && (compOp->code == EQUALS) && leftAttInSchema && rightAttInSchema);
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

//GroupBy Operation
GroupByOperationNode::GroupByOperationNode(NameList *groupingAtts, FuncOperator *parseTree, OperationNode *node) : OperationNode("groupBy")
{
    groupOrder.growFromParseTree(groupingAtts, node->outSchema);
    func.GrowFromParseTree(parseTree, *node->outSchema);
    outSchema = resultantSchema(groupingAtts, parseTree, node);
}
void GroupByOperationNode::printNodeInfo(std::ostream &os, size_t level) const
{
    // os << "singletonLeaf CNF:" << endl;
    // //cnf.Print();
    // for (int i = 0; i < numRelations; i++)
    //     os << relationNames[i] << ", ";
    // os << endl;
    // os << "Estimate = " << estimatedTuples << endl;
}
Schema *GroupByOperationNode::resultantSchema(NameList *groupingAtts, FuncOperator *parseTree, OperationNode *node)
{
    Function f;
    Attribute atts[2][1] = {{{"sum", Int}}, {{"sum", Double}}};
    Schema *cSchema = node->outSchema;
    f.GrowFromParseTree(parseTree, *cSchema);

    return cSchema;
}

//SumOperationNode
SumOperationNode::SumOperationNode(FuncOperator *parseTree, OperationNode *node) : OperationNode("sum")
{
    func.GrowFromParseTree(parseTree, *node->outSchema);
}
void SumOperationNode::printNodeInfo(std::ostream &os, size_t level) const
{
}

Schema *SumOperationNode::resultSchema(FuncOperator *parseTree, OperationNode *node)
{
    Function fun;
    Attribute atts[2][1] = {{{"sum", Int}}, {{"sum", Double}}};
    fun.GrowFromParseTree(parseTree, *node->outSchema);
    //as not passing the outschema to base, so setting it here
    this->outSchema = new Schema("", 1, atts[fun.getSumType()]);
}

//duplicate removal operation node
DupRemovalOperationNode::DupRemovalOperationNode(OperationNode *node) : OperationNode("dupRemoval")
{
    this->outSchema = new Schema(*node->outSchema);
}
void DupRemovalOperationNode::printNodeInfo(std::ostream &os, size_t level) const
{
}

//Project functionality
ProjectOperationNode::ProjectOperationNode(NameList *atts, OperationNode *node) : OperationNode("project")
{
    //update the outschema of this ProjectNode
}
void ProjectOperationNode::printNodeInfo(std::ostream &os, size_t level) const
{
}

//Write OperationNode
WriteOperationNode::WriteOperationNode(FILE *outFile, OperationNode *node) : OperationNode("write")
{
    outFile = outFile;
    this->outSchema = new Schema(*node->outSchema);
}
void WriteOperationNode::printNodeInfo(std::ostream &os, size_t level) const
{
}