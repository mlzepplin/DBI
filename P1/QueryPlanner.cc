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
        OperationNode *currentNode = new SelectOperationNode(statistics, outSchema, tables->tableName, tables->aliasAs);
        nodesVector.push_back(currentNode);
        //currentNode->printNodeInfo();
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

            ((SelectOperationNode *)nodesVector[i])->printNodeInfo(outStream, 0);
        }
        else
            ;
    }
}

void QueryPlanner::setOutputMode(char *mode)
{
    outMode = mode;
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

void QueryPlanner::deepCopyAndList(AndList *&populateMe, AndList *copyMe)
{
    //note every pointer movement of populateMe, also update the caller of populateMe
    //exactly the same way, but it's not the same as for copyMe
    AndList *head = populateMe;
    // populateMe = new AndList();
    //deep copy the whole AndList
    while (copyMe != NULL)
    {
        populateMe = new AndList();
        OrList *copyOr = copyMe->left;
        //deep copy the whole OrList
        while (copyOr != NULL)
        {
            populateMe->left = new OrList();             // OrList *popOr = populateMe->left;
            populateMe->left->left = new ComparisonOp(); // ComparisonOp *popCompOp = populateMe->left->left;

            ComparisonOp *copyCompOp = copyOr->left;

            //deepCopy the whole Comparison Op
            populateMe->left->left->code = copyCompOp->code;

            //deep Copying left operand
            populateMe->left->left->left = new Operand();
            populateMe->left->left->left->code = copyCompOp->left->code;
            populateMe->left->left->left->value = copyCompOp->left->value;

            //deepCopying right Operand
            populateMe->left->left->right = new Operand();
            populateMe->left->left->right->code = copyCompOp->right->code;
            populateMe->left->left->right->value = copyCompOp->right->value;

            populateMe->left->rightOr = NULL;
            populateMe->left = populateMe->left->rightOr;
            copyOr = copyOr->rightOr;
        }
        populateMe->rightAnd = NULL;
        populateMe = populateMe->rightAnd;

        copyMe = copyMe->rightAnd;
    }
    populateMe = head;
}

bool cmp(OperationNode *&a, OperationNode *&b)
{
    return a->getNumTuples() > b->getNumTuples();
}

void QueryPlanner::planJoins()
{

    vector<OperationNode *>::iterator nodesVecIter;
    //required for printing purposes
    vector<JoinOperationNode *> joinVector;
    double estimate = 0.0;
    Statistics statsCopy = *statistics; //making a deep copy

    //sort in decending order on numTuples
    std::sort(nodesVector.begin(), nodesVector.end(), cmp);

    while (nodesVector.size() > 1)
    {
        //pop off the last two
        OperationNode *node1 = nodesVector.back();
        nodesVector.pop_back();
        OperationNode *node2 = nodesVector.back();
        nodesVector.pop_back();

        //constructing new JoinNode which inherently updates relNames,numRelations,outSchema as well
        JoinOperationNode *joinNode = new JoinOperationNode(node1, node2);
        //populate subAndlist
        AndList *subAndList = joinNode->buildSubAndList(boolean);
        joinNode->cnf.GrowFromParseTree(subAndList, node1->outSchema, node2->outSchema, joinNode->literal);
        //apply and estimate
        joinNode->estimatedTuples = statsCopy.Estimate(subAndList, joinNode->relationNames, joinNode->numRelations);
        statsCopy.Apply(subAndList, joinNode->relationNames, joinNode->numRelations);

        nodesVector.push_back((OperationNode *)joinNode);
        joinVector.push_back(joinNode);
    }
    for (int i = joinVector.size() - 1; i >= 0; i--)
    {
        joinVector[i]->printNodeInfo();
    }
}

void QueryPlanner::performProject()
{
    if (attsToSelect && !finalFunction && !groupingAtts)
        root = new ProjectOperationNode(attsToSelect, root);
}

//############################################
//OperationNode
//############################################
int OperationNode::pipeId = 0;

OperationNode::OperationNode(string operationName)
{
    this->operationName = operationName;
    this->outPipeId = pipeId++;
}

OperationNode::OperationNode(string operationName, Statistics *statistics)
{
    this->operationName = operationName;
    this->statistics = statistics;
    this->outPipeId = pipeId++;
}

OperationNode::OperationNode(string operationName, Statistics *statistics, Schema *outSchema)
{
    this->operationName = operationName;
    this->statistics = statistics;
    this->outSchema = outSchema;
    this->outPipeId = pipeId++;
}

OperationNode::OperationNode(string operationName, Schema *outSchema)
{
    this->operationName = operationName;
    this->outSchema = outSchema;
    this->outPipeId = pipeId++;
}

std::string OperationNode::getOperationName()
{
    return operationName;
}

//############################################
//AndListBasedOperationNode
//############################################
AndListBasedOperationNode::AndListBasedOperationNode(string operationName) : OperationNode(operationName)
{
}

AndList *AndListBasedOperationNode::buildSubAndList(AndList *&boolean)
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

        if (isValidOr(current->left))
        { //if matched, trim it out and add it to the subAndList
            previous->rightAnd = current->rightAnd;
            current->rightAnd = subAndList;
            subAndList = current;
            current = current->rightAnd;
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

bool AndListBasedOperationNode::isValidOr(OrList *booleanOrList)
{
    ComparisonOp *compOp = booleanOrList->left;

    while (booleanOrList)
    {
        //cout << "in is or while" << endl;
        if (isValidComparisonOp(compOp))
        {
            return true;
        }
        else
            booleanOrList = booleanOrList->rightOr;
    }
    return false;
}

//############################################
//SelectOperationNode
//############################################
SelectOperationNode::SelectOperationNode(Statistics *&statistics, Schema *outSchema, char *relationName, char *aliasName) : AndListBasedOperationNode("select")
{
    this->statistics = statistics;
    this->relationNames[0] = relationName;
    this->aliasName = aliasName;
    this->outSchema = outSchema;
    numRelations = 1;
    numTuples = statistics->getNumTuplesOfRelation(relationNames[0]);
    estimatedTuples = numTuples;
}

bool SelectOperationNode::isValidComparisonOp(ComparisonOp *compOp)
{
    Operand *leftOperand = compOp->left;
    Operand *rightOperand = compOp->right;
    //return (leftOperand->code == NAME && rightOperand->code != NAME && (schema->Find(leftOperand->value) != -1));
    bool leftAttInSchema = (outSchema->Find(leftOperand->value) != -1) ? true : false;
    bool rightAttInSchema = (outSchema->Find(rightOperand->value) != -1) ? true : false;
    if (rightOperand->code != NAME)
        return leftAttInSchema;

    return compOp->code != EQUALS;
}

void SelectOperationNode::printNodeInfo(std::ostream &os, size_t level)
{
    os << "select CNF:" << endl;
    cnf.Print();
    for (int i = 0; i < numRelations; i++)
        os << relationNames[i] << ", ";
    os << endl;
    os << "Estimate = " << estimatedTuples << endl;
}

//############################################
//Join Operation Node
//############################################
JoinOperationNode::JoinOperationNode(OperationNode *node1, OperationNode *node2) : AndListBasedOperationNode("join")
{
    leftOperationNode = node1;
    rightOperationNode = node2;
    combineRelNames();
    populateJoinOutSchema();
}

bool JoinOperationNode::isValidComparisonOp(ComparisonOp *compOp)
{
    Operand *leftOperand = compOp->left;
    Operand *rightOperand = compOp->right;

    bool leftAttInSchema = (outSchema->Find(leftOperand->value) != -1) ? true : false;
    bool rightAttInSchema = (outSchema->Find(rightOperand->value) != -1) ? true : false;

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

    //Schema outSchema(catalogPath, numTotalAtts, joinAttList);
    this->outSchema = new Schema(catalogPath, numTotalAtts, joinAttList);
    // cout << "numatts join " << this->outSchema->GetNumAtts() << endl;
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
    for (int i = numLeft, j = 0; i < numRelations; i++, j++)
    {
        relationNames[i] = rightOperationNode->relationNames[j];
    }
}

void JoinOperationNode::printNodeInfo(std::ostream &os, size_t level)
{
    os << "Join CNF: ";
    cnf.Print();
    os << "Pipe Id: " << this->outPipeId << endl;
    for (int i = 0; i < numRelations; i++)
        os << relationNames[i] << ", ";
    os << endl;
    os << "Estimate = " << estimatedTuples << endl;
}

//############################################
//GroupBy Operation
//############################################
GroupByOperationNode::GroupByOperationNode(NameList *groupingAtts, FuncOperator *parseTree, OperationNode *node) : OperationNode("groupBy")
{
    groupOrder.growFromParseTree(groupingAtts, node->outSchema);
    func.GrowFromParseTree(parseTree, *node->outSchema);
    outSchema = resultantSchema(groupingAtts, parseTree, node);
}

void GroupByOperationNode::printNodeInfo(std::ostream &os, size_t level)
{
    os << "OrderMaker:" << endl;
    //os << groupOrder.Print() << endl;
    os << "Function: " << endl;
    // print func
}

Schema *GroupByOperationNode::resultantSchema(NameList *groupingAtts, FuncOperator *parseTree, OperationNode *node)
{
    Function f;
    Attribute atts[2][1] = {{{"sum", Int}}, {{"sum", Double}}};
    Schema *groupBySchema = node->outSchema;
    f.GrowFromParseTree(parseTree, *groupBySchema);

    return groupBySchema;
}

//############################################
//SumOperationNode
//############################################
SumOperationNode::SumOperationNode(FuncOperator *parseTree, OperationNode *node) : OperationNode("sum")
{
    func.GrowFromParseTree(parseTree, *node->outSchema);
}

Schema *SumOperationNode::resultSchema(FuncOperator *parseTree, OperationNode *node)
{
    Function fun;
    Attribute atts[2][1] = {{{"sum", Int}}, {{"sum", Double}}};
    fun.GrowFromParseTree(parseTree, *node->outSchema);
    //as not passing the outschema to base, so setting it here
    this->outSchema = new Schema("", 1, atts[fun.getSumType()]);
}

void SumOperationNode::printNodeInfo(std::ostream &os, size_t level)
{
    os << "Function: " << endl;
    func.Print();
}

//############################################
//duplicate removal operation node
//############################################
DupRemovalOperationNode::DupRemovalOperationNode(OperationNode *node) : OperationNode("dupRemoval")
{
    this->outSchema = new Schema(*node->outSchema);
}
void DupRemovalOperationNode::printNodeInfo(std::ostream &os, size_t level)
{
}

//############################################
//Project functionality
//############################################
ProjectOperationNode::ProjectOperationNode(NameList *atts, OperationNode *node) : OperationNode("project")
{
    //update the outschema of this ProjectNode
    Schema *tempSchema = node->outSchema;
    Attribute projectAtts[MAX_ATTS];
    for (; atts; atts = atts->next, numProjectedAtts++)
    {
        if (tempSchema->Find(atts->name) != -1)
        {
            projectAtts[numProjectedAtts].name = atts->name;
            projectAtts[numProjectedAtts].myType = tempSchema->FindType(atts->name);
        }
    }

    outSchema = new Schema("", numProjectedAtts, projectAtts);
}

void ProjectOperationNode::printNodeInfo(std::ostream &os, size_t level)
{
    os << keepMe[0];

    for (int i = 1; i < numProjectedAtts; ++i)
    {
        os << ',' << keepMe[i];
    }
    os << endl;
    os << numInputAtts << "Input Attributes" << endl;
    os << numProjectedAtts << "Projected Attributes" << endl;
}

//############################################
//Write OperationNode
//############################################
WriteOperationNode::WriteOperationNode(FILE *&outFile, OperationNode *node) : OperationNode("write")
{
    outputFile = outFile;
    this->outSchema = new Schema(*node->outSchema);
}

void WriteOperationNode::printNodeInfo(std::ostream &os, size_t level)
{
    //write the outstream to the outFile
    os << "Output written to " << outputFile << endl;
}