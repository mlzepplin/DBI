#include "QueryPlanner.h"
#include <float.h>

char *catalogPath = "./catalog";
char *binPath = "../bin/";
//QueryPlanner

void PrintOperand(struct Operand *pOperand)
{
    if (pOperand != NULL)
    {
        cout << pOperand->value << " ";
    }
    else
        return;
}

void PrintComparisonOp(struct ComparisonOp *pCom)
{
    if (pCom != NULL)
    {
        PrintOperand(pCom->left);
        switch (pCom->code)
        {
        case LESS_THAN:
            cout << " < ";
            break;
        case GREATER_THAN:
            cout << " > ";
            break;
        case EQUALS:
            cout << " = ";
        }
        PrintOperand(pCom->right);
    }
    else
    {
        return;
    }
}
void PrintOrList(struct OrList *pOr)
{
    if (pOr != NULL)
    {
        struct ComparisonOp *pCom = pOr->left;
        PrintComparisonOp(pCom);

        if (pOr->rightOr)
        {
            cout << " OR ";
            PrintOrList(pOr->rightOr);
        }
    }
    else
    {
        return;
    }
}
void PrintAndList(struct AndList *pAnd)
{
    if (pAnd != NULL)
    {
        struct OrList *pOr = pAnd->left;
        PrintOrList(pOr);
        if (pAnd->rightAnd)
        {
            cout << " AND ";
            PrintAndList(pAnd->rightAnd);
        }
    }
    else
    {
        return;
    }
}

int QueryPlanner::clear_pipe(Pipe &in_pipe, Schema *schema, bool print)
{
    //cout << "here1" << endl;
    Record rec;
    int cnt = 0;
    while (in_pipe.Remove(&rec))
    {
        cout << "here" << endl;
        if (print)
        {
            rec.Print(schema);
        }
        cnt++;
    }
    return cnt;
}

int clear_pipe(Pipe &in_pipe, Schema *schema, Function &func, bool print)
{
    Record rec;
    int cnt = 0;
    double sum = 0;
    while (in_pipe.Remove(&rec))
    {
        if (print)
        {
            rec.Print(schema);
        }
        int ival = 0;
        double dval = 0;
        func.Apply(rec, ival, dval);
        sum += (ival + dval);
        cnt++;
    }
    cout << " Sum: " << sum << endl;
    return cnt;
}

void QueryPlanner::planOperationOrder()
{
    buildSelectFile();
    planAndBuildJoins();
    //buildSelectPipe();
    //buildOnlySum();
    //buildDuplicate();
    //buildProject();
    //buildWrite();
    //root->printNodeInfo(this->outStream);
}

void QueryPlanner::executeQueryPlanner()
{
    int totalNodesInTree = root->outPipeId;
    //NOTE: ALL RELATION'S WAIT-UNTIL DONE WILL HAVE TO BE CALLED FROM THE OUTSIDE
    //SO THAT WE CAN PLAY AROUND WITH THE SEQUENCE, OF RELOPS, WITH THEIR WAITUNTILDONES
    Pipe **outPipesList = new Pipe *[totalNodesInTree];
    RelationalOp **relopsList = new RelationalOp *[totalNodesInTree];
    root->executeOperation(outPipesList, relopsList);
    Record reco;
    int i = 0;

    while (outPipesList[root->outPipeId]->Remove(&reco) != 0 && i < 200)
    {
        // cout << endl;
        // cout << "##############################" << endl;
        // reco.Print(root->outSchema);
        i++;
    }
    cout << "num records: " << i << endl;
    for (int i = totalNodesInTree - 1; i >= 0; --i)
    {
        cout << "w" << i << endl;
        relopsList[i]->WaitUntilDone();
    }
    //print to console
    for (int i = 0; i < totalNodesInTree; ++i)
    {
        delete outPipesList[i];
        delete relopsList[i];
    }

    delete[] outPipesList;
    delete[] relopsList;
    //deallocating root
    root->outPipeId = 0;
    delete root;
    root = NULL;
    nodesVector.clear();
}
void QueryPlanner::setOutputMode(char *mode)
{
    outMode = mode;
}

void QueryPlanner::buildSelectFile()
{
    int i = 0;
    while (tables != NULL)
    {
        statistics->CopyRel(tables->tableName, tables->aliasAs);
        Schema *outSchema = new Schema(catalogPath, tables->tableName, tables->aliasAs);
        OperationNode *currentNode = new SelectFileOperationNode(outSchema, boolean, tables->tableName, tables->aliasAs, statistics);
        nodesVector.push_back(currentNode);
        tables = tables->next;
        i++;
    }
    if (i == 1)
        root == nodesVector[0];
}

void QueryPlanner::buildSelectPipe()
{
    root = new SelectAfterJoinOperationNode(root, boolean, statistics);
}
void QueryPlanner::buildOnlySum()
{
    if (finalFunction)
        root = new SumOperationNode(finalFunction, root);
}
void QueryPlanner::buildSum()
{
    //build groupby if grouping attributes are provided
    if (groupingAtts)
    {
        if (!finalFunction)
        {
            cout << "Group by can't be built without aggregate function";
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
void QueryPlanner::buildWrite()
{
    root = new WriteOperationNode(outputFile, root);
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

void QueryPlanner::buildDuplicate()
{
    if (distinctAtts)
        root = new DupRemovalOperationNode(root);
}
void QueryPlanner::planAndBuildJoins()
{
    double estimate = 0.0;
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
        JoinOperationNode *joinNode = new JoinOperationNode(node1, node2, boolean, statistics);

        //joinNode Estimate and Apply, will update QueryPlanner's statistics accordingly
        joinNode->estimateAndApply();

        nodesVector.push_back((OperationNode *)joinNode);
        joinVector.push_back(joinNode); //only there for printing purposes
    }
    // for (int i = joinVector.size() - 1; i >= 0; i--)
    // {
    //     joinVector[i]->printNodeInfo();
    // }
    //Assign root to the final join
    root = nodesVector[nodesVector.size() - 1];
}

void QueryPlanner::buildProject()
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
AndListBasedOperationNode::AndListBasedOperationNode(string operationName, Statistics *statistics) : OperationNode(operationName)
{
    this->statistics = statistics;
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

    //GOAL:: parse the boolean and trim all relevance out
    //and append that relevance to a new subndList
    while (current != NULL)
    {
        if (isValidOr(current->left))
        { //if matched, trim it out and add it to the subAndList
            previous->rightAnd = current->rightAnd;
            current->rightAnd = subAndList;
            subAndList = current;
            current = previous->rightAnd;
        }
        else
        { //skip current and let it remain in boolean
            previous = current;
            current = previous->rightAnd;
        }
    }
    //removing the initial dummy node that was added just to init previous
    boolean = head.rightAnd;
    //Assign the built AndList to the aList of node
    this->aList = subAndList;
    return subAndList;
}

bool AndListBasedOperationNode::isValidOr(OrList *booleanOrList)
{
    ComparisonOp *compOp = booleanOrList->left;

    while (booleanOrList)
    {
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
//SelectAfterJoinOperationNode
//############################################
SelectAfterJoinOperationNode::SelectAfterJoinOperationNode(OperationNode *node, AndList *&aList, Statistics *statistics) : AndListBasedOperationNode("selectPipe", statistics)
{
    this->child = node;
    this->outSchema = child->outSchema;
    buildSubAndList(aList); //populates this->aList internally
    cout << endl;
    cout << "selectPipe" << endl;
    PrintAndList(aList);
    this->cnf.GrowFromParseTree(this->aList, outSchema, literal);
}
void SelectAfterJoinOperationNode::executeOperation(Pipe **outPipesList, RelationalOp **relopsList)
{
    child->executeOperation(outPipesList, relopsList);
    SelectPipe *selectPipe = new SelectPipe();
    relopsList[outPipeId] = selectPipe;
    outPipesList[outPipeId] = new Pipe(PIPE_SIZE);
    Record rec;
    selectPipe->Use_n_Pages(5);
    selectPipe->Run(*outPipesList[child->outPipeId], *outPipesList[outPipeId], this->cnf, this->literal);
}
bool SelectAfterJoinOperationNode::isValidComparisonOp(ComparisonOp *compOp)
{
    Operand *leftOperand = compOp->left;
    Operand *rightOperand = compOp->right;

    bool leftAttInSchema = (outSchema->Find(leftOperand->value) != -1) ? true : false;
    bool rightAttInSchema = (outSchema->Find(rightOperand->value) != -1) ? true : false;

    return (leftOperand->code == NAME && rightOperand->code == NAME && (compOp->code != EQUALS) && leftAttInSchema && rightAttInSchema);
}
void SelectAfterJoinOperationNode::printNodeInfo(std::ostream &os, size_t level)
{
}

//############################################
//SelectFileOperationNode
//############################################
SelectFileOperationNode::SelectFileOperationNode(Schema *outSchema, AndList *&andList, char *relationName, char *aliasName, Statistics *statistics) : AndListBasedOperationNode("select", statistics)
{
    this->relationNames[0] = relationName;
    this->aliasName = aliasName;
    this->outSchema = outSchema;
    this->numRelations = 1;
    buildSubAndList(andList); //populates this->aList internally
    this->cnf.GrowFromParseTree(aList, outSchema, literal);
    numTuples = statistics->getNumTuplesOfRelation(aliasName);
    estimatedTuples = numTuples;
}

bool SelectFileOperationNode::isValidComparisonOp(ComparisonOp *compOp)
{
    Operand *leftOperand = compOp->left;
    Operand *rightOperand = compOp->right;
    return (leftOperand->code == NAME && rightOperand->code != NAME && (outSchema->Find(leftOperand->value) != -1));
}

void SelectFileOperationNode::printNodeInfo(std::ostream &os, size_t level)
{
    os << "select CNF:" << endl;
    cnf.Print();
    for (int i = 0; i < numRelations; i++)
        os << relationNames[i] << ", ";
    os << endl;
    os << "Estimate = " << estimatedTuples << endl;
}

void SelectFileOperationNode::executeOperation(Pipe **outPipesList, RelationalOp **relopsList)
{
    //reading input from table's bin file [and not alias's]
    string binFileName = string(binPath) + string(relationNames[0]) + ".bin";
    cout << "select: " << binFileName << endl;
    dbFile.Open((char *)binFileName.c_str());
    SelectFile *selectFile = new SelectFile();
    relopsList[outPipeId] = selectFile;
    outPipesList[outPipeId] = new Pipe(PIPE_SIZE);
    //will populate outpipe
    selectFile->Run(dbFile, *outPipesList[outPipeId], this->cnf, this->literal);
}

//############################################
//Join Operation Node
//############################################
JoinOperationNode::JoinOperationNode(OperationNode *node1, OperationNode *node2, AndList *&aList, Statistics *statistics) : AndListBasedOperationNode("join", statistics)
{
    leftOperationNode = node1;
    rightOperationNode = node2;
    combineRelNames();
    populateJoinOutSchema();
    buildSubAndList(aList); //populates this->aList internally
}
void JoinOperationNode::estimateAndApply()
{
    estimatedTuples = statistics->Estimate(aList, relationNames, numRelations);
    statistics->Apply(aList, relationNames, numRelations);
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
    //go through all the table names, get their alias from alias mappings
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

    this->outSchema = new Schema(catalogPath, numTotalAtts, joinAttList);
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
    leftOperationNode->printNodeInfo(os);
    rightOperationNode->printNodeInfo(os);

    os << "Join CNF: ";
    cnf.Print();
    os << "Pipe Id: " << this->outPipeId << endl;
    for (int i = 0; i < numRelations; i++)
        os << relationNames[i] << ", ";
    os << endl;
    os << "Estimate = " << estimatedTuples << endl;
    cout << "schema: " << endl;
    //OrderMaker(this->outSchema).Print();
}
void JoinOperationNode::executeOperation(Pipe **outPipesList, RelationalOp **relopsList)
{
    leftOperationNode->executeOperation(outPipesList, relopsList);
    rightOperationNode->executeOperation(outPipesList, relopsList);

    Record rec;
    cout << "join" << outPipeId << endl;
    cout << "subAndlist" << endl;
    PrintAndList(aList);
    int i = 0, j = 0;
    while (outPipesList[leftOperationNode->outPipeId]->Remove(&rec) && i < 10)
    {
        cout << "\nleft" << endl;
        rec.Print(leftOperationNode->outSchema);
        i++;
    }
    while (outPipesList[rightOperationNode->outPipeId]->Remove(&rec) && j < 10)
    {
        cout << "\nright" << endl;
        rec.Print(rightOperationNode->outSchema);
        j++;
    }
    Join *join = new Join();
    relopsList[outPipeId] = join;
    outPipesList[outPipeId] = new Pipe(PIPE_SIZE);
    //will populate outpipe
    join->Use_n_Pages(5);
    join->Run(*outPipesList[leftOperationNode->outPipeId], *outPipesList[rightOperationNode->outPipeId], *outPipesList[outPipeId], this->cnf, this->literal, *(leftOperationNode->outSchema), *(rightOperationNode->outSchema));
}

//############################################
//GroupBy Operation
//############################################
GroupByOperationNode::GroupByOperationNode(NameList *groupingAtts, FuncOperator *parseTree, OperationNode *node) : OperationNode("groupBy")
{
    groupOrder.growFromParseTree(groupingAtts, node->outSchema);
    this->function.GrowFromParseTree(parseTree, *node->outSchema);
    outSchema = resultantSchema(groupingAtts, parseTree, node);
    this->child = node;
}

void GroupByOperationNode::printNodeInfo(std::ostream &os, size_t level)
{
    child->printNodeInfo(os);
    os << "Group by: ";
    os << "OrderMaker:" << endl;
    //    ((const_cast<OrderMaker*>(&groupOrder))->Print() ;
    os << "Function: " << endl;
    // print func
}

Schema *GroupByOperationNode::resultantSchema(NameList *groupingAtts, FuncOperator *parseTree, OperationNode *node)
{
    Attribute atts[2][1] = {{{"sum", Int}}, {{"sum", Double}}};
    Schema *groupBySchema = node->outSchema;
    this->function.GrowFromParseTree(parseTree, *groupBySchema);

    return groupBySchema;
}
void GroupByOperationNode::executeOperation(Pipe **outPipesList, RelationalOp **relopsList)
{
    child->executeOperation(outPipesList, relopsList);
    GroupBy *groupBy = new GroupBy();
    relopsList[outPipeId] = groupBy;
    outPipesList[outPipeId] = new Pipe(PIPE_SIZE);
    //will populate outpipe
    groupBy->Run(*outPipesList[child->outPipeId], *outPipesList[outPipeId], groupOrder, this->function);
}

//############################################
//SumOperationNode
//############################################
SumOperationNode::SumOperationNode(FuncOperator *parseTree, OperationNode *node) : OperationNode("sum")
{
    //this->function.GrowFromParseTree(parseTree, *node->outSchema);
    this->outSchema = buildOutSchema(parseTree, node);
    this->child = node;
}
Schema *SumOperationNode::buildOutSchema(FuncOperator *parseTree, OperationNode *node)
{
    Function fun;
    Attribute atts[2][1] = {{{"sum", Int}}, {{"sum", Double}}};
    //as not passing the outschema to base, so setting it here
    fun.GrowFromParseTree(parseTree, *node->outSchema);
    return new Schema("", 1, atts[fun.getSumType()]);
}

void SumOperationNode::printNodeInfo(std::ostream &os, size_t level)
{
    child->printNodeInfo(os);
    os << "Sum: ";
    os << "Function: " << endl;
    (const_cast<Function *>(&function))->Print();
}
void SumOperationNode::executeOperation(Pipe **outPipesList, RelationalOp **relopsList)
{
    child->executeOperation(outPipesList, relopsList);
    Sum *sum = new Sum();
    relopsList[outPipeId] = sum;
    outPipesList[outPipeId] = new Pipe(PIPE_SIZE);
    cout << "Sum" << endl;
    //will populate outpipe
    Record rec;
    int i = 0;
    while (outPipesList[child->outPipeId]->Remove(&rec) && i < 10)
    {
        i++;
        cout << "\nleft" << endl;
        rec.Print(child->outSchema);
    }
    sum->Use_n_Pages(5);
    sum->Run(*outPipesList[child->outPipeId], *outPipesList[outPipeId], function);
}

//############################################
//duplicate removal operation node
//############################################
DupRemovalOperationNode::DupRemovalOperationNode(OperationNode *node) : OperationNode("dupRemoval")
{
    this->outSchema = new Schema(*node->outSchema);
    this->child = node;
}
void DupRemovalOperationNode::printNodeInfo(std::ostream &os, size_t level)
{
    child->printNodeInfo(os);
    os << "duplicate removal: " << endl;
}
void DupRemovalOperationNode::executeOperation(Pipe **outPipesList, RelationalOp **relopsList)
{
    child->executeOperation(outPipesList, relopsList);
    DuplicateRemoval *duplicateRemoval = new DuplicateRemoval();
    relopsList[outPipeId] = duplicateRemoval;
    outPipesList[outPipeId] = new Pipe(PIPE_SIZE);
    //will populate outpipe
    int i = 0;
    Record rec;
    while (outPipesList[child->outPipeId]->Remove(&rec) && i < 10)
    {
        i++;
        cout << "\nleft" << endl;
        rec.Print(child->outSchema);
    }
    duplicateRemoval->Use_n_Pages(5);
    duplicateRemoval->Run(*outPipesList[child->outPipeId], *outPipesList[outPipeId], *(this->outSchema));
}

//############################################
//Project functionality
//############################################
ProjectOperationNode::ProjectOperationNode(NameList *atts, OperationNode *node) : OperationNode("project")
{
    //update the outschema of this ProjectNode
    Schema *tempSchema = node->outSchema;
    this->child = node;
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
    child->printNodeInfo(os);
    os << "project: ";
    os << keepMe[0];

    for (int i = 1; i < numProjectedAtts; ++i)
    {
        os << ',' << keepMe[i];
    }
    os << endl;
    os << numInputAtts << "Input Attributes" << endl;
    os << numProjectedAtts << "Projected Attributes" << endl;
}
void ProjectOperationNode::executeOperation(Pipe **outPipesList, RelationalOp **relopsList)
{
    child->executeOperation(outPipesList, relopsList);
    Project *project = new Project();
    relopsList[outPipeId] = project;
    outPipesList[outPipeId] = new Pipe(PIPE_SIZE);
    //will populate outpipe
    project->Run(*outPipesList[child->outPipeId], *outPipesList[outPipeId], keepMe, numInputAtts, numProjectedAtts);
}

//############################################
//Write OperationNode
//############################################
WriteOperationNode::WriteOperationNode(FILE *&outFile, OperationNode *node) : OperationNode("write")
{
    this->child = node;
    //outputFile = outFile;
    this->outSchema = new Schema(*node->outSchema);
}

void WriteOperationNode::printNodeInfo(std::ostream &os, size_t level)
{
    child->printNodeInfo(os);
    //write the outstream to the outFile
    os << "write out" << endl;
    // os << "Output written to " << outputFile << endl;
}
void WriteOperationNode::executeOperation(Pipe **outPipesList, RelationalOp **relopsList)
{
    child->executeOperation(outPipesList, relopsList);
    WriteOut *writeOut = new WriteOut();
    relopsList[outPipeId] = writeOut;
    outPipesList[outPipeId] = new Pipe(PIPE_SIZE);
    Record rec;
    if (outPipesList[child->outPipeId]->Remove(&rec))
    {
        cout << "\nwrite's child" << endl;
        rec.Print(child->outSchema);
    }
    //will populate outpipe
    writeOut->Run(*outPipesList[child->outPipeId], outputFile, *(this->outSchema));
}