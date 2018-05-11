#ifndef QUERY_PLANNER_H
#define QUERY_PLANNER_H
#define MAX_NUM_RELS 20
#define MAX_ATTS 100

#include "Schema.h"
#include "Statistics.h"
#include "Function.h"
#include "ParseTree.h"
#include "Comparison.h"
#include "Pipe.h"
#include "RelOp.h"
#include "HeapFile.h"

#include <algorithm>
#include <iostream>
#include <string>
#include <vector>
#include <fstream>
#include <iostream>

extern FuncOperator *finalFunction;
extern TableList *tables;
extern AndList *boolean;
extern NameList *groupingAtts;
extern NameList *attsToSelect;
extern int distinctAtts;
extern int distinctFunc;

//QUESTION? - DOES EVERY NODE NEED TO MAINTAIN ITS OWN STATS OBJECT
//OR ALL REFER TO A COMMON OBJECT THAT GETS UPDATED FOR EACH AS WE GO ALONG?
class QueryPlanner;
class OperationNode;
class JoinOperationNode;
class GroupByOperationNode;
class SingletonLeafNode;
class WriteOperationNode;
class DupRemovalOperationNode;
class SumOperationNode;
class ProjectOperationNode;
class SelectAfterJoinOperationNode;

class QueryPlanner
{

private:
  OperationNode *root;
  vector<OperationNode *> nodesVector;         //represents the pseudo tree
  vector<JoinOperationNode *> joinNodesVector; //represents the join pseudo tree
  char *outFilePath;
  std::ofstream outStream;
  Statistics *statistics;
  AndList *andList;
  static int pipeId;
  string outMode;
  FILE *outputFile;
  vector<JoinOperationNode *> joinVector;

public:
  QueryPlanner(Statistics *statistics, char *outFilePath, AndList *andList)
  {
    this->statistics = statistics;
    this->outFilePath = outFilePath;
    this->andList = andList;
    outStream.open(outFilePath);
  }

  QueryPlanner(Statistics *statistics)
  {
    this->statistics = statistics;
  }

  void planOperationOrder();

  //builders - build the tree in bottom up fashion
  void buildSelectFile();
  void planAndBuildJoins();
  void buildSum();
  void buildOnlySum();
  void buildProject();
  void buildDuplicate();
  void buildWrite();
  void buildSelectPipe();

  //exec the built up plan
  void executeQueryPlanner();

  //Helper methods
  void deepCopyAndList(AndList *&populateMe, AndList *copyMe);
  void print(std::ostream &os);
  void setOutputMode(char *out);
  int clear_pipe(Pipe &in_pipe, Schema *schema, bool print);
};

class OperationNode
{
  friend class QueryPlanner;
  friend class JoinOperationNode;
  friend class GroupByOperationNode;
  friend class SingletonLeafNode;
  friend class WriteOperationNode;
  friend class DupRemovalOperationNode;
  friend class SumOperationNode;
  friend class SelectFileOperationNode;
  friend class SelectAfterJoinOperationNode;
  friend class ProjectOperationNode;

protected:
  Schema *outSchema;
  string operationName;

  char *relationNames[MAX_NUM_RELS]; //populated differently for different things
  int numRelations;

  int outPipeId;
  static int pipeId;

  int estimatedTuples;
  int numTuples;
  OperationNode *child;
  // int optimalTuples;
  // Statistics *statistics;

  OperationNode *root;

public:
  //constructors
  OperationNode(string operationName);
  OperationNode(string operationName, Schema *outSchema);
  int getNumTuples() { return numTuples; }
  // OperationNode(string operationName, Schema *outSchema, string, Statistics *statistics);
  // OperationNode(string operationName, Schema *outSchema, vector<string>, Statistics *statistics);

  virtual void printNodeInfo(std::ostream &os, size_t level = 0) = 0;
  virtual void executeOperation(Pipe **outPipesList, RelationalOp **relopsList) = 0;
  std::string getOperationName();

  //virtual ~OperationNode();
};

class AndListBasedOperationNode : public OperationNode
{
protected:
  CNF cnf;
  Record literal;
  AndList *aList;
  Statistics *statistics;

public:
  AndListBasedOperationNode(string operationName, Statistics *statistics);
  AndList *buildSubAndList(AndList *&boolean);
  bool isValidOr(OrList *orList);
  virtual bool isValidComparisonOp(ComparisonOp *compOp) = 0;
  virtual void executeOperation(Pipe **outPipesList, RelationalOp **relopsList) = 0;
};

class JoinOperationNode : public AndListBasedOperationNode
{
  friend class QueryPlanner;

private:
  OperationNode *leftOperationNode;
  OperationNode *rightOperationNode;

public:
  JoinOperationNode(OperationNode *node1, OperationNode *node2, AndList *&aList, Statistics *statistics);
  void printNodeInfo(std::ostream &os, size_t level = 0);
  void combineRelNames();
  void populateJoinOutSchema();
  bool isValidComparisonOp(ComparisonOp *compOp);
  void executeOperation(Pipe **outPipesList, RelationalOp **relopsList);
  void estimateAndApply();
};

class SelectFileOperationNode : public AndListBasedOperationNode
{
private:
  char *aliasName;
  DBFile dbFile;

public:
  SelectFileOperationNode(Schema *outSchema, AndList *&aList, char *relationName, char *aliasName, Statistics *statistics);
  bool isValidComparisonOp(ComparisonOp *compOp);
  void printNodeInfo(std::ostream &os, size_t level = 0);
  void executeOperation(Pipe **outPipesList, RelationalOp **relopsList);
};

class SelectAfterJoinOperationNode : public AndListBasedOperationNode
{

private:
  char *aliasName;

public:
  SelectAfterJoinOperationNode(OperationNode *node, AndList *&aList, Statistics *statistics);
  void executeOperation(Pipe **outPipesList, RelationalOp **relopsList);
  bool isValidComparisonOp(ComparisonOp *compOp);
  void printNodeInfo(std::ostream &os, size_t level = 0);
};

class ProjectOperationNode : public OperationNode
{

  int keepMe[MAX_ATTS];
  int numInputAtts;
  int numProjectedAtts;

public:
  ProjectOperationNode(NameList *atts, OperationNode *node); //will update outschema from inside
  void printNodeInfo(std::ostream &os, size_t level = 0);
  void executeOperation(Pipe **outPipesList, RelationalOp **relopsList);
};

class DupRemovalOperationNode : public OperationNode
{
  OrderMaker dupRemovalOrder;

public:
  DupRemovalOperationNode(OperationNode *node);
  void printNodeInfo(std::ostream &os, size_t level = 0);
  void executeOperation(Pipe **outPipesList, RelationalOp **relopsList);
};

class SumOperationNode : public OperationNode
{
  Function function;

public:
  SumOperationNode(FuncOperator *parseTree, OperationNode *node);
  void printNodeInfo(std::ostream &os, size_t level = 0);
  Schema *buildOutSchema(FuncOperator *parseTree, OperationNode *node);
  void executeOperation(Pipe **outPipesList, RelationalOp **relopsList);
};

class GroupByOperationNode : public OperationNode
{
  OrderMaker groupOrder;
  Function function;

public:
  GroupByOperationNode(NameList *groupingAtts, FuncOperator *parseTree, OperationNode *node);
  void printNodeInfo(std::ostream &os, size_t level = 0);
  Schema *resultantSchema(NameList *groupingAtts, FuncOperator *parseTree, OperationNode *node);
  void executeOperation(Pipe **outPipesList, RelationalOp **relopsList);
};

class WriteOperationNode : public OperationNode
{
  friend class QueryPlanner;

public:
  WriteOperationNode(FILE *&outFile, OperationNode *node);
  void printNodeInfo(std::ostream &os, size_t level = 0);
  FILE *outputFile;
  void executeOperation(Pipe **outPipesList, RelationalOp **relopsList);
};

#endif