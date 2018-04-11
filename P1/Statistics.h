#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <list>
#include <string>
using namespace std;

struct RelationInfo
{
	int numTuples;
	std::unordered_map<std::string, int> attributeMap;
} typedef RelationInfo;

typedef unordered_map<string, RelationInfo>::iterator RelMapIter;
class Statistics
{
  private:
	std::unordered_map<string, RelationInfo> *relationMap;

  public:
	Statistics();
	Statistics(Statistics &copyMe); // Performs deep copy
	~Statistics();

	void AddRel(char *relName, int numTuples);
	void AddAtt(char *relName, char *attName, int numDistincts);
	void CopyRel(char *oldName, char *newName);

	void Read(char *fromWhere);
	void Write(char *fromWhere);

	void Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
	double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);

	unordered_set<string> getRelNamesOfAttributes(struct AndList *parseTree, char *relNames[], int numToJoin);
	string getRelationOfAtt(string attName, char *relNames[], int numToJoin);
	vector<string> tokeniseKey(string input);

	//checks if the join set-subset conditions match, if all the attributes present in parseTree
	//are also present in some relation from relNames
	unordered_set<string> validateJoin(struct AndList *parseTree, char *relNames[], int numToJoin);

	//populates the numDistincts with the number of unique entries the attribute has in its
	//respective relation, and returns the number of tuples of the containing relation
	//NOTE:assumes that the attribute exists in at least one relation from relNames[]
	int getNumTuples(string attName, char *relNames[], int numToJoin, int &numDistincts);
};

#endif
