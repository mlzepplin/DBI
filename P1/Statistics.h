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

class Statistics
{
  private:
	std::unordered_map<string, RelationInfo> relationMap;
	std::list<unordered_set<string>> joinList;

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

	bool checkAttributes(struct AndList *parseTree, char *relNames[], int numToJoin);
	bool findAttInRelation(string attName, char *relNames[], int numToJoin);
	double fractionise(int numTuples, int numDistincts);

	//checks if the join set-subset conditions match, if all the attributes present in parseTree
	//are also present in some relation from relNames, and updates the joinList if isApply is true
	void validateJoin(struct AndList *parseTree, char *relNames[], int numToJoin, bool isApply);

	//populates the numDistincts with the number of unique entries the attribute has in its
	//respective relation, and returns the number of tuples of the containing relation
	//NOTE:assumes that the attribute exists in at least one relation from relNames[]
	int getNumTuples(string attName, char *relNames[], int numToJoin, int &numDistincts);
};

#endif
