#include "Statistics.h"
#include "unordered_map"

Statistics::Statistics()
{
}
Statistics::Statistics(Statistics &copyMe)
{
}
Statistics::~Statistics()
{
}

void Statistics::AddRel(char *relName, int numTuples)
{
    if (relationMap.find(relName) == relationMap.end())
    {
        //create empty attribute map
        unordered_map<string, int> tempAttributeMap;

        //insert into relation Map
        RelationInfo relationInfo = {numTuples, tempAttributeMap};
        relationMap.insert(std::make_pair(relName, relationInfo));
    }
    else
    { //update numTuples
        relationMap.find(relName)->second.numTuples = numTuples;
    }
}
void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{
    //need to find the relation first
    if (relationMap.find(relName) == relationMap.end())
    {
        cerr << "the relation to which you're trying to add attribute, does not exist!!" << endl;
        exit(1);
    }
    else
    {
        RelationInfo *relationInfo = &relationMap.find(relName)->second;
        //check if attribute already exists
        if (relationInfo->attributeMap.find(attName) == relationInfo->attributeMap.end())
        { //if not, then add the attribute
            relationInfo->attributeMap.insert(std::make_pair(attName, numDistincts));
        }
        else
        { //update numDistincts
            relationInfo->attributeMap.find(attName)->second = numDistincts;
        }
    }
}
void Statistics::CopyRel(char *oldName, char *newName)
{
    //just provides a copy of the relationInfo object
    RelationInfo relationInfo = relationMap.find(oldName)->second;

    if (relationMap.find(newName) != relationMap.end())
        relationMap.insert(std::make_pair(newName, relationInfo));
    else
    {
        cerr << "relation wih newName already exists" << endl;
        exit(1);
    }
}

void Statistics::Read(char *fromWhere)
{
}
void Statistics::Write(char *fromWhere)
{
}

void Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
}
double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{
}
