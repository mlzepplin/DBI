#include "Statistics.h"
#include "cstring"
#include <sstream>
#include <iostream>
#include <fstream>

Statistics::Statistics()
{
    relationMap = new unordered_map<string, RelationInfo>();
}

Statistics::Statistics(Statistics &copyMe)
{ //copy constructor, creating a deep copy

    for (unordered_map<string, RelationInfo>::iterator relItr = copyMe.relationMap->begin(); relItr != copyMe.relationMap->end(); relItr++)
    {
        string relName = relItr->first;
        RelationInfo relationInfo;
        relationInfo.numTuples = relItr->second.numTuples;

        for (unordered_map<std::string, int>::iterator attItr = relItr->second.attributeMap.begin(); attItr != relItr->second.attributeMap.end(); attItr++)
        {
            string attName = attItr->first;
            int numDistinct = attItr->second;
            relationInfo.attributeMap.insert(pair<std::string, int>(attName, numDistinct));
        }

        relationMap->insert(pair<std::string, RelationInfo>(relName, relationInfo));
        relationInfo.attributeMap.clear();
    }
}

Statistics::~Statistics()
{
    delete relationMap;
    relationMap = NULL;
}

void Statistics::AddRel(char *relName, int numTuples)
{
    if (relationMap->find(relName) == relationMap->end())
    {
        //create empty attribute map
        unordered_map<string, int> tempAttributeMap;

        //insert into relation Map
        RelationInfo relationInfo = {numTuples, tempAttributeMap};
        relationMap->insert(std::make_pair(relName, relationInfo));

        //populating join list with singleton
        unordered_set<string> tempRel;
        tempRel.insert(relName);
    }
    else
    { //update numTuples
        relationMap->find(relName)->second.numTuples = numTuples;
    }
}

void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{
    //need to find the relation first
    if (relationMap->find(relName) == relationMap->end())
    {
        cerr << "the relation to which you're trying to add attribute, does not exist!!" << endl;
        exit(1);
    }
    else
    {
        RelationInfo *relationInfo = &relationMap->find(relName)->second;

        //If numDistincts is initially passed in as a –1, then the number of distincts is assumed to be equal to the number of tuples in the associated relation.
        if (numDistincts == -1)
            numDistincts = relationInfo->numTuples;

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
vector<string> Statistics::getAllAttsNames()
{
    vector<string> allAtts;
    RelMapIter relIter = relationMap->begin();
    AttMapIter attIter;
    while (relIter != relationMap->end())
    {
        unordered_map<string, int> attMap = relIter->second.attributeMap;
        attIter = attMap.begin();
        while (attIter != attMap.end())
        {
            allAtts.push_back(attIter->first);
            attIter++;
        }

        relIter++;
    }
    return allAtts;
}
void Statistics::CopyRel(char *oldName, char *newName)
{
    AttMapIter attMapIter;
    //just provides a copy of the relationInfo object
    RelationInfo relationInfo = relationMap->find(oldName)->second;
    unordered_map<string, int> attMap = relationInfo.attributeMap;

    if (relationMap->find(newName) == relationMap->end())
    {
        AddRel(newName, relationInfo.numTuples);
        for (attMapIter = attMap.begin(); attMapIter != attMap.end(); attMapIter++)
        {
            char newAttName[200];
            string newAtt = (string)newName + "." + attMapIter->first;
            strcpy(newAttName, newAtt.c_str());
            AddAtt(newName, newAttName, attMapIter->second);
        }
    }
    else
    {
        cerr << "relation wih newName already exists" << endl;
        exit(1);
    }
}

void Statistics::Read(char *fromWhere)
{
    relationMap->clear();
    FILE *statisticsInfo;
    statisticsInfo = fopen(fromWhere, "r");

    //If the file does not exist , create an empty file instead of throwing an error
    if (statisticsInfo == NULL)
    {
        statisticsInfo = fopen(fromWhere, "w");
        fprintf(statisticsInfo, "eof");

        statisticsInfo = fopen(fromWhere, "r");
    }
    fclose(statisticsInfo);

    ifstream in(fromWhere);

    char rel[200], att[200];

    //read Relation:
    string line;
    std::getline(in, line);

    while (line != "eof")
    {
        if (line == "Relation:")
        {
            //read relation name
            string relName;
            in >> relName;
            strcpy(rel, relName.c_str());

            int numTuples;
            in >> numTuples;

            AddRel(rel, numTuples);

            in >> line;

            //read all attributes of a relation
            while ((line != "Relation:") && (line != "eof"))
            {
                //read first attribute
                string attName;
                attName = line;

                //read numDistinct of an attribute
                int numDistinct;
                in >> numDistinct;

                strcpy(att, attName.c_str());
                AddAtt(rel, att, numDistinct);

                in >> line;
            }
        }
    }

    in.close();
}

void Statistics::Write(char *fromWhere)
{

    ofstream out;
    out.open(fromWhere);

    //Loop through the relation map
    for (unordered_map<std::string, RelationInfo>::iterator relItr = relationMap->begin(); relItr != relationMap->end(); relItr++)
    {
        char *relName = new char[relItr->first.length() + 1];
        strcpy(relName, relItr->first.c_str());

        out << "Relation:\n";
        out << relName << '\n';
        out << relItr->second.numTuples << '\n';

        //Loop through attribute map
        for (unordered_map<std::string, int>::iterator attItr = relItr->second.attributeMap.begin(); attItr != relItr->second.attributeMap.end(); attItr++)
        {
            char *attName = new char[attItr->first.length() + 1];
            strcpy(attName, attItr->first.c_str());

            out << attName << '\n';
            out << attItr->second << '\n';
        }
    }

    out << "eof";
    out.close();
}

unordered_set<string> Statistics::getRelNamesOfAttributes(struct AndList *parseTree, char *relNames[], int numToJoin)
{
    struct AndList *currentAnd = parseTree;
    struct OrList *currentOr;
    struct ComparisonOp *currentComparisonOp;
    struct Operand *leftOperand, *rightOperand;
    unordered_set<string> matchedRelNames;
    string attName;

    if (parseTree == NULL)
        return matchedRelNames; //return empty

    do
    {
        currentOr = currentAnd->left;
        do
        {
            currentComparisonOp = currentOr->left;
            leftOperand = currentComparisonOp->left;
            rightOperand = currentComparisonOp->right;
            if (leftOperand->code == NAME)
            {
                attName = leftOperand->value;

                matchedRelNames.insert(getRelationOfAtt(attName, relNames, numToJoin));

                if (rightOperand->code == NAME)
                {
                    attName = rightOperand->value;
                    matchedRelNames.insert(getRelationOfAtt(attName, relNames, numToJoin));
                }
            }
            currentOr = currentOr->rightOr;
        } while (currentOr != NULL);
        currentAnd = currentAnd->rightAnd;
    } while (currentAnd != NULL);
    return matchedRelNames;
}

string Statistics::getRelationOfAtt(string attName, char *relNames[], int numToJoin)
{
    unordered_map<string, RelationInfo>::iterator relMapIter;
    unordered_map<string, int>::iterator attMapIter;
    unordered_map<string, int> attMap;

    for (relMapIter = relationMap->begin(); relMapIter != relationMap->end(); relMapIter++)
    {
        attMap = relMapIter->second.attributeMap;
        attMapIter = attMap.find(attName);
        if (attMapIter == attMap.end())
            continue;
        else
        { //return matched rel name
            return relMapIter->first;
        }
    }
    cerr << "attribute: " << attName << "- cannot be found in relationMap" << endl;
    exit(1);
}

int Statistics::getNumTuples(string attName, char *relNames[], int numToJoin, int &numDistincts)
{
    //assumes that attribute does exist in atleast one relation
    unordered_map<string, RelationInfo>::iterator relMapIter;
    unordered_map<string, int>::iterator attMapIter;
    unordered_map<string, int> attMap;

    for (relMapIter = relationMap->begin(); relMapIter != relationMap->end(); relMapIter++)
    {
        attMap = relMapIter->second.attributeMap;
        attMapIter = attMap.find(attName);
        if (attMapIter == attMap.end())
            continue;
        else
            break;
    }

    numDistincts = attMapIter->second;
    return relMapIter->second.numTuples;
}

vector<string> Statistics::tokeniseKey(string input)
{
    vector<string> tokens;
    stringstream check1(input);
    string intermediate;
    while (getline(check1, intermediate, '|'))
    {
        tokens.push_back(intermediate);
    }
    return tokens;
}

unordered_set<string> Statistics::validateJoin(struct AndList *parseTree, char *relNames[], int numToJoin)
{

    unordered_set<string>::iterator relNamesSetIter;
    unordered_set<string> relNamesSet;
    unordered_map<string, RelationInfo>::iterator relMapIter;
    unordered_set<string> matchedRelNamesSet;
    struct AndList *currentAnd = parseTree;
    struct OrList *currentOr = currentAnd->left;

    matchedRelNamesSet = getRelNamesOfAttributes(parseTree, relNames, numToJoin);

    //making a set version of relNames, to reduce internal lookups to O(1)
    for (int i = 0; i < numToJoin; i++)
        relNamesSet.insert(relNames[i]);

    relNamesSetIter = relNamesSet.begin();

    while (relNamesSetIter != relNamesSet.end())
    {

        //get the set to which current relation from relNames belongs
        bool relationExistsInRelationMap = false;

        //Note: the list keeps on getting shorter as subsets that completely match --> get removed
        for (relMapIter = relationMap->begin(); relMapIter != relationMap->end(); ++relMapIter)
        {
            bool relationInCurrentMapElement = false;
            //if current relNames is a substring of current relMapIter's key
            if (relNamesSetIter == relNamesSet.end())
                return matchedRelNamesSet; //update to add iterator returns

            //get the key string of relMapIter
            string relMapKey = relMapIter->first;

            //get a delimited vector of strings, iterate and lookup
            vector<string> tokens = tokeniseKey(relMapKey);

            //lookup all substrings of this relationMap key in relNamesSet
            for (int i = 0; i < tokens.size(); i++)
            {
                if (tokens[i] == (*relNamesSetIter))
                {
                    relationExistsInRelationMap = true;
                    relationInCurrentMapElement = true;

                    break;
                }
            }
            if (relationInCurrentMapElement)
            {
                for (int i = 0; i < tokens.size(); i++)
                {
                    //Verify all of the tokens are present in the relNames
                    if (relNamesSet.find(tokens[i]) == relNamesSet.end())
                    {
                        cerr << "can't predict join, subset mismatch" << endl;
                        exit(1);
                    }

                    //remember to remove the found one's from the relNamesSet
                    relNamesSet.erase(tokens[i]);
                }
            }

            if (relNamesSet.empty())
                break;
        }
        if (!relationExistsInRelationMap)
        {
            cerr << "can't predict join relation " << *relNamesSetIter << " doesn't even exist!" << endl;
            exit(1);
        }
        relNamesSetIter = relNamesSet.begin();
    }
    return matchedRelNamesSet;
}

void Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
    unordered_set<string> matchedRelSet;
    unordered_set<string>::iterator matchedRelSetIter;
    unordered_map<string, int> attMap;
    unordered_map<string, int>::iterator attMapIter;
    matchedRelSet = validateJoin(parseTree, relNames, numToJoin);
    //char *newKey = new char[200];
    string newKey = "";

    //everything went fine, so we'll be able to predict the join output for relNames
    for (int i = 0; i < numToJoin; i++)
    {
        if (i > 0)
        {
            newKey += "|";
        }
        newKey += relNames[i];
    }

    //add the bigger set of relNames to the joinList
    //create an attmap of new join
    double estimate = Estimate(parseTree, relNames, numToJoin);

    char *newkey = new char[200];
    strcpy(newkey, newKey.c_str());
    AddRel(newkey, estimate);

    for (matchedRelSetIter = matchedRelSet.begin(); matchedRelSetIter != matchedRelSet.end(); matchedRelSetIter++)
    {
        //iterate over att map
        attMap = relationMap->find(*matchedRelSetIter)->second.attributeMap;
        for (attMapIter = attMap.begin(); attMapIter != attMap.end(); attMapIter++)
        { //insert all atts to the newkey relation
            relationMap->find(newKey)->second.attributeMap.insert(*attMapIter);
        }
        //delete the previous relations that got joined
        relationMap->erase(*matchedRelSetIter);
    }
}

double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{
    double estimatedTuples = 1.0;

    unordered_set<string>::iterator matchedRelSetIter;
    unordered_set<string> matchedRelSet;
    matchedRelSet = validateJoin(parseTree, relNames, numToJoin);
    if (parseTree == NULL)
    {
        if (numToJoin == 1)
        {
            //selection
            for (matchedRelSetIter = matchedRelSet.begin(); matchedRelSetIter != matchedRelSet.end(); matchedRelSetIter++)
            {
                vector<string> tokens = tokeniseKey((*matchedRelSetIter));

                for (int i = 0; i < tokens.size(); i++)
                {
                    if (tokens[i] == relNames[0])
                    {
                        return relationMap->find(*matchedRelSetIter)->second.numTuples;
                    }
                }
            }
        }
        else
        {
            //cross product bw all relNames
            for (int i = 0; i < numToJoin; i++)
            {
                matchedRelSetIter = matchedRelSet.begin();

                while (matchedRelSetIter != matchedRelSet.end())
                {
                    vector<string> tokens = tokeniseKey((*matchedRelSetIter));
                    bool matched = false;
                    for (int j = 0; j < tokens.size(); j++)
                    {
                        if (tokens[j] == relNames[i])
                        {

                            estimatedTuples *= relationMap->find(*matchedRelSetIter)->second.numTuples;
                            matchedRelSet.erase(*matchedRelSetIter);
                            matchedRelSetIter = matchedRelSet.begin();
                            matched = true;
                        }
                    }
                    if (!matched)
                        matchedRelSetIter++;
                }
                return estimatedTuples;
            }
        }
    }

    struct AndList *currentAnd = parseTree;
    struct OrList *currentOr;
    struct ComparisonOp *currentComparisonOp;
    struct Operand *leftOperand, *rightOperand;
    string attName;
    double maxJoinTuples = 1.0;

    //compute max possible join tuples
    for (int i = 0; i < numToJoin; i++)
    {
        matchedRelSetIter = matchedRelSet.begin();

        while (matchedRelSetIter != matchedRelSet.end())
        {
            vector<string> tokens = tokeniseKey((*matchedRelSetIter));
            bool matched = false;
            for (int j = 0; j < tokens.size(); j++)
            {
                if (tokens[j] == relNames[i])
                {
                    maxJoinTuples *= relationMap->find(*matchedRelSetIter)->second.numTuples;
                    matchedRelSet.erase(*matchedRelSetIter);
                    matchedRelSetIter = matchedRelSet.begin();
                    matched = true;
                }
            }
            if (!matched)
                matchedRelSetIter++;
        }
    }

    //reducing the maxTuples by factoring with all the attribute conditions
    double minAndTuplesEstimate = maxJoinTuples;
    do
    {
        currentOr = currentAnd->left;

        double maxOrTuplesEstimate = 0.0;
        do
        {
            currentComparisonOp = currentOr->left;
            leftOperand = currentComparisonOp->left;
            rightOperand = currentComparisonOp->right;
            int numRightDistincts = 0, numLeftDistincts = 0;
            int rightTuples = 0, leftTuples = 0;
            double currentOrTuplesEstimate = 0.0;

            leftTuples = getNumTuples(leftOperand->value, relNames, numToJoin, numLeftDistincts);

            if (currentComparisonOp->code == EQUALS)
            {
                if (rightOperand->code == NAME)
                {
                    double numDistinctOfSmallerRelation = 1.0;
                    rightTuples = getNumTuples(rightOperand->value, relNames, numToJoin, numRightDistincts);

                    if (leftTuples > rightTuples)
                        numDistinctOfSmallerRelation = numLeftDistincts;
                    else
                        numDistinctOfSmallerRelation = numRightDistincts;

                    currentOrTuplesEstimate = ((double)leftTuples * rightTuples) / numDistinctOfSmallerRelation;

                    maxOrTuplesEstimate = std::max(maxOrTuplesEstimate, currentOrTuplesEstimate);
                }
                else
                {
                    maxOrTuplesEstimate = std::max(maxOrTuplesEstimate, leftTuples / (double)numLeftDistincts);
                }
            }
            else
            { //less than or greater than condition
                maxOrTuplesEstimate = std::max(maxOrTuplesEstimate, (1.0 / 3) * leftTuples);
            }
            currentOr = currentOr->rightOr;
        } while (currentOr != NULL);

        minAndTuplesEstimate = std::min(minAndTuplesEstimate, maxOrTuplesEstimate);
        currentAnd = currentAnd->rightAnd;
    } while (currentAnd != NULL);

    estimatedTuples = minAndTuplesEstimate;
    return estimatedTuples;
}
