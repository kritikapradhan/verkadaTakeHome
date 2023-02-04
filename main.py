#!/usr/bin/env python

import json
import requests
from collections import OrderedDict
from itertools import islice

# Import any libraries you need

## Do not edit
class VerkadaDB():
    def __init__(self):
        self._data = {}
    ## You may add to the class definition below this line
    ## To-do: add class methods
    def initializeTable(self, tableKey):
        self._data[tableKey]={}

    def initializePrimaryKey(self, tableKey, primaryKey):
        self._data[tableKey][primaryKey]={}

    def update(self, tableKey, primaryKey, valuesToUpdate):
        ##assuming table exists
        for key in valuesToUpdate:
            self._data[tableKey][primaryKey][key] = valuesToUpdate[key]

    def __add__(self, tableKey, primaryKey, values):
        if primaryKey in self._data[tableKey] and self._data[tableKey][primaryKey]['company']==values['company']:
            return
        self.initializePrimaryKey(tableKey, primaryKey)
        for key in values:
            self._data[tableKey][primaryKey][key]=values[key]

    def __delete__(self, tableKey, primaryKey):
        if primaryKey in self._data[tableKey]:
            del self._data[tableKey][primaryKey]

    def __get__(self, tableKey):
        return self._data[tableKey]

    def getJSON(self,tableKey):
        return json.dumps(self._data[tableKey])

## Do not edit
dbInstance = VerkadaDB()


## To-do: Implement Function (mimics AWS Lambda handler)
## Input: JSON String which mimics AWS Lambda input
def lambda_handler(json_input):
    global dbInstance
    json_obj=json.loads(json_input)

    # find the name and company from input
    name=json_obj["email"].split('@')[0].strip()
    company=json_obj["email"].split('@')[1].split('.')[0]

    # not adding verkada emails to DB instance
    if company=='verkada':
        return {}

    topLevelName=json_obj["email"].split('@')[1].split('.')[1]

    # grabbing the nationality to for accuracy with gender and age
    nationalityUrl='https://api.nationalize.io?name='+name
    nationalityJson=json.loads(requests.get(nationalityUrl).text)
    nationality=getNationality(nationalityJson)

    genderURl='https://api.genderize.io?name='+name+'&country_id='+nationality
    gender=json.loads(requests.get(genderURl).text)['gender']

    ageUrl='https://api.agify.io?name='+name+'&country_id='+nationality
    age=json.loads(requests.get(ageUrl).text)['age']

    dbInstance.__add__('ByName' , name,{"company": company, "topLevelName": topLevelName, "age": age, "gender": gender,
                            "nationality": nationality})

    if gender == 'male' and age >= 30:
        dbInstance.__add__('CampaignCandidates',name,{"company": company, "topLevelName": topLevelName, "age": age, "gender": gender,
                            "nationality": nationality})
    json_output = json.dumps({"name": name, "company":company,"topLevelName":topLevelName,"age":age,"gender":gender,"nationality":nationality})
## Output: JSON String which mimics AWS Lambda Output
    return json_output

def getNationality(nationalityJson):
    maxProb=0
    nationality=''
    for elem in nationalityJson['country']:
        if elem['probability']>maxProb:
            maxProb=elem['probability']
            nationality=elem['country_id']
    return nationality

## To Do: Create a table to hold the information you process
dbInstance.initializeTable('ByName')
dbInstance.initializeTable('CampaignCandidates')


## Do not edit
lambda_handler(json.dumps({"email": "John@acompany.com"}))
lambda_handler(json.dumps({"email": "Kyle@ccompany.com"}))
lambda_handler(json.dumps({"email": "Georgie@dcompany.net"}))
lambda_handler(json.dumps({"email": "Karen@eschool.edu"}))
lambda_handler(json.dumps({"email": "Annie@usa.gov"}))
lambda_handler(json.dumps({"email": "Elvira@fcompay.org"}))
lambda_handler(json.dumps({"email": "Juan@gschool.edu"}))
lambda_handler(json.dumps({"email": " Julie@hcompany.com"}))
lambda_handler(json.dumps({"email": "Pierre@ischool.edu"}))
lambda_handler(json.dumps({"email": "Ellen@canada.gov"}))
lambda_handler(json.dumps({"email": "Willy@bcompany.co.uk"}))
lambda_handler(json.dumps({"email": "Craig@jcompany.org"}))
lambda_handler(json.dumps({"email": "Juan@kcompany.net"}))
lambda_handler(json.dumps({"email": "Jack@verkada.com"}))
lambda_handler(json.dumps({"email": "Jason@verkada.com"}))
lambda_handler(json.dumps({"email": "Billy@verkada.com"}))
lambda_handler(json.dumps({"email": "Brent@verkada.com"}))

## Put code for Part 2 here
##sample input= ['Craig']
def removeData(listOfNames):
    for name in listOfNames:
        dbInstance.__delete__('ByName', name)
        dbInstance.__delete__('CampaignCandidates', name)

# find people from input table name
def findCampaignPeople(table):
    campaignData=dbInstance.__get__(table)
    campaignData_asc=OrderedDict(sorted(campaignData.items(), key=lambda kv: kv[1]['age']))
    final4=dict(islice(campaignData_asc.items(), 4))
    return json.dumps(final4)

if __name__=="__main__":
    removeData(['Craig'])
    queryDataJson=findCampaignPeople('CampaignCandidates')
    databaseContent=dbInstance.getJSON('ByName')
    q1response="I would host a conversation with the business lead to understand what are the " \
               "key factors we noticed that need to be improved post-campaign. I would retrospect my prior " \
               "implementation and I would research into how I could modify my campaign list, keeping those key " \
               "factors in mind. Data that would help improve the effectiveness of my system would be the qualitative " \
               "analysis, from the people that were originally selected for the campaign. The qualitative analysis " \
               "will give me interpersonal and real time information, that APIs lack. I noticed that the data we " \
               "gathered to generate this list can be made more extensive, to increase our accuracy to target the " \
               "right candidates, such as occupation, income, current geographic location, psychographic and behavioral " \
               "responses. With information comes power, and this will help me build a more effective system."

    q2response="The market keeps changing, strategies on how to market a product need to also change hand in hand. " \
               "I would want to change my approach by first equipping myself with information on the product I am " \
               "marketing, in this case Verkada. Once I know about the product, I gain insight on the type of investors " \
               "that I should target to help generate more revenue fronts for the product. Secondly, I want to gain " \
               "insight into the major competitors of the product I am selling. This would help me target investors " \
               "smartly. Instead of concentrating on the largest purchasing demographic by age and gender, I would also " \
               "factor in occupation and geographic location. Occupation would give me more insight into who would be more" \
               " interested in security systems. By including geographic location, I can target locations that I think would " \
               "require more security coverage. "
    endpointUrl='https://rwph529xx9.execute-api.us-west-1.amazonaws.com/prod/pushToSlack'

    json_output=json.dumps({ "name": "Kritika Pradhan", "queryData": queryDataJson, "databaseContents": databaseContent,"FreeResponseQ1Response": q1response,"FreeResponseQ2Response": q2response})

    result = requests.post(endpointUrl, json_output)