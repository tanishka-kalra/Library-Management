from fastapi import FastAPI
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from urllib.parse import quote_plus
from bson import ObjectId
import json

app=FastAPI()
ds={}

#Function to create a connection to mongoDB
def details(collectionName):
    username = data['user']
    password = data['pass']
    clusterId= data['clusterId']
    encoded_username = quote_plus(username)
    encoded_password = quote_plus(password)
    uri=f"mongodb+srv://{encoded_username}:{encoded_password}@{clusterId}.mongodb.net/"
    client = MongoClient(uri, server_api=ServerApi('1'))
    db = client['student']
    collection = db[collectionName]
    return collection

data=None
count=99
def loadData():
    global data
    global count

    #Loading credentials from a config file
    with open('config.json','r')as file:
        data=json.load(file)
    collection=details("rollInfo")
    cursor = collection.find({})

    #Logic to generate next roll number
    #We are assuming that roll number starts from 100
    mx=99
    for doc in cursor:
        ds[doc['roll']]=doc['obj']
        if mx<doc['roll']:mx=doc['roll']
    count=mx+1


loadData()   #Populating the roll number data structure


#Utility function to add student to DB
def addStudent(student):
    collection=details("studentInfo")
    result = collection.insert_one(student)
    return str(result.inserted_id)


#Add student endpoint
@app.post("/students",status_code=201)
def create_student(student: dict):
    global count
    if student.get("name")==None:
        return {'Message':'Name Required'}
    if student.get("age")==None:
        return {'Message':'Age Required'}
    if student.get("address")==None:
        return {'Message':'Address Required'}
    addr=student['address']
    if addr.get("city")==None:
        return {'Message':'City Required in Address'}
    if addr.get("country")==None:
        return {'Message':'Country Required in Address'}

    result = addStudent(student)
    ds[count]=result
    roll_no=count
    rollCollection=details("rollInfo")
    rollCollection.insert_one({'roll':roll_no,'obj':result})
    count=count+1
    return {'id' : roll_no}


#Get student with filters endpoint
@app.get("/students",status_code=200)
def get_data(id: int = 0,country: str="",age: int=-1):
    collection=details("studentInfo")
    if id==0:
        if len(country)==0 and age==-1:
            cursor = collection.find({})
            all_data=[]
            for doc in cursor:
                all_data.append({"name":doc['name'],"age":doc["age"]})
            return {'data':all_data}
        if len(country)>0 and age==-1:  #Filter for only country
            cursor = collection.find({"address.country":country})
            all_data=[{'name':doc['name'],'age':doc['age']} for doc in cursor]
            return {'data':all_data}
        if len(country)==0 and age!=-1:  #Filter for only age
            cursor = collection.find({"age":{"$gte":age}})
            all_data=[{'name':doc['name'],'age':doc['age']} for doc in cursor]
            return {'data':all_data}
        if len(country)>0 and age!=-1:  #Filter for both country and age
            cursor = collection.find({"age":{"$gte":age},"address.country":country})
            all_data=[{'name':doc['name'],'age':doc['age']} for doc in cursor]
            return {'data':all_data}
    else:
        if ds.get(id)==None: return {'Message':"No data found"}
        cursor = collection.find({"_id":ObjectId(ds[id])})
        for doc in cursor:
            return {'name':doc['name'],'age':doc['age'],'address':doc['address']}

#Delete student on the basis of roll number endpoint
@app.delete("/students/{student_id}", status_code = 200)
def delete_data(student_id : int):
    collection=details("studentInfo")
    if ds.get(student_id)==None:
        return {}
    roll_no = ds[student_id]
    collection.delete_one({"_id":ObjectId(roll_no)})
    collection=details("rollInfo")
    collection.delete_one({"roll":student_id})
    return {}


#Update student endpoint
@app.patch("/students/{student_id}",status_code=204)
def update_data(updated_data : dict):
    collection=details("studentInfo")
    if updated_data.get("id")==None:
        return {'Message':'Id is required'}
    student_id=int(updated_data['id'])
    if ds.get(student_id)==None:
        return {"Message":"Invalid ID"}
    dataToUpdate={}
    if updated_data.get("name")!=None:
        dataToUpdate['name']=updated_data['name']
    if updated_data.get("age")!=None:
        dataToUpdate['age']=updated_data['age']
    if updated_data.get("address")!=None:
        dataToUpdate['address']=updated_data['address']
    obj_id=ds[student_id]
    collection.update_one({"_id": ObjectId(obj_id)}, {"$set": dataToUpdate})
