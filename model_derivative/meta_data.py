#!/usr/bin python
# -*- coding: utf-8 -*-
import logging
import re
import sys
import json
import requests
import sqlite3
from pymongo import MongoClient
from bson.objectid import ObjectId

class ManageMetaData:

    def __init__(self):
        pass

    def __insert_nodeSqLite(self,node, parent_id=None, level=0,cursor_db=None):
        if level == 0:
            cursor_db.execute('INSERT INTO family (name, objectid) VALUES (?, ?)', (node['name'], node['objectid']))
            node_id = cursor_db.lastrowid
            for child in node.get('objects', []):
                self.__insert_nodeSqLite(node=child, parent_id=node_id, level=level + 1,cursor_db=cursor_db)
        elif level == 1:
            cursor_db.execute('INSERT INTO category (family_id, name, objectid) VALUES (?, ?, ?)', (parent_id, node['name'], node['objectid']))
            node_id = cursor_db.lastrowid
            for child in node.get('objects', []):
                self.__insert_nodeSqLite(child, node_id, level + 1,cursor_db=cursor_db)
        elif level == 2:
            cursor_db.execute('INSERT INTO types (category_id, name, objectid) VALUES (?, ?, ?)', (parent_id, node['name'], node['objectid']))
            node_id = cursor_db.lastrowid
            for child in node.get('objects', []):
                self.__insert_nodeSqLite(child, node_id, level + 1,cursor_db=cursor_db)
        elif level == 3:
            cursor_db.execute('INSERT INTO elements (types_id, name, objectid) VALUES (?, ?, ?)', (parent_id, node['name'], node['objectid']))
            node_id = cursor_db.lastrowid

    def __getTreeSqlLite(self,level=0, parent_id=None,cursor=None):                  
        tree = []
        if level == 0:
            cursor.execute('SELECT id, name, objectid FROM family')
            rows = cursor.fetchall()
            for row in rows:
                children = self.__getTreeSqlLite(level + 1, row[0],cursor=cursor)
                tree.append({'name': row[1], 'objectid': row[2], 'children': children})
        elif level == 1:
            cursor.execute('SELECT id, name, objectid FROM category WHERE family_id = ?', (parent_id,))
            rows = cursor.fetchall()
            for row in rows:
                children = self.__getTreeSqlLite(level + 1, row[0],cursor=cursor)
                tree.append({'name': row[1], 'objectid': row[2], 'children': children})
        elif level == 2:
            cursor.execute('SELECT id, name, objectid FROM types WHERE category_id = ?', (parent_id,))
            rows = cursor.fetchall()
            for row in rows:
                children = self.__getTreeSqlLite(level + 1, row[0],cursor=cursor)
                tree.append({'name': row[1], 'objectid': row[2], 'children': children})
        elif level == 3:
            cursor.execute('SELECT id, name, objectid FROM elements WHERE types_id = ?', (parent_id,))
            rows = cursor.fetchall()
            for row in rows:
                tree.append({'name': row[1], 'objectid': row[2], 'children': []})
        return tree


    def translateToSVF2(self,api_url='https://developer.api.autodesk.com/',
                            token='', urn='', region='us'):
        """
        For the server to extract metadata from a model, you must first translate the model 
        to a viewer-friendly format. While this tutorial translates the model to SVF2, you 
        can just as well translate it to SVF. The procedure to extract metadata is identical 
        regardless of what format you translate to.

        @params:api_url
        @type:string

        @params:token
        @type:string

        @params:urn
        @type:string

        @params:region
        @type:string
        
        Return data[]
        """
        data = []
        try:
            headers = {'content-type': 'application/json',
                       'Authorization': 'Bearer ' + str(token), 'x-ads-region': 'US'}
            payload = {
                "input": {"urn": urn},
                "output": {
                            "destination": {"region": region},
                            "formats": [
                                            {
                                                "type": "svf2",
                                                "views": ["2d", "3d"]
                                            }
                                        ]
                            }
                }
            resp_job = requests.post(str(api_url) + '/modelderivative/v2/designdata/job', headers=headers, data=json.dumps(payload))
            if resp_job.status_code == 201:
                data = json.loads(resp_job.text)
            else:
                logging.error(resp_job.text)
        except Exception as error:
            logging.error(error)
        return data

    def checkStatusJob(self,api_url='https://developer.api.autodesk.com/',token='', urn=''):
        """
        There are two ways to check the status of a translation job. The first is to set up a webhook that notifies you when the jib is done. The second is to poll the status of the job periodically. For this tutorial, you will be polling the status of the translation job. This means that you must periodically inspect the manifest produced by the translation job. The status attribute in the manifest reports the status of the translation job. The status can be:
        
        @Return: status
            pending: The job has been received and is pending for processing.
            inprogress: The job has started processing, and is running.
            success: The job has finished successfully.
            failed: The translation has failed.
            timeout: The translation has timed out and no output is generated.
        @type:[]

        @params:api_url
        @type:string

        @params:token
        @type:string

        @params:urn
        @type:string

        Return data = []
        """
        data = []
        try:
            headers = {'content-type': 'application/json','Authorization': 'Bearer ' + str(token), 'x-ads-region': 'US'}

            resp_status = requests.get(str(api_url) + f"/modelderivative/v2/designdata/{urn}/manifest", headers=headers)
            
            if int(resp_status.status_code) == 200:
                data = json.loads(resp_status.text)
            else:
                data = []
        except Exception as error:
            logging.error(error)
        return data

    def metaData(self,api_url='https://developer.api.autodesk.com/',token='', urn='',guid=''):
        """
        Returns a list of model views (Viewables) in the source design specified by the urn URI parameter. 
        It also returns the ID that uniquely identifies the model view. You can use this ID with other metadata endpoints
        to obtain information about the objects within model view.

        Most design applications like Fusion 360 and Inventor contain only one model view per design. However,
        some applications like Revit allow multiple model views (e.g., HVAC, architecture, perspective) per design.

        Note You can retrieve metadata only from an input file that has been translated to SVF or SVF2.

        @params:api_url
        @type:string

        @params:token
        @type:string

        @params:urn
        @type:string

        @params:guid
        @type:string

        @Return: status
        @type:[]
        """
        status = []
        try:
            headers = {'content-type': 'application/json',
                       'Authorization': 'Bearer ' + str(token), 'x-ads-region': 'US'}
            if guid != '':
                path_url = f"/modelderivative/v2/designdata/{urn}/metadata/{guid}/properties"
            else:
                path_url = f"/modelderivative/v2/designdata/{urn}/metadata"
            
            resp_status = requests.get(str(api_url) + path_url, headers=headers)
            if resp_status.status_code == 200:
                status = json.loads(resp_status.text)
            else:
                status = []
                logging.error(resp_status)
        except Exception as error:
            logging.error(error)
        return status

    def getAllPropertiesQuery(self,api_url='https://developer.api.autodesk.com/',token='', urn='',guid='',query=None):
        """
        Returns a list of model views (Viewables) in the source design specified by the urn URI parameter. 
        It also returns the ID that uniquely identifies the model view. You can use this ID with other metadata endpoints
        to obtain information about the objects within model view.

        Most design applications like Fusion 360 and Inventor contain only one model view per design. However,
        some applications like Revit allow multiple model views (e.g., HVAC, architecture, perspective) per design.

        Note You can retrieve metadata only from an input file that has been translated to SVF or SVF2.

        @params:api_url
        @type:string

        @params:token
        @type:string

        @params:urn
        @type:string

        @params:guid
        @type:string

        @params:query
        @type:{}

        @Return: status
        @type:[]
        """
        query_set = []
        try:
            if query != None:
                headers = {'content-type': 'application/json','Authorization': 'Bearer ' + str(token), 'x-ads-region': 'US'}
                path_url = f"{api_url}modelderivative/v2/designdata/{urn}/metadata/{guid}/properties:query"
                resp_status = requests.post(path_url,data=json.dumps(query), headers=headers)
                #print(resp_status.text)
                if resp_status.status_code == 200:
                    query_set = json.loads(resp_status.text)
                else:
                    query_set = []
                    logging.error(resp_status)
            else:
                query_set.append({'status':'error','message':'var query could be empty'})
        except Exception as error:
            logging.error(error)
        return query_set

    def createObjectTreeSqlLite(self,api_url='https://developer.api.autodesk.com/',token='', urn='',guid='',sqllite_db_name='',sqllite_path=''):
        """
        @params:api_url
        @type:string

        @params:token
        @type:string

        @params:urn
        @type:string

        @params:guid
        @type:string

        @Return: {}
        @type:[]
        """
        data_tree = []
        status = {'status':500,'message':'error to load data'}
        try:
            headers = {'content-type': 'application/json','Authorization': 'Bearer ' + str(token), 'x-ads-region': 'US'}
            path_url = f"{api_url}/modelderivative/v2/designdata/{urn}/metadata/{guid}"
            resp_status = requests.get(path_url,headers=headers)
            if resp_status.status_code == 200:
                data_tree = json.loads(resp_status.text)
                if sqllite_db_name != '' and sqllite_path != '':
                    #1 .- CHECK DB    
                    conn = sqlite3.connect(f"{sqllite_path}/{sqllite_db_name}")
                    cursor = conn.cursor()

                   #2 .- CREATE TABLES
                    cursor.execute('''
                        CREATE TABLE IF NOT EXISTS family (
                            id INTEGER PRIMARY KEY,
                            name TEXT,
                            objectid INTEGER
                        )
                    ''')
                    cursor.execute('''
                        CREATE TABLE IF NOT EXISTS category (
                            id INTEGER PRIMARY KEY,
                            family_id INTEGER,
                            name TEXT,
                            objectid INTEGER,
                            FOREIGN KEY (family_id) REFERENCES family (id)
                        )
                    ''')
                    cursor.execute('''
                        CREATE TABLE IF NOT EXISTS types (
                            id INTEGER PRIMARY KEY,
                            category_id INTEGER,
                            name TEXT,
                            objectid INTEGER,
                            FOREIGN KEY (category_id) REFERENCES category (id)
                        )
                    ''')
                    cursor.execute('''
                        CREATE TABLE IF NOT EXISTS elements (
                            id INTEGER PRIMARY KEY,
                            types_id INTEGER,
                            name TEXT,
                            objectid INTEGER,
                            FOREIGN KEY (types_id) REFERENCES types (id)
                        )
                    ''')

                    #3 .- DATA TREE
                    for item in data_tree['data']['objects'][0]['objects']:
                        self.__insert_nodeSqLite(node=item ,parent_id=None, level=0,cursor_db=cursor)

                    conn.commit()
                    status = {'status':201,'message':'data loaded'}
                conn.close()
            else:
                status = []
                logging.error(resp_status)
        except Exception as error:
            logging.error(error)
        return status
    
    def getObjectTreeSqlLite(self,sqllite_db_name='',sqllite_path=''):
        """
        @params:sqllite_db_name
        @type:string

        @params:sqllite_path
        @type:string

        @Return: []
        @type:[]
        """
        data_tree = []
        try:
            if sqllite_db_name != '' and sqllite_path != '':
                #1 .- CHECK DB    
                conn = sqlite3.connect(f"{sqllite_path}/{sqllite_db_name}")
                cursor = conn.cursor()
                #2 .- CREATE TREE JSON
                data_tree = self.__getTreeSqlLite(level=0, parent_id=None,cursor=cursor)
                conn.close()
        except Exception as error:
            logging.error(error)
        return data_tree
    

    def saveElementesPropertiesSqlLite(self,api_url='https://developer.api.autodesk.com/',token='', urn='',guid='',objects_ids='',sqllite_db_name='',sqllite_path=''):
        """
        Returns a list of model views (Viewables) in the source design specified by the urn URI parameter. 
        It also returns the ID that uniquely identifies the model view. You can use this ID with other metadata endpoints
        to obtain information about the objects within model view.

        Most design applications like Fusion 360 and Inventor contain only one model view per design. However,
        some applications like Revit allow multiple model views (e.g., HVAC, architecture, perspective) per design.

        Note You can retrieve metadata only from an input file that has been translated to SVF or SVF2.

        @params:api_url
        @type:string

        @params:token
        @type:string

        @params:urn
        @type:string

        @params:guid
        @type:string

        @params:objects_ids
        @type:str

        @Return: status
        @type:[]
        """
        status = {'status':500,'message':'error to load data'}
        try:
            if len(objects_ids) > 0:
                if sqllite_db_name !='' and sqllite_path !='':
                    #1 GET DATA 
                    headers = {'content-type': 'application/json','Authorization': 'Bearer ' + str(token), 'x-ads-region': 'US'}
                    path_url = f"{api_url}modelderivative/v2/designdata/{urn}/metadata/{guid}/properties:query"
                    payload = {
                                    "query": {
                                        "$in": [ "objectid",]
                                    },
                                    "fields": ["objectid","name","externalId","properties"],
                                    "pagination": {"offset": 0,"limit": 100},
                                    "payload": "text"}
                    
                    for ids in objects_ids:
                        payload["query"]["$in"].append(ids)

                    resp_status = requests.post(path_url,data=json.dumps(payload), headers=headers)

                    #2 SAVE DB
                    conn = sqlite3.connect(f"{sqllite_path}/{sqllite_db_name}")
                    cursor = conn.cursor()

                    #2 .- CREATE TABLES
                    cursor.execute('''
                        CREATE TABLE IF NOT EXISTS properties (
                            id INTEGER PRIMARY KEY,
                            objectid INTEGER,
                            name TEXT,
                            properties_data TEXT
                        )
                    ''')
                    conn.commit()

                    #3 .- READ THE DATA
                    if resp_status.status_code == 200:
                        query_set = json.loads(resp_status.text)
                        
                        if len(query_set) > 0:
                            for items in query_set["data"]["collection"]:
                                print(items['name'])
                                cursor.execute('INSERT INTO properties (objectid,name,properties_data)  VALUES (?,?,?)', (items['objectid'],items['name'],json.dumps(items['properties'])))
                                conn.commit()
                    else:
                        logging.error(resp_status)
                        status = {'status':'ok','message':resp_status}
                    
                    conn.close()
                    status = {'status':'ok','message':f"data saved in {sqllite_db_name}"}
            else:
                status.append({'status':'error','message':'var query could be empty'})
        except Exception as error:
            logging.error(error)
        return status
 
