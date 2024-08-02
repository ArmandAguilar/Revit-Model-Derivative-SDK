#!/usr/bin python
# -*- coding: utf-8 -*-
import logging
import re
import sys
import json
import requests
import base64


class BucketsObjects:

    def uploadSmallModel(api_url,
                         file_name='home.rtv',
                         path_rtv='c:\\demo\home.rtv',
                         objectId='',
                         token=''):
        """
        This method make the upload of rtv file to bucket in autodesk cloud S3
        
        @param:api_url
        @type:string

        @param:file_name
        @type:string

        @param:path_rt
        @type:string

        @param:objectId
        @type:string

        @param:objectId
        @type:token
        
        Return [status:200,500]
        """
        location = '' 
        data = [] 
        # 1 .- READ THE FILE RTV
        file_object = open(str(path_rtv), "rb")

        # 2 .- UPLOAD MODEL AUTODESK

        # 2.1 .- GET THE SPACE IN SS3
        logging.info('GET THE SPACE IN SS3')
        end_point = f'{api_url}/oss/v2/buckets/{objectId}/objects/{file_name}/signeds3upload?parts=1&minutesExpiration=30'

        headers = {'Content-Type': 'application/octet-stream','Authorization': 'Bearer ' + str(token)}

        req_forge_parts = requests.get(end_point, headers=headers)
        if int(req_forge_parts.status_code) == 200:
            space_bucket = json.loads(req_forge_parts.text)
        
        # 2.2 .- MAKE THE UPLOAD         
            upload_file = requests.put(space_bucket['urls'][0],data=file_object) 

        # 2.3 .- END THE FILE
        end_point = f'{api_url}/oss/v2/buckets/{objectId}/objects/batchcompleteupload'
        headers = {'Content-Type': 'application/json','Authorization': 'Bearer ' + str(token)}

        payload = { "requests":[{"objectKey":str(file_name),"uploadKey":space_bucket['uploadKey']}]}

        req_forge_save = requests.post(end_point,data=json.dumps(payload) ,headers=headers)

        # 3.- SEND JOB TO FORGE CLOUD
        if int(req_forge_save.status_code) == 200:
            forge_file = json.loads(req_forge_save.text)
            content = eval(req_forge_save.content)
            urn = base64.b64encode(content['results'][file_name]['objectId'].encode('utf-8'))
            urn = urn.decode('utf-8')
            objectKey = file_name
            objectId = forge_file['results'][file_name]['objectId']
            location = forge_file['results'][file_name]['location']

            data_urn = {
                    "input": {"urn": urn,
                              "rootFilename": str(file_name)},
                    "output": {
                        "destination": {"region": "us"},
                        "formats": [{"type": "svf2", "views": ["2d", "3d"]}]
                    }
                }
            
            headers = {'Content-Type': 'application/json','Authorization': 'Bearer ' + str(token)}
            req_job_autodesk = requests.post(str(api_url) + '/modelderivative/v2/designdata/job', data=json.dumps(data_urn), headers=headers)

            if req_job_autodesk.status_code == 200:
                data = json.loads(req_job_autodesk.text)
        return data
    
    def bucketsObjectsStatus(api_url, token, bucketKey, objectKey, sessionId, region='US'):
        """
        This endpoint returns status information about a resumable upload
        Required OAuth Scopes data:read

        @param:api_url
        @type:string

        @param:token
        @type:string

        @param:bucketKey
        @type:string

        @param:objectKey
        @type:string

        @param:sessionId
        @type:string

        @param:region
        @type:string

        @Return []

        """
        data = []
        try:
            headers = {'content-type': 'application/json',
                       'Authorization': 'Bearer ' + str(token), 'x-ads-region': str(region)}

            url = f"{api_url}/oss/v2/buckets/{bucketKey}/objects/{objectKey}/status/{sessionId}"
            resp_bucket = requests.get(url, headers=headers)
            if resp_bucket.status_code == 200:
                data = json.load(resp_bucket.text)

        except Exception as error:
            logging.error(error)
            data = json.load(resp_bucket.text)

        return data

    def bucketKeyObjects(api_url, token, bucketKey, region='US', limit=0, startAt=''):
        """
        List objects in a bucket. It is only available to the bucket creator.
        Required OAuth Scopes data:read

        @param:api_url
        @type:string

        @param:token
        @type:string

        @param:bucketKey
        @type:string

        @param:region
        @type:string

        @param:limit
        @type:int
        The number of objects to return in the result set.
        Acceptable values = 1 - 100. Default = 10.

        @param:beginsWith
        @type:string
        String to filter the result set by. The result set is restricted to
        items whose objectKey begins with the provided string.

        @param:startAt
        @type:int
        The position to start listing the result set. This parameter is used to request the next set of items,
        when the response is paginated. The next attribute of the response provides the URI for the next result set,
        complete with the startAt parameter pre-populated.

        @return  {
                "items" : [
                    {
                    "bucketKey" : "apptestbucket",
                    "objectKey" : "objectKeyFoo",
                    "objectId" : "urn:adsk.objects:os.object:apptestbucket/objectKeyFoo",
                    "sha1" : "cdbf71bfc07cbc18372a5dd4b6e161463cb7fd35",
                    "size" : 7,
                    "location" : "https://developer.api.autodesk.com/oss/v2/buckets/apptestbucket/objects/objectKeyFoo"
                    }
                ],
                "next" : "https://developer.api.autodesk.com/oss/v2/buckets/apptestbucket/objects?startAt=objectKeyFoo&limit=1"
                }

        """
        data = []
        data_loop = []
        params = ''
        startAt = ''
        loop_buckets = True
        try:
            headers = {'content-type': 'application/json','Authorization': 'Bearer ' + str(token), 'x-ads-region': str(region)}

            if region != 'Default':
                params += f'region={region}&'
            if limit > 0:
                params += f'limit={limit}&'
            
            #Clean url param
            params = params[:-1]
            url_loop = f"{api_url}oss/v2/buckets/{bucketKey}/objects?{params}{startAt}"
            while loop_buckets == True:
                resp_bucket = requests.get(url_loop, headers=headers)
                if int(resp_bucket.status_code) == 200:
                    data_loop = json.loads(resp_bucket.text)
                    for item in data_loop["items"]:
                        data.append(item)
                    
                    if "next" in json.dumps(data_loop):
                        url_loop = data_loop["next"]
                    else:
                        loop_buckets = False

        except Exception as error:
            logging.error(error)
        return data
