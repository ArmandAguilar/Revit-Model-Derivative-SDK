#!/usr/bin python
# -*- coding: utf-8 -*-
import logging
import sys
import json
import requests


class BucketsOss:
    def create_bucket(url_api, token, bucket_name, policyKey, access):
        """
        Creates a bucket. Buckets are arbitrary spaces that are created by applications and are used
        to store objects for later retrieval. A bucket is owned by the application that creates it.

        @param:url_api
        @type:string

        @param:token
        @type:string

        @param:policyKey {transient, temporary, persistent}
        @type:string

        @param:access {full, read}
        @type:string

        @param:bucket_name
        @type:string

        @Retrun {
                "bucketKey":"apptestbucket",
                "bucketOwner":"RlKfGlAbb7N8VJwLllOvpfonB1Ex52qG",
                "createdDate":1463785698600,
                "permissions":[
                    {
                    "authId":"RlKffonB1Ex52GlAbb7N8VJwLllOvpqG",
                    "access":"full"
                    }
                ],
                "policyKey":"transient"
                }
        """
        data = []
        try:
            headers = {'content-type': 'application/json',
                       'Authorization': 'Bearer ' + str(token), 'x-ads-region': 'US'}
            payload = {
                "bucketKey": str(bucket_name).lower(),
                "access": str(access),
                "policyKey": str(policyKey)
            }

            resp_bucket = requests.post(str(url_api) + '/oss/v2/buckets', headers=headers, data=json.dumps(payload))

            if resp_bucket.status_code == 200:
                data = json.loads(resp_bucket.text)
        except Exception as error:
            logging.error(error)
        return data

    def get_bukets(api_url, token, region='Default', limit=0, startAt=0):
        """
        This endpoint will return the buckets owned by the application. This endpoint supports pagination

        @param:api_url
        @type:string

        @param:region
        @type: string
        Acceptable values: US, EMEA Default: US

        @param:limit
        @type:int
        Limit to the response size, Acceptable values: 1-100 Default = 10

        @Return "items" : [ {
                            "bucketKey" : "00001fbf-8505-49ab-8a42-44c6a96adbd0",
                            "createdDate" : 1441329298362,
                            "policyKey" : "transient"
                        }
        """

        data = []
        data_loop = []
        params = ''
        startAt = ''
        try:
            loop_buckets = True
            headers = {'content-type': 'application/json',
                       'Authorization': 'Bearer ' + str(token), 'x-ads-region': str(region)}

            if region != 'Default':
                params += f'region={region}&'
            if limit > 0:
                params += f'limit={limit}&'

            #Clean url param
            params = params[:-1]
            url_loop = f"{api_url}oss/v2/buckets?{params}{startAt}"
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
                #loop_buckets = False
        except Exception as error:
            logging.error(error)
        return data

    def get_bukets_details(api_url, token, region='US', bucketKey=''):
        """
        This endpoint will return the buckets owned by the application. This endpoint supports pagination

        @param:api_url
        @type:string

        @param:token
        @type: string

        @param:bucketKey
        @type:int
        URL-encoded bucket key for which to get details

        @Return {
                "bucketKey":"apptestbucket",
                "bucketOwner":"RlKffonB1Ex52GlAbb7N8VJwLllOvpqG",
                "createdDate":1463785698600,
                "permissions":[
                    {
                    "authId":"RlKffonB1Ex52GlAbb7N8VJwLllOvpqG",
                    "access":"full"
                    }
                ],
                "policyKey":"transient"
                }
        """

        data = []
        try:
            
            headers = {'content-type': 'application/json',
                       'Authorization': 'Bearer ' + str(token), 'x-ads-region': str(region)}
            url = f"{api_url}oss/v2/buckets/{bucketKey}/details"
            resp_bucket = requests.get(url, headers=headers)
            if int(resp_bucket.status_code) == 200:
                data = json.loads(resp_bucket.text)

        except Exception as error:
            logging.error(error)
        return data

    def delete_bukets(api_url, token, region='US', bucketKey=''):
        """
        Deletes a bucket. The bucket must be owned by the application.
        We recommend only deleting small buckets used for acceptance testing or
        prototyping, since it can take a long time for a bucket to be deleted.

        Note that the bucket name will not be immediately available for reuse.

        @param:api_url
        @type:string

        @param:token
        @type: string

        @param:bucketKey
        @type:int
        URL-encoded bucket key for which to get details

        @Return {
                "bucketKey":"apptestbucket",
                "bucketOwner":"RlKffonB1Ex52GlAbb7N8VJwLllOvpqG",
                "createdDate":1463785698600,
                "permissions":[
                    {
                    "authId":"RlKffonB1Ex52GlAbb7N8VJwLllOvpqG",
                    "access":"full"
                    }
                ],
                "policyKey":"transient"
                }
        """

        data = []
        try:
            headers = {'content-type': 'application/json',
                       'Authorization': 'Bearer ' + str(token), 'x-ads-region': str(region)}

            url = f"{api_url}oss/v2/buckets/{bucketKey}"
            resp_bucket = requests.delete(url, headers=headers)
            if int(resp_bucket.status_code) == 200:
                data.append({'status':'delete','bucketKey':bucketKey})

        except Exception as error:
            logging.error(error)
        return data
