#!/usr/bin python
# -*- coding: utf-8 -*-
import logging
import sys
import json
import requests


class authForge:

    def __init__(self,clientId, clientSecret, url):
        try:
            self.clientId = clientId
            self.clientSecret = clientSecret
            self.url = url
        except Exception as err:
            logging.error(err)

    def getToken(self, scope='data:read', time_out=60):
        """
        Forge Token: Make the login in FOREGE API
        @param:scope
        @type:string
        @example : data:read data:write data:create bucket:create bucket:read bucket:delete

        @return token
        """
        try:
            token = ''
            headers = {'Content-Type': 'application/x-www-form-urlencoded'}

            payload = {'client_id': str(self.clientId), 'client_secret': str(self.clientSecret), 'grant_type': "client_credentials",
                       "scope": "data:read data:write data:create bucket:create bucket:read bucket:delete"}

            res_aut_forge = requests.post(
                str(self.url) + '/authentication/v2/token', data=payload, headers=headers, timeout=time_out)

            if res_aut_forge.status_code == 200:
                data_resp = json.loads(res_aut_forge.content)
                token = data_resp['access_token']

        except Exception as error:
            logging.error(error)

        return token
