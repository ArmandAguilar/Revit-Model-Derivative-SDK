# Revit Data Management & Derivative Model SDK
[![Supported Versions](https://img.shields.io/pypi/pyversions/requests.svg)]()
[![License](https://img.shields.io/npm/l/express.svg)]()


## Install

```python
pip install requests
pip install sphinx
pip install .
```

## 🔐 Authentication

```python
objAuthForge = auth.authForge(clientId=os.environ.get('_CLIENT_ID_FORGE_'),
                clientSecret=os.environ.get('_CLIENT_SECRET_FORGE_'),
                url=os.environ.get('_URL_API_FORGE_'))
```

Explore the token
```python
token = objAuthForge.getToken()
```

## 🪣 Bucktes
lorem lorem lorem lorme lorem lorem ore

#### Create bucktes

```python
buckets_oss.BucketsOss.create_bucket(url_api="https://developer.api.autodesk.com/"
                            ,token=token
                            ,bucket_name='000espaciotest'
                            ,policyKey='persistent'
                            ,access='full')

```

#### Read a list buckets

```python
buckets_oss.BucketsOss.get_bukets(api_url='https://developer.api.autodesk.com/',
                    token=token,region='US',
                    limit=0,
                    startAt=0)
```

#### Read  bucket detail

```python
 buckets_oss.BucketsOss.get_bukets_details(api_url='https://developer.api.autodesk.com/',
                                                token=token,region='US',
                                                bucketKey='000espaciotest')

```

#### Delete bucket
```python
buckets_oss.BucketsOss.delete_bukets(
    api_url='https://developer.api.autodesk.com/',
    token=token,region='US',
    bucketKey='000espaciotest')
```


## ☁️ Upload model to bucket
lorem lorem lorem lorem loremlrem

```python
filepath = '/Users/armand/Documents/httdocs/Librerias/forge_sdk/HT-ELEC-001_PG.rvt'
filename = 'HT-ELEC-001_PG.rvt'
bucketKey = '000espaciotest'

buckets_objects_oss.BucketsObjects.uploadSmallModel(api_url='https://developer.api.autodesk.com/',
                            file_name=filename,
                            path_rtv=filepath,
                            objectId=bucketKey,
                            token=token)
```
#### Get all revit model into a bucket

```python
buckets_objects_oss.BucketsObjects.bucketKeyObjects(api_url="https://developer.api.autodesk.com/",
                                              token=token,
                                              bucketKey='000espaciotest',
                                              region='US',
                                              limit=0, 
                                              startAt='')

```

## 🏢 Meta data of model

lorel lore lore mlorem lroem lroem

##### Step 1:  get the GUI
```python

object_urn = 'lorellroem lorem lorem lorem lorem lorem'

info_model = meta_data.ManageMetaData()

info_model.metaData(api_url='https://developer.api.autodesk.com/',
                                  token=token, 
                                  urn=object_urn,
                                  guid='')

```

```json
{
    'data': {
        'type': 'metadata',
        'metadata': [
            {
                'name': '{3D}', 👈
                'role': '3d',
                'guid': 'Ac1d49988-b216-67b2-31eb' 👈
            },
            {
                'name': 'ARQ CUTS',
                'role': '2d',
                'guid': 'AAA1ad404e49730-76e7457fa0ab'
            }
        ]
    }
}

```

##### Step 2:  Create DB of Families and Elements

```python
guid_3d = 'Ac1d49988-b216-67b2-31eb'

info_model.createObjectTreeSqlLite(api_url='https://developer.api.autodesk.com/',
                                        token=token,
                                        urn=object_urn,
                                        guid=data_guid_3,
                                        sqllite_db_name='autodesk.wabi.db',
                                        sqllite_path='/Users/armand/Documents/httdocs/Librerias/aps_sdk/csv/db')

```

##### Step 3:  Print the tree view
lorel lorel lore lore

```python
tree_json = info_model.getObjectTreeSqlLite(sqllite_db_name='autodesk.wabi.db',
                                            sqllite_path='/Users/armand/Documents/httdocs/Librerias/aps_sdk/csv/db')

```

JSON

```json
[
    {
        "name": "Pads",
        "objectid": 462,
        "children": [
            {
                "name": "Pad",
                "objectid": 81334,
                "children": [
                    {
                        "name": "BC_PLATAFORMA TERRENO WABI",
                        "objectid": 81336,
                        "children": [
                            {
                                "name": "Pad",
                                "objectid": 3494,
                                "children": []
                            },
                            {
                                "name": "Pad",
                                "objectid": 3493,
                                "children": []
                            }
                        ]
                    },
                    {
                        "name": "BC_Plataforma de construcci\u00f3n",
                        "objectid": 81335,
                        "children": [
                            {
                                "name": "Pad",
                                "objectid": 11292,
                                "children": []
                            }
                        ]
                    }
                ]
            }
        ]
    }
]
```
