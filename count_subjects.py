from elasticsearch_dsl.connections import connections
from elasticsearch_dsl import Search
from elasticsearch import Elasticsearch
import pandas as pd

client = connections.create_connection(hosts=['http://localhost:9200'], timeout=60, max_retries=10, retry_on_timeout=True)

s = Search(using=client, index="documents", doc_type="article")

#CURL code here
body = {
    "size": 0,
    "aggs": {
        "by_source": {
            "terms": {
                "field": "SourceType",
                "size": 100
            },
        "aggs": {
        "by_obj": {
            "terms": {
                "field": "ObjectType",
                "size": 99999
            },
         "aggs": {
         "by_pub": {
            "terms": {
                "field": "Publication.PublicationID",
                "size": 99999
                }
              }
            }
          }
        }
      }
    }
  }

s = s.from_dict(body)
body = s.to_dict()

t = s.execute()

    
for i in t.to_dict()['aggregations']['by_source']['buckets']:
    sourceType = i['key']
    docCount = i['doc_count']
    for j in i['by_obj']['buckets']:
        objectType = j['key']
        objectCount = j['doc_count']
        df = pd.DataFrame(j['by_pub']['buckets'], columns=["key", "doc_count"]) 
        df['objectType'] = objectType
        df['sourceType'] = sourceType
print(df)
