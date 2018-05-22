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

s = s.from_dict(body)
body = s.to_dict()

t = s.execute()

#df = pd.DataFrame(t.to_dict()['aggregations']['by_subject']['buckets'])
#df.to_csv('sourceTypes.csv')
    
for i in t.to_dict()['aggregations']['by_source']['buckets']:
    df = pd.DataFrame(i['by_pub']['buckets'])
    df.to_csv(i['key']+'.csv')
    print(i['key'])
