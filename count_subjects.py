from elasticsearch_dsl.connections import connections
from elasticsearch_dsl import Search
from elasticsearch import Elasticsearch
import pandas as pd
import sqlite3

conn = sqlite3.connect('datastore.db')
c = conn.cursor()
create_statements = ["CREATE TABLE IF NOT EXISTS source_type_counts (_id integer primary key autoincrement, source_type text, doc_count integer)",
"CREATE TABLE IF NOT EXISTS object_type_counts (_id integer primary key autoincrement, object_type text, doc_count integer);",
"CREATE TABLE IF NOT EXISTS periodical_counts (_id integer primary key autoincrement, periodical_id, doc_count integer, source_id integer, object_id integer, \
FOREIGN KEY(source_id) REFERENCES source_type_counts(_id), FOREIGN KEY(object_id) REFERENCES object_type_counts(_id))"]

for u in create_statements:
    c.execute(u)
    conn.commit()

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

insert_source = "INSERT INTO source_type_counts (_id, source_type, doc_count) VALUES (null, ?,?)"
insert_object = "INSERT INTO object_type_counts (_id, object_type, doc_count) VALUES (null, ?,?)"
    
for i in t.to_dict()['aggregations']['by_source']['buckets']:
    sourceType = i['key']
    docCount = i['doc_count']
    c.execute(insert_source, (sourceType, docCount))
    conn.commit()
    for j in i['by_obj']['buckets']:
        objectType = j['key']
        objectCount = j['doc_count']
        c.execute(insert_object, (objectType, objectCount))
        conn.commit()
        df = pd.DataFrame(j['by_pub']['buckets'], columns=["key", "doc_count"]) 
        
        #get source and objects ids from db, then add to df
	objectType_id = c.execute("SELECT _id FROM object_type_counts WHERE object_type=?", (objectType,)).fetchone()[0]
        df['object_id'] = objectType_id
        sourceType_id = c.execute("SELECT _id FROM source_type_counts WHERE source_type=?", (sourceType,)).fetchone()[0]
        df['source_id'] = sourceType_id
        
        df = df.rename(columns = {"key":"periodical_id"})
        df.to_sql('periodical_counts', con=conn, if_exists='append', index=False)
        conn.commit()
