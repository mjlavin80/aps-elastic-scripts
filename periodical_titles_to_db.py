#!/usr/bin/env python3 
import sqlite3
from elasticsearch_dsl.connections import connections
from elasticsearch_dsl import Search
from elasticsearch import Elasticsearch

#set up db
conn = sqlite3.connect('datastore.db')
c = conn.cursor()

#create table if not exists periodical_meta
create_query = "CREATE TABLE IF NOT EXISTS periodical_meta (_id integer primary key autoincrement, \
periodical_id integer, aps_id integer, title text, qualifier text, FOREIGN KEY(periodical_id) REFERENCES periodical_counts(_id))"

c.execute(create_query)

#get list of periodical ids from sqlite
periodical_ids_query = "SELECT _id, aps_id from periodical_counts GROUP BY aps_id ORDER BY aps_id ASC"
rows = c.execute(periodical_ids_query).fetchall()

#set up elastic connection
client = connections.create_connection(hosts=['http://localhost:9200'], timeout=60, max_retries=10, retry_on_timeout=True)
s = Search(using=client, index="documents", doc_type="article")

for i in rows:
    #CURL code here
    body = {
        "_source": ["Publication.Title", "Publication.Qualifier"],
        "query": {
            "match": {
                "Publication.PublicationID": i[1]
                }
            }
        }
    #get all articles by id
    s = s.from_dict(body)
    t = s.execute()
    
    #get first result (all ids should have at least one, or else where did the id come from?)
    first_hit = t.to_dict()['hits']['hits'][0]
    
    qual = first_hit['_source']['Publication']['Qualifier']
    text_title = first_hit['_source']['Publication']['Title']
    insert_statement = "INSERT INTO periodical_meta (_id, periodical_id, aps_id, title, qualifier) VALUES (null, ?, ?, ?)"
    c.execute(insert_statement, (i[0], i[1], text_title, qual))
    conn.commit()