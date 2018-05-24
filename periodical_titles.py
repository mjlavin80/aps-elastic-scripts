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
periodical_id integer, title text, qualifier text, FOREIGN KEY(periodical_id) REFERENCES periodical_counts(_id))"

c.execute(create_query)

#get list of periodical ids from sqlite
periodical_ids_query = "SELECT _id, periodical_id from periodical_counts GROUP BY periodical_id ORDER BY periodical_id ASC"
rows = c.execute(periodical_ids_query).fetchall()

#set up elastic connection
client = connections.create_connection(hosts=['http://localhost:9200'], timeout=60, max_retries=10, retry_on_timeout=True)
s = Search(using=client, index="documents", doc_type="article")

for i in rows[:1]:
    #get all articles by id
    #CURL code here
    body = {
        "_source": ["Publication.Title", "Publication.Qualifier", "NumericPubDate"],
        "query": {
            "match": {
                "Publication.PublicationID": i[1]
                }
            }
        }
    s = s.from_dict(body)
    t = s.execute()
    
    #processing dict, title is key
    titles_dict = {}
    
    #loop articles to look for title and qualifier
    for hit in t.to_dict()['hits']['hits']:
        pubdate = hit['_source']['NumericPubDate']
        qual = hit['_source']['Publication']['Qualifier']
        text_title = hit['_source']['Publication']['Title']
        text_title_qual = text_title + "#####" + qual
    	#has title been seen before?
    	try:
            #if so check date, if earliest or latest, update
            if pubdate < titles_dict[text_title_qual]['first']:
                titles_dict[text_title_qual]['first'] = pubdate
            if pubdate > titles_dict[text_title_qual]['last']:
                titles_dict[text_title_qual]['last'] = pudate                
    	except:
            #if not, add (with dates)
            titles_dict[text_title_qual] = {}
    	    titles_dict[text_title_qual]['first'] = pubdate
            titles_dict[text_title_qual]['last'] = pudate
    print(titles_dict.keys())