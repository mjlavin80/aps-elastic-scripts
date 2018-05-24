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
periodical_id integer, title text, qualifier text, )"
c.execute(create_query)

#get list of periodical ids from sqlite
periodical_ids_query = "SELECT _id, periodical_id from periodical_counts GROUP BY periodical_id ORDER BY periodical_id ASC"
rows = c.execute(periodical_ids_query).fetchall()

#set up elastic connection
client = connections.create_connection(hosts=['http://localhost:9200'], timeout=60, max_retries=10, retry_on_timeout=True)
s = Search(using=client, index="documents", doc_type="article")

for i in rows:
    #get all articles by id
    #CURL code here
	body = {
        "_source": ["Publication.Title", "Publication.Qualifier", "NumericPubDate"],
        "query": {
            "match": {
                "Publication.PublicationID": "266687"
                }
            }
        }
    s = s.from_dict(body)
    t = s.execute()
    print(t['hits'].keys())
    #processing dict, title is key
    titles_dict = {}
    #loop articles to look for title and qualifier
    	#has title been seen before?
    	#try:
    		#title = titles_dict[]
    	#if not add (with date)
    	#if so if date earliest or latest, update
    	#has qualifier been seen before?
    	# if not add (with date)
    	#if so if date earliest or latest, update
