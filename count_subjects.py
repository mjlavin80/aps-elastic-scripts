from elasticsearch_dsl.connections import connections
from elasticsearch_dsl import Search
from elasticsearch import Elasticsearch

es = Elasticsearch('localhost', timeout=60, max_retries=10, retry_on_timeout=True)

client = connections.create_connection(hosts=['http://localhost:9200'])

s = Search(using=client, index="documents", doc_type="article")

#CURL code here
body = {
    "size": 0,
    "aggs": {
        "by_subject": {
            "terms": {
                "field": "SourceType.keywords",
                "size": 10
            }
        }
    }
}

s = s.from_dict(body)
body = s.to_dict()

t = s.execute()

print(t)