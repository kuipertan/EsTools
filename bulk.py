from elasticsearch import Elasticsearch
import json
client = Elasticsearch('localhost:9201',sniffer_timeout=180)


def delete_docs(bd):
    response = client.bulk(
        index="bfd_mf_v2",
        request_timeout=180,
        body=bd)

'''
for hit in response['hits']['hits']:
    print(hit['_score'], hit['_source']['title'])
'''

fp = open('rmids', 'r')
ids = fp.readlines()
fp.close()

# { "delete" : { "_index" : "test", "_type" : "type1", "_id" : "2" } }

one = { "delete" : { "_index" : "bfd_mf_v2", "_type" : "type1", "_id" : "2" } }
bd=''
for line in ids:
    t,id = line.split()
    one['delete']['_type'] = t
    one['delete']['_id'] = id
    bd = bd + json.dumps(one) + '\n'

delete_docs(bd)
