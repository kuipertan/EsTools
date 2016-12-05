
# coding=utf-8
from elasticsearch import Elasticsearch 
import requests,json

'''
   This myreindex.py implements the function reindex, but identifies an new routing key
in the new index. The routing key is the values of some field value from the old index,
not the fix value _routing:"=cat"
'''

ES_ADDR='http://localhost:9200/' 
FROM_INDEX='bfd_mf_v2'
TO_INDEX='bfd_mf_v3'
PERSISTENT='12h'
#SIZE=1000
ROUTING_KEY='titleSimHash'
QUERY = '''{ 
    "query":{ 
        "match_all":{} 
    },
    "size": 1000
}'''




def main(): 
    #q = { "match_all": {} } 
    # http://localhost:9200/index/_search?scroll=5m
    url = '%s%s/_search?scroll=%s' %(ES_ADDR, FROM_INDEX, PERSISTENT)
    r = requests.post(url, data = QUERY, timeout=60)
    if r.status_code != 200:
        return
    #print r.content._scroll_id 
    esr = json.loads(r.content)
    print esr['_scroll_id']
    scroll_id = esr['_scroll_id']
     
    """
    { "index" : { "_index" : "test", "_type" : "type1", "_id" : "1" } }
    { "field1" : "value1" }
    """
    while True:
        bulk_data = ""
        if not esr.has_key("hits"):
            break
        
        if len(esr['hits']['hits']) == 0:
            print 'over'
            break

        bulk = ''
        for doc in esr['hits']['hits']:
            op = {"index":{}}
            op["index"]["_index"] = TO_INDEX
            op["index"]["_type"] = doc["_type"]
            op["index"]["_id"] = doc["_id"]
            op["index"]["_routing"] = doc["_source"][ROUTING_KEY]
            bulk = "%s%s\n%s\n" % (bulk, json.dumps(op), json.dumps(doc["_source"]))
            #bulk = "%s%s\n%s\n" % (bulk, json.dumps(op, ensure_ascii=False), json.dumps(doc["_source"], ensure_ascii=False))

        r = requests.put(ES_ADDR+'_bulk', data = bulk)
        if r.status_code != 200:
            print "bulk error:"
            print r.status_code
            break

        q = '{ "scroll" : "%s", "scroll_id" : "%s" }' % (PERSISTENT, scroll_id)
        url = ES_ADDR + '_search/scroll'
        r = requests.post(url, data = q, timeout=60)
        if r.status_code != 200:
            print r.status_code
            break

        esr = json.loads(r.content)
        print esr['_scroll_id']
        scroll_id = esr['_scroll_id']



    

if __name__ == '__main__': 
    main() 

