from datetime import datetime
from elasticsearch import Elasticsearch

def connect_to_elasticsearch(hosts, username=None, password=None):
    try:
        es = Elasticsearch(hosts, http_auth=(username, password) if username and password else None)
        if es.ping():
            print('Connected to Elasticsearch.')
        else:
            print('Could not connect to Elasticsearch.')
        return es
    except Exception as e:
        print('Elasticsearch connection failed:', e)
        return None

def get_indices_created_after(es, date):
    all_indices = es.indices.get_alias("*")
    indices_created_after = [
        index for index in all_indices 
        if datetime.fromtimestamp(int(es.indices.get_settings(index)[index]['settings']['index']['creation_date']) / 1000) > date
    ]
    return indices_created_after

def reindex_indices(es, indices, new_index):
    for index in indices:
        es.reindex(
            body={
                "source": {
                    "index": index
                },
                "dest": {
                    "index": new_index
                }
            }
        )

def main_pipeline():
    es = connect_to_elasticsearch(['http://your_elasticsearch_cluster_url:9200'], 'your_username', 'your_password')
    date = datetime(2023, 1, 1)
    indices = get_indices_created_after(es, date)
    print("Indices created after the date are:", indices)
    reindex_indices(es, indices, 'new_index_name')

if __name__ == '__main__':
    main_pipeline()
