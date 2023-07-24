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

def get_indices_with_doc_count_less_than(es, alias, max_doc_count):
    # Get all indices for the alias
    all_indices = es.indices.get_alias(name=alias).keys()

    indices_with_less_docs = [
        index for index in all_indices
        if es.count(index=index)['count'] <= max_doc_count
    ]

    return indices_with_less_docs

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
    max_doc_count = 5000  # replace this with the maximum document count you want
    alias = 'your_alias'  # replace this with your alias
    indices = get_indices_with_doc_count_less_than(es, alias, max_doc_count)
    print("Indices with document count less than or equal to", max_doc_count, "are:", indices)
    reindex_indices(es, indices, 'new_index_name')

if __name__ == '__main__':
    main_pipeline()
