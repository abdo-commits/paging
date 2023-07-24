import argparse
import requests

class JobRunner:
    def __init__(self):
        self.elasticsearch_endpoint = 'http://elasticsearch_endpoint.com'  # replace with actual Elasticsearch endpoint
        self.hive_endpoint = 'http://hive_endpoint.com'  # replace with actual Hive endpoint

    def retrieve_data(self, endpoint):
        print(f"Retrieving data from {endpoint}")
        response = requests.get(endpoint)
        return response.json()

    def write_to_elasticsearch(self, data):
        print(f"Writing data to Elasticsearch at {self.elasticsearch_endpoint}")
        # Write your code for sending 'data' to Elasticsearch

    def write_to_hive(self, data):
        print(f"Writing data to Hive at {self.hive_endpoint}")
        # Write your code for sending 'data' to Hive

    def job1(self):
        print("Running Job 1: Retrieving data from campaigns endpoint, then writing to Elasticsearch and Hive.")
        data = self.retrieve_data('http://example.com/api/campaigns')
        self.write_to_elasticsearch(data)
        self.write_to_hive(data)

    def job2(self):
        print("Running Job 2: Retrieving data from photos endpoint, then writing to Elasticsearch and Hive.")
        data = self.retrieve_data('http://example.com/api/photos')
        self.write_to_elasticsearch(data)
        self.write_to_hive(data)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run different jobs that involve retrieving data from an API, \
                                                  writing to Elasticsearch, and writing to Hive, depending on the \
                                                  argument passed.')
    
    parser.add_argument('job_number', type=int, choices=[1, 2], 
                        help='Enter 1 to run job 1 (retrieve from campaigns, write to Elasticsearch and Hive), \
                              enter 2 to run job 2 (retrieve from photos, write to Elasticsearch and Hive).')

    args = parser.parse_args()
    
    runner = JobRunner()
    
    if args.job_number == 1:
        runner.job1()
    elif args.job_number == 2:
        runner.job2()
