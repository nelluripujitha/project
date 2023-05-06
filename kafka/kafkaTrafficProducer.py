
#!/usr/bin/env python
import csv
from confluent_kafka import Producer
import json
import sys
from random import choice
from argparse import ArgumentParser, FileType
from configparser import ConfigParser

# Define a callback function to handle delivery reports
def delivery_callback(err, msg):
     if err:
         print('ERROR: Message failed delivery: {}'.format(err))
     else:
         print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))
# Define a function to read from the CSV file and send the data to Kafka
def send_csv_to_kafka(file_path,producer):
    # Define the Kafka topic
    topic = 'trafficDataSet'
    with open(file_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Convert each row to a JSON string
            json_str = json.dumps(row)

            # Send the JSON string to Kafka
            producer.produce(topic, json.dumps(row).encode('utf-8'),"", callback=delivery_callback)
    # Wait for any outstanding messages to be delivered and delivery reports to be received
    producer.flush()

def main():
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])

    # Create Producer instance
    producer = Producer(config)
    if len(sys.argv) != 2:
        print('Usage: python producer.py <file_path>')
        sys.exit(1)

    file_path = "input/Traffic_Counts_at_Signals.csv"
    send_csv_to_kafka(file_path,producer)

if __name__ == '__main__':
    main()
