from kafka import KafkaProducer
from time import sleep
import os
from subprocess import call

def connect_kafka_producer():
  _producer = None
  try:
    _producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
  except Exception as ex:
    print('Exception while connecting Kafka')
    print(str(ex))
  finally:
    return _producer

def publish_message(producer_instance, topic_name, val):
  try:
    value_bytes = bytes(val, encoding='utf-8')
    producer_instance.send(topic_name, value=value_bytes)
    producer_instance.flush()
    print('Message {} published successfully.'.format(val))
  except Exception as ex:
    print('Exception in publishing message')
    print(str(ex))

def load_data(path, producer_instance, topic_name, sleep_time):
  try:
    with open(path, "rt", encoding="utf-8") as f:
      row_counter = 0
      for row in f:
        row_counter += 1
        if row_counter > 1:
          publish_message(kafka_producer, topic_name, row)
        else:
          pass
        sleep(sleep_time)
  except Exception as ex:
    print('Exception in loading data')
    print(str(ex))

def reset_kafka_topic(topic_name):
  try:
    call(["kafka-topics", "--zookeeper", "localhost:2181", "--delete", "--topic", topic_name])
    call(["kafka-topics", "--create", "--zookeeper", "localhost:2181", "--replication-factor", "1", "--partitions", "1", "--topic", topic_name])
  except Exception as ex:
    print('Exception in resetting data')
    print(str(ex))
  finally:
    pass

if __name__ == '__main__':
  kafka_producer = connect_kafka_producer()

  dataset_dir_path = os.path.join(os.getcwd(), 'lastfm-dataset/csv')
  dataset_file_path = os.path.join(dataset_dir_path, 'user_artists.csv')
  sleep_time = 0.001
  topic_name = 'lastfm'

  reset_kafka_topic(topic_name)
  load_data(dataset_file_path, kafka_producer, topic_name, sleep_time)

  



