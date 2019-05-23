from kafka import KafkaConsumer
from json import loads
import os, time

def connect_kafka_consumer(topic_name):
  _consumer = None
  try:
    _consumer = KafkaConsumer(topic_name,
                              bootstrap_servers=['localhost:9092'],
                              auto_offset_reset='earliest',
                              enable_auto_commit=True)
  except Exception as ex:
    print('Exception while connecting Kafka')
    print(str(ex))
  finally:
    return _consumer

def write_batch_file(consumer_instance, batch_data, dir_path):
  batch_counter = 0
  batch_number = 0
  try:
    while True:
      for message in consumer_instance:
        if batch_counter >= batch_data:
          batch_counter = 0
          batch_number += 1
          writefile.close()
        if batch_counter == 0:
          dataset_file_path = os.path.join(dir_path, ('batch' + str(batch_number) + '.txt'))
          writefile = open(dataset_file_path, "w", encoding="utf-8")
        message = message.value.decode("utf-8")
        writefile.write(message)
        print('Write message {}'.format(message))
        batch_counter += 1
        print('Current batch : ' + str(batch_number) + ' Current time for this batch : ' + str(batch_counter))
  except KeyboardInterrupt:
    writefile.close()
    print('Keyboard Interrupt called by user, exiting.....')

if __name__ == '__main__':
  topic_name = 'lastfm'
  kafka_consumer = connect_kafka_consumer(topic_name)
  
  if not os.path.exists('lastfm-dataset/batch/'):
    os.makedirs('lastfm-dataset/batch/')
  dataset_dir_path = os.path.join(os.getcwd(), 'lastfm-dataset/batch')
  
  batch_data = 31000 
  write_batch_file(kafka_consumer, batch_data, dataset_dir_path)
  

  


