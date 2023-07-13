import kafka
import time

time.sleep(20)

consumer = kafka.KafkaConsumer(group_id='test', bootstrap_servers=['kafka1:19092'])
print(consumer.topics(), flush=True)