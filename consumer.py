###############
##   Basic testing pulling data back out of kafka. 
##   The data going in was dictionary converted to a string of parsed apache logs. 
##   This pulls the data out, converts it back to a dict to then have Counter stats ran
##   There is no need to parse both sides... complicated for the sake of testing
##   stop polling after certain count for now
###############

from confluent_kafka import Consumer, KafkaError
from collections import Counter
import ast

c = Consumer({'bootstrap.servers': 'kafka-priv', 'group.id': 'mygroup',
              'default.topic.config': {'auto.offset.reset': 'smallest'}})
c.subscribe(['httpd_logs_access'])
running = True
array = []
host_counter = Counter()
cnt = 0
while running:
    msg = c.poll()
    if not msg.error() and cnt < 1000:
       dict = ast.literal_eval(msg.value().decode('utf-8'))
       host_counter.update([dict['host']])
       cnt = cnt + 1
    else:
      running = False

    #elif msg.error().code() != KafkaError._PARTITION_EOF:
    #    print(msg.error())

for lin in host_counter.most_common(5):
    print lin
c.close()
