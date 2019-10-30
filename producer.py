from time import gmtime, strftime, sleep
from confluent_kafka import Producer

p = Producer({'bootstrap.servers': 'localhost:9092'})


while True:
        now = strftime("%Y-%m-%d %H:%M:%S", gmtime())
        p.produce('test_topic', now.encode('utf-8'))
        p.flush()
        sleep(1)

#p.produce('mytopic', 'Test Message')
#p.flush()