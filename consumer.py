from confluent_kafka import Consumer, KafkaError

settings = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'mygroup',
    'client.id': 'client-1',
    'enable.auto.commit': True,
    'session.timeout.ms': 25000,
    'default.topic.config': {'auto.offset.reset': 'smallest'}
}
c = Consumer(settings)
c.subscribe(['even'])

settingsc2 = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'mygroup',
    'client.id': 'client-3',
    'enable.auto.commit': True,
    'session.timeout.ms': 25000,
    'default.topic.config': {'auto.offset.reset': 'smallest'}
}
c2 = Consumer(settingsc2)
c2.subscribe(['odd'])


settingsc3 = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'mygroup',
    'client.id': 'client-2',
    'enable.auto.commit': True,
    'session.timeout.ms': 25000,
    'default.topic.config': {'auto.offset.reset': 'smallest'}
}
c3 = Consumer(settingsc3)
c3.subscribe(['zero'])

while True:
    msg = c.poll(0.1)
    if msg is None:
        continue
    elif not msg.error():
        print('Received message: {0}'.format(msg.value()))
        # print('consumer 1 : {0}'.format(msg2.value()))
    else:
        print('Error occured: {0}'.format(msg.error().str()))

    msg = c2.poll(.1)

    if msg is None:
        continue
    elif not msg.error():
        print('Received message: {0}'.format(msg.value()))
    else:
        print('Error occured: {0}'.format(msg.error().str()))

    msg = c3.poll(.1)

    if msg is None:
        continue
    elif not msg.error():
        print('Received message: {0}'.format(msg.value()))
    else:
        print('Error occured: {0}'.format(msg.error().str()))

