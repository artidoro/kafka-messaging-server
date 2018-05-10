from kafka import KafkaProducer

print('starting test...')

producer = KafkaProducer(bootstrap_servers='kafka:9092', api_version=(0,10,1))

print('created producer; about to send message')

future = producer.send(topic='test', value=bytes('If you see this message in the consumer terminal, it is because it worked!', 'utf-8'))

result = future.get(timeout=60)

print('msg sent, done!')
