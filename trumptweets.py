from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import SimpleProducer, KafkaClient
from kafka import KafkaProducer
import json

access_token = "1163722320675917825-FksT39mhHtA5HIkgi68ztAcTfK30g0"
access_token_secret = "AXq9ZQqzrEnZBkIcVRnHvQ59sKeeskGpDrqFvvyahLzEt"
consumer_key = "swNlLMN3d4iCzbMbAlt33c122"
consumer_secret = "iJ5Ql2xEJMVj5pHSQv2bSClmqaw1tYVC1Rru5ZGjXwYqDkTlSE"

class StdOutListener(StreamListener):
    def on_data(self, data):
        print("ok")
        print(type(data))
        print(type(data.encode('utf-8')))
        producer.send("trump", data.encode('utf-8'))
        # producer.send_messages("trump", data.encode('utf-8'))
        return True
    def on_error(self, status):
        print (status)

kafka = KafkaClient("localhost:9092")
# producer = SimpleProducer(kafka,  batch_send_every_n=2,
#                           batch_send_every_t=1000, async=True)
producer = KafkaProducer(bootstrap_servers='localhost:9092')
l = StdOutListener()
print("no")

auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, l)
print("s")
stream.filter(track="jaipur")

