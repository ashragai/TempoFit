import pulsar
from pulsar import schema, ConsumerType
import ast
import redis
import json

#establish pulsar connection and create consumer with sub
client = pulsar.Client('pulsar://10.0.0.4:6650,10.0.0.13:6650,10.0.0.10:6650')
consumer = client.subscribe(topic= 'fullout', subscription_name= 'sub', consumer_type= ConsumerType.Shared) 

R_HOST = os.getenv('RED_HOST')
R_PORT = os.getenv('RED_PORT')
redisCli = redis.Redis(host= R_HOST, port = R_PORT)


#recieve and process msgs
#write incoming data to Redis store
while True:
    msg= consumer.receive()
    try:
        (idn, hr, dur, title, artist) = ast.literal_eval(msg.data())
        if not client.exists(idn):
            usr_dict = {'heartrate' : [hr], 'title' : [title], 'dur' : [dur]}
        else:
            usr_dict = json.loads(client.get(idn))
            usr_dict['heartrate'].append(hr)
            usr_dict['title'].append(title)
            usr_dict['dur'].append(dur)
        update = json.dumps(usr_dict)
        redisCli.set(idn, update)
        consumer.acknowledge(msg)
    except Exception as e:
        print e
        consumer.negative_acknowledge(msg)