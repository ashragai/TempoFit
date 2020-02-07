#########
#Script to generate blocks of Pulsar producers to simulate streaming FitRec data
#Each block is assigned one producer and fixed number of users
#All the user data in a block is written to one topic for initial processing (running avg of heartrate)
#########
import pulsar
import sys
import time
import csv
import ast
from collections import defaultdict

#establish pulsar-client connection
client = pulsar.Client('pulsar://10.0.0.4:6650')

#generates a producer in a given block
def generateProducer(block):
    topic_name = 'input-block-' + str(block)
    producer = client.create_producer(topic_name)
    return producer

#reads workout data from CSV and outputs a dictionary of producers and user data for each block
def readWorkouts(file, numLines, block_size):
    user_data = defaultdict(dict)
    #grab the heart_rate and timestamp data from a workout in the fitrec data
    with open(file) as f:
        reader = csv.reader(f)
        header = next(reader)
        for ii in range(numLines):
            workout_block = int(ii/block_size)
            print "Reading line {} and placing on block {}".format(ii, workout_block)
            if ii % block_size == 0:            
                user_data[workout_block]['producer'] = generateProducer(workout_block) 
                print "Producer for block {} generated successfully...".format(workout_block)
                user_data[workout_block]['timestamp'] = []
                user_data[workout_block]['heartrate'] = []
            workout = next(reader)
            heart_rate, timestamp = ast.literal_eval(workout[3]), ast.literal_eval(workout[-3])
            t0 = timestamp[0]
            timestamp = [x - t0 for x in timestamp]
            user_data[workout_block]['timestamp'].append(iter(timestamp))
            user_data[workout_block]['heartrate'].append(iter(heart_rate))
    return user_data

#walks through  a block of user data and publishes to the block's input Pulsar function
#topic schema is [user-id, time, heartrate]
#returns True if every user in the block has published all data
def publishData(block, delay = 0.1):
    num_users = len(block['timestamp'])
    pd = block['producer']
    block_complete = True
    for ii in range(num_users):
        try:
            msg = [ii, next(block['timestamp'][ii]), next(block['heartrate'][ii])]
            print msg
            block_complete = False
            pd.send(str(msg)).encode('utf-8')
            time.sleep(delay)
        except:
            pass
    return block_complete

def main(file_name):
    numLines = 100
    block_size = 100
    lag = 0.25
    to_publish = readWorkouts(str(file_name), numLines, block_size)
    blk_stat = [True] * len(to_publish)
    while True:
        for blk in to_publish:
            if blk_stat:
                print "Publishing data from block ", blk
                publishData(to_publish[blk])
                time.sleep(lag)
        if not all(blk_stat):
            break

#reads workout data in a csv file, stores the data in a dictionary in blocks
#each block is then published to its own topic 
if __name__ == '__main__':
    main(sys.argv[1])

