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
            #print "Reading line {} and placing on block {}".format(ii, workout_block)
            if ii % block_size == 0:
                print "Generating producer for block ", workout_block
                user_data[workout_block]['producer'] = generateProducer(workout_block)
                print "Producer created successfully..."
                user_data[workout_block]['timestamp'] = []
                user_data[workout_block]['heartrate'] = []
            workout = next(reader)
            timestamp, heart_rate = ast.literal_eval(workout[1]), ast.literal_eval(workout[2])
            t0 = timestamp[0]
            timestamp = [x - t0 for x in timestamp]
            user_data[workout_block]['timestamp'].append(iter(timestamp))
            user_data[workout_block]['heartrate'].append(iter(heart_rate))
    print "Data written to dict"
    return user_data

def send_callback(res, msg):
    pass

#walks through  a block of user data and publishes to the block's input Pulsar function
#topic schema is [user-id, time, heartrate]
#returns True if every user in the block has published all data
def publishData(block, block_size, delay = 0.0001):
    N = len(block['timestamp'])
    pd = block['producer']
    block_complete = True
    t1 = time.time()
    recs = 0
    for ii in range(N):
        try:
            msg = [ii, next(block['timestamp'][ii]), next(block['heartrate'][ii])]
            if ii == 0:
                print msg
            if block_complete:
                block_complete = False
            pd.send_async(str(msg).encode('utf-8'), send_callback)
            recs += 1
            time.sleep(delay)
        except Exception as e:
            print e
            pass
    if recs != 0:
        print "Published {} events at rate {}".format(recs, (time.time() -t1)/recs)
    else:
        print "Failed to publish the records :-("
    return block_complete

def main(file_name):
    numLines = 1000
    block_size = 1000
    lag = 2.0
    to_publish = readWorkouts(str(file_name), numLines, block_size)
    blk_done = [False] * len(to_publish)
    while True:
        for blk in to_publish:
            if not blk_done[blk]:
                print "Publishing data from block ", blk
                blk_done[blk] = publishData(to_publish[blk], block_size)
        time.sleep(lag)
        if all(blk_done):
            break

#reads workout data in a csv file, stores the data in a dictionary in blocks
#each block is then published to its own topic
if __name__ == '__main__':
    main(sys.argv[1])