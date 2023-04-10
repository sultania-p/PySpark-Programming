from pyspark import SparkConf, SparkContext

# create the spark conf
conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
# create the sparkcontext object
sc = SparkContext(conf= conf)

def parseLine(line):
    # line -> 0,Will,33,385
    fields = line.split(",")    # [0, Will, 33, 385]
    age = int(fields[2])        # 33
    numFriends = int(fields[3]) # 385
    return (age, numFriends)


lines = sc.textFile('C:/ExD/work/big_data/spark/SparkCourse/fakefriends.csv')
# print(type(lines))
rdd = lines.map(parseLine)  # (33, 385)
                            # (33, 2)
                            # (55, 221)..
friends_rdd = rdd.mapValues(lambda x: (x, 1))   # (33, (385, 1))
                                                # (33, (2, 1))
                                                # (55, (221, 1)) ..
totalsByAge = friends_rdd.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))  # (33, (387, 2)) ..
# As key remains constant we can just call mapvalues for the average
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])        # (33, 325.33) ..
results = averagesByAge.collect()       # returns a list    [(33, 325.33), (26, 242.05) ... ]
for result in results:
    print(result)