import os,sys #for os.environment handling

os.environ["PYSPARK_DRIVER_PYTHON"] = "C:\ProgramData\Anaconda3\python.exe"
os.environ["PYSPARK_PYTHON"] = "C:\ProgramData\Anaconda3\python.exe"
os.environ["SPARK_PYTHONPATH"] = "C:\ProgramData\Anaconda3\python.exe"
os.environ["HADOOP_HOME"] = "C:\ProgramData\Anaconda3\Lib\site-packages\pyspark\\"

os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars spark-streaming-kafka-0-8-assembly_2.11-2.3.2.jar pyspark-shell'


from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import json #for raw tweet parsing
from pyspark.streaming.kafka import KafkaUtils


def quiet_logs(sparkcontext):
    logger = sparkcontext._jvm.org.apache.log4j
    logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


def map_raw_to_tuple(raw_data):
    """ Takes raw tweets and takes out the date/time, language, and text """
    json_parsed_data = json.loads(raw_data[1])
    # pprint.pprint(json_parsed_data)
    if 'created_at' in json_parsed_data:
        time_field = json_parsed_data['created_at']
        lang_field = json_parsed_data['lang']
        text_field = json_parsed_data['text']
        return time_field, lang_field, text_field
    else:
        """ This means we reached the limit data...
            twitter {'limit': {'timestamp_ms': 'xxxxxx', 'track': xx}} 
            """
        return None, None, None


def sanitize_tweets(tweet):
    """
    You didn’t do a good job.
    https://apps.washingtonpost.com/g/page/politics/washington-post-abc-news-poll-oct-8-11-2018/2340/ …
    #kavanaugh #kavanope

    Should become this:

    You didn’t do a good job.
    """
    # TODO tar bort @usernames
    # TODO ta bort emojis
    # TODO ta bort länkar
    sanitize_tweet = tweet
    return sanitize_tweet


def calculate_sentiment(text):
    """

    """
    # score = model.evaluate(text)
    score = 0
    return score


def recieve_data(ip_address, port, is_socket=False):
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")

    sc = SparkContext(conf=conf)
    quiet_logs(sc)
    ssc = StreamingContext(sc, 5) # 5 second batch interval
    conf_str = str(ip_address) + ":" + str(port)

    if is_socket:
        raw_tweets = ssc.socketTextStream(ip_address, port)
    else:
        raw_tweets = KafkaUtils.createDirectStream(
            ssc, topics=['twitterstream'], kafkaParams={"metadata.broker.list": conf_str})

    parsed_tweets = raw_tweets.map(map_raw_to_tuple).filter(lambda x: x[0] is not None)
    parsed_tweets = parsed_tweets.map(sanitize_tweets)
    parsed_tweets = parsed_tweets.map(calculate_sentiment)
    parsed_tweets.pprint()         # Print tweets we find to the consol

    ssc.start()			   # Start reading the stream
    ssc.awaitTermination() # Wait for the process to terminate


if __name__ == "__main__":
    ip_address = "localhost"  # Replace with your stream IP
    port = 9092  # Replace with your stream port

    recieve_data(ip_address, port, is_socket=False)


