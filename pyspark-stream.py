import os,sys #for os.environment handling
import re
#os.environ["PYSPARK_DRIVER_PYTHON"] = "C:\ProgramData\Anaconda3\python.exe"
#os.environ["PYSPARK_PYTHON"] = "C:\ProgramData\Anaconda3\python.exe"
#os.environ["SPARK_PYTHONPATH"] = "C:\ProgramData\Anaconda3\python.exe"
#os.environ["HADOOP_HOME"] = "C:\ProgramData\Anaconda3\Lib\site-packages\pyspark\\"

os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars spark-streaming-kafka-0-8-assembly_2.11-2.3.2.jar pyspark-shell'


from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import json #for raw tweet parsing
from pyspark.streaming.kafka import KafkaUtils

from textblob import TextBlob
from dateutil import parser


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

def format_time(tweet_data, time_format = "%Y-%m-%d %H:%M"):
    parsed_time = parser.parse(tweet_data[0])
    new_time_format = parsed_time.strftime(time_format)

    return (new_time_format, tweet_data[1], tweet_data[2])

def get_sanitation_function():
    """
    To get faster regex, we compile a pattern, and use that throughout our program.
    """
    regex_link_hashtag_username = r"http\S+|@([A-Za-z0-9_]+)|#([A-Za-z0-9_]+)"
    regex_punctuation = r"[….,\/#$%\^&\*;:{}=\-_`~()\n\t]+"
    regex_emoji = u"(\ud83d[\ude00-\ude4f])|"  # emoticons
    u"(\ud83c[\udf00-\uffff])|"  # symbols & pictographs (1 of 2)
    u"(\ud83d[\u0000-\uddff])|"  # symbols & pictographs (2 of 2)
    u"(\ud83d[\ude80-\udeff])|"  # transport & map symbols
    u"(\ud83c[\udde0-\uddff])"  # flags (iOS)
    "+"
    regex = regex_link_hashtag_username+r"|"+regex_punctuation+r"|"+regex_emoji
    regex_pattern = re.compile(regex, flags=re.UNICODE)

    return lambda tweet: sanitize_tweets_fast(tweet, regex_pattern)

def get_sanitation_function2():
    """
    To get faster regex, we compile a pattern, and use that throughout our program.
    """
    regex_v2 = r"@([A-Za-z0-9_]+)|#([A-Za-z0-9_])+|http\S+|[^A-Z^a-z^ ^]+"
    regex_pattern = re.compile(regex_v2, flags=re.UNICODE)

    return lambda tweet: sanitize_tweets_fast(tweet, regex_pattern)

def get_sanitation_function3():
    """
    To get faster regex, we compile a pattern, and use that throughout our program.
    """
    regex = r"(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)"
    regex_pattern = re.compile(regex, flags=re.UNICODE)

    return lambda tweet: sanitize_tweets_fast(tweet, regex_pattern)

def sanitize_tweets_fast(tweet_data, regex_obj):
    #Possibility that this doesnt work, depending on how spark works with this shit
    """
    Removes links, hashtags, username tags, emojis, and punctuation.
    """
    tweet_sanitize = re.sub(' +|[\/\-\n\t]+',' ',tweet_data[2])
    tweet_sanitize = regex_obj.sub(r'', tweet_sanitize)
    return (tweet_data[0], tweet_data[1], tweet_sanitize.lower())



def calculate_sentiment(tweet_data):
    """
    Gets the sentiment by using the textblob api
    """
    tweet_sentiment = TextBlob(tweet_data[2]).sentiment
    return (tweet_data[0], (tweet_sentiment[0], tweet_sentiment[1])) #Set the score in the tuple

def append_sentiments(x,y):
    #TODO is y ever a list?
    if(type(x) is list):
        if(type(y) is list):
            x = x+y
            print("1")
            print(x)
        else:
            print("HERE")
            print(x)
            print(y)
            x = x.append(y)
            print("2")
            print(x)
    elif(type(y) is list):
        #x is not list
        x = y.append(x)
        print("3")
        print(x)
    else:
        #neither is list
        x = [x, y]
        print("4")
        print(x)

    return x

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


    sanitation_function = get_sanitation_function2()#Compiles the regex objects
    parsed_tweets = raw_tweets.map(map_raw_to_tuple).filter(lambda x: x[0] is not None)
    parsed_tweets.pprint()
    parsed_tweets = parsed_tweets.map(format_time)
    parsed_tweets = parsed_tweets.map(sanitation_function)
    parsed_tweets = parsed_tweets.map(calculate_sentiment)
    parsed_tweets.pprint()         # Print tweets we find to the consol
    parsed_tweets = parsed_tweets.groupByKey()
    parsed_tweets.mapValues(list).pprint()
    #parsed_tweets.pprint()         # Print tweets we find to the consol

    ssc.start()			   # Start reading the stream
    ssc.awaitTermination() # Wait for the process to terminate


if __name__ == "__main__":
    ip_address = "localhost"  # Replace with your stream IP
    port = 9092  # Replace with your stream port

    recieve_data(ip_address, port, is_socket=False)


