import os,sys #for os.environment handling

os.environ["PYSPARK_DRIVER_PYTHON"] = "C:\ProgramData\Anaconda3\python.exe"
os.environ["PYSPARK_PYTHON"] = "C:\ProgramData\Anaconda3\python.exe"
os.environ["SPARK_PYTHONPATH"] = "C:\ProgramData\Anaconda3\python.exe"
os.environ["HADOOP_HOME"] = "C:\ProgramData\Anaconda3\Lib\site-packages\pyspark\\"

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import json #for raw tweet parsing


def quiet_logs(sparkcontext):
    logger = sparkcontext._jvm.org.apache.log4j
    logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


sc = SparkContext("local[2]", "Twitter Demo")
quiet_logs(sc)
ssc = StreamingContext(sc, 5) # 5 second batch interval

IP = "localhost"	# Replace with your stream IP
Port = 5555			# Replace with your stream port

raw_tweets = ssc.socketTextStream(IP, Port)


def map_raw_to_tuple(raw_data):
    """ Takes raw tweets and takes out the date/time, language, and text """
    json_parsed_data = json.loads(raw_data)
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


parsed_tweets = raw_tweets.map(map_raw_to_tuple).filter(lambda x: x[0] is not None)
parsed_tweets.pprint()         # Print tweets we find to the consol

ssc.start()			   # Start reading the stream
ssc.awaitTermination() # Wait for the process to terminate

