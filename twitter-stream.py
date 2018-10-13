import os, configparser
import tweepy
import kafka, socket

class TweetsListenerSocket(tweepy.StreamListener):

    def __init__(self, host, port):
        super(TweetsListenerSocket, self).__init__()
        self.client_socket = self.create_socket(host, port)

    def create_socket(self, host, port):
        s = socket.socket()  # Create a socket object
        s.bind((host, port))  # Bind to the port

        print("Listening on port: %s" % str(port))
        s.listen(5)  # Now wait for client connection.
        c, addr = s.accept()  # Establish connection with client.
        print("Received request from: " + str(addr))

        return s

    def on_data(self, data):
        try:
            self.client_socket.send(data.encode('utf-8'))

            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        print(status)
        return True


class TweetsListenerKafka(tweepy.StreamListener):

    def __init__(self, host, port):
        super(TweetsListenerKafka, self).__init__()
        self.producer = self.create_producer(host, port)

    def create_producer(self, host, port):
        client = kafka.SimpleClient(str(host)+":"+str(port))
        producer = kafka.SimpleProducer(client, async=True, batch_send_every_t=10, batch_send_every_n = 1000)  # batch_send_every_n = xxx also
        return producer

    def on_data(self, data):
        try:
            self.producer.send_messages('twitterstream', data.encode('utf-8'))

            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        print(status)
        return True


def send_data(host, port, search_filter, is_socket=False):
    # Read the credententials from 'twitter.txt' file
    config = configparser.ConfigParser()
    config.read('twitter.txt')
    consumer_key = config['DEFAULT']['consumer_key']
    consumer_secret = config['DEFAULT']['consumer_secret']
    access_token = config['DEFAULT']['access_token']
    access_secret = config['DEFAULT']['access_secret']

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)

    if is_socket:
        tweet_listener = TweetsListenerSocket(host, port)
    else:
        tweet_listener = TweetsListenerKafka(host, port)

    twitter_stream = tweepy.Stream(auth, tweet_listener)
    twitter_stream.filter(track=[search_filter])


if __name__ == "__main__":
    send_data("localhost", 9191, 'kavanaugh', is_socket=True)




