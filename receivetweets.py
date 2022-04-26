import tweepy
from tweepy.auth import OAuthHandler
from tweepy import Stream
import socket
import json
import os
from dotenv import load_dotenv


load_dotenv()

consumer_key = os.getenv("CONSUMER_KEY")
consumer_secret = os.getenv("CONSUMER_SECRET")
access_token = os.getenv("ACCESS_TOKEN")
access_token_secret = os.getenv("ACCESS_TOKEN_SECRET")

print(consumer_key)
print(consumer_secret)
print(access_token)
print(access_token_secret)

# we create this class that inherits from the StreamListener in tweepy StreamListener


class TweetsListener(tweepy.Stream):

    def __init__(self, csocket):
        self.client_socket = csocket
    # we override the on_data() function in StreamListener

    def on_data(self, data):
        try:
            message = json.loads(data)
            print(message['text'].encode('utf-8'))
            self.client_socket.send(message['text'].encode('utf-8'))
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def if_error(self, status):
        print(status)
        return True


def send_tweets(c_socket):
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    twitter_stream = Stream(auth, TweetsListener(c_socket))
    # we are interested in this topic.
    twitter_stream.filter(track=['football'])


if __name__ == "__main__":
    new_skt = socket.socket()         # initiate a socket object
    host = "127.0.0.1"     # local machine address
    port = 5555                 # specific port for your service.
    new_skt.bind((host, port))        # Binding host and port

    print("Now listening on port: %s" % str(port))

    new_skt.listen(5)  # waiting for client connection.
    # Establish connection with client. it returns first a socket object,c, and the address bound to the socket
    c, addr = new_skt.accept()

    print("Received request from: " + str(addr))
    # and after accepting the connection, we aill sent the tweets through the socket
    send_tweets(c)
