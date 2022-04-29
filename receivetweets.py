import tweepy
from tweepy.auth import OAuthHandler
from tweepy import Stream
import socket
import json
import os

consumer_key = "kfLBGa8SzFFVXb8yDD05ujFBK"
consumer_secret = "eeRF19jxEastmCtp0CYj4X86ZKfiOCNb6KzAnXGmJdrQN7q2vO"
access_token = "942255638205177858-KiRafYc6uQDkCmXUIracz3ljk6KT6NM"
access_secret = "WLzqah0oGNN46KpYlWZpcETU7NUPJI9ng3SvFVkqhfV12"

# we create this class that inherits from the StreamListener in tweepy StreamListener


class TweetsListener(tweepy.Stream):

    def __init__(self, csocket, *args):
        super().__init__(*args)
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
    auth.set_access_token(access_token, access_secret)

    twitter_stream = TweetsListener(
        c_socket, consumer_key, consumer_secret, access_token, access_secret)
    # we are interested in this topic.
    twitter_stream.filter(track=['ipl'])


if __name__ == "__main__":
    new_skt = socket.socket()         # initiate a socket object
    host = "127.0.0.1"     # local machine address
    port = 5555               # specific port for your service.
    new_skt.bind((host, port))        # Binding host and port

    print("Now listening on port: %s" % str(port))

    new_skt.listen(5)  # waiting for client connection.
    # Establish connection with client. it returns first a socket object,c, and the address bound to the socket
    c, addr = new_skt.accept()

    print("Received request from: " + str(addr))
    # and after accepting the connection, we aill sent the tweets through the socket
    send_tweets(c)
