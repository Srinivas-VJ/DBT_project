import tweepy
from tweepy.auth import OAuthHandler
from tweepy import Stream
import socket
import json
import os


class TweetsListener(tweepy.Stream):

    def __init__(self, csocket, *args):
        super().__init__(*args)
        self.client_socket = csocket

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

    twitter_stream.filter(track=['ipl'])


if __name__ == "__main__":
    consumer_key = "add  here"
    consumer_secret = "add  here"
    access_token = "add  here"
    access_secret = "add  here"
    new_skt = socket.socket()
    host = "127.0.0.1"
    port = 5556
    new_skt.bind((host, port))

    print("port: %s" % str(port))

    new_skt.listen(5)

    c, addr = new_skt.accept()

    print("got request: " + str(addr))
    send_tweets(c)
