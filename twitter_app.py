import socket
import sys
import requests
import requests_oauthlib
import json
# Replace the values below with yours
ACCESS_TOKEN = 'Replace your key here'
ACCESS_SECRET = 'Replace your key here'
CONSUMER_KEY = 'Replace your key here'
CONSUMER_SECRET = 'Replace your key here'
my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_TOKEN, ACCESS_SECRET)
## get the stream of tweets from the twitter #
def get_tweets():
	url = 'https://stream.twitter.com/1.1/statuses/filter.json?track='+'Pakistan'
	query_url = url
	response = requests.get(query_url, auth=my_auth, stream=True)
	print(query_url, response)
	return response
## Pass tweets to spark ##
def send_tweets_to_spark(http_resp, tcp_connection):
	for line in http_resp.iter_lines():
		try:
			full_tweet = json.loads(line)
			tweet_text = full_tweet['text']#['description']
			print(tweet_text)
			tcp_connection.send(tweet_text + '\n')
		except:
			pass
## 01 define sockets configurations ##
TCP_IP = "localhost"
TCP_PORT = 9009
conn = None
## 02 start binding and start listening at deine port and IP ##
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")
## 03 accept the connection #
conn, addr = s.accept()
print("Connected... Starting getting tweets.")
resp = get_tweets()
send_tweets_to_spark(resp, conn)
