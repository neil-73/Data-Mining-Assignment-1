# multipleTweetSearch.py 

# Data Mining Assignment 1: Twitter Hashtag Search Code
#       Neil Ramdath         - 100519195
#       Faizan Contractor    - 100451291

# Import required packages
import tweepy
import json

from tweepy import Stream
from tweepy import OAuthHandler 
from tweepy.streaming import StreamListener

from pymongo import MongoClient
from threading import Thread

# Set up db
con = MongoClient('localhost', 27017)
dBase  = con.streamData

# Create database collection for each hashtag
altcoin         = dBase.altcoin
bitcoin         = dBase.bitcoin
coindesk        = dBase.coindesk
cryptocurrency  = dBase.cryptocurrency
gold            = dBase.gold
appl            = dBase.appl
goog            = dBase.goog
yhoo            = dBase.yhoo

# Custom access & token keys
consumerKey = "LLprq6uMCZlMrmLhERfbiTRKy"
consumerSecret = "szjQI3kLvESaareW1SFL3PqZOJKIfXhGWhIMiKXEzgM4txW9Xq"
accessToken = "784853241251131395-djoXasQg4NV6wCeXuoBdJhDyS3yJ6Y9"
accessSecret = "MQyllMCDCUpRzY4O4XBEmWabkZzXW6JwoUii9LWGp8X0Q"
    
# Create streamlistener class    
class StreamListener(tweepy.StreamListener):

    # Define __init___ 
    def __init__(self, keyword, api=None):

        # Call super method
        super(StreamListener, self).__init__(api)
        self.keyword = keyword

    # Define on status
    def on_status(self, tweet):

        #Print to console
        print ('Ran "on_status"')

    # Define on error
    def on_error(self, status_code):

        # Return value
        return False

    # Define when streaming data
    def on_data(self, data):

        # Use try & except for error handling
        try:
            
            # Load tweet as string to avoid errors
            tweet = json.loads(str(data))

            # Assign value to variable "user"
            user = tweet["user"]["screen_name"] + " tweeted #" + self.keyword
            
            # Print contents of user    
            print (user)

            # Insert specific tweet into appropriate db collection
            # Using set of if & else if statements ...

            if self.keyword == 'altcoin':
                altcoin.insert(tweet)

            elif self.keyword == 'bitcoin':
                bitcoin.insert(tweet)

            elif self.keyword == 'coindesk':
                coindesk.insert(tweet)

            elif self.keyword == 'cryptocurrency':
                cryptocurrency.insert(tweet)

            elif self.keyword == 'gold':
                gold.insert(tweet)

            elif self.keyword == 'appl':
                appl.insert(tweet)

            elif self.keyword == 'goog':
                goog.insert(tweet)

            elif self.keyword == 'yhoo':
                yhoo.insert(tweet)

        # Use except to handle potential error
        except ValueError:
            print ("Value error...")

# Define the start of the stream
def start_stream(auth, track):
    tweepy.Stream(auth=auth, listener=StreamListener(track)).filter(track=[track])

# Use keys & tokens to authorize twitter API access
auth = tweepy.OAuthHandler(consumerKey, consumerSecret)
auth.set_access_token(accessToken, accessSecret)

# Define hashtags being searched for...
track = ['altcoin','bitcoin','coindesk','cryptocurrency','gold','appl','goog','yhoo']

# Use for loop to search for multiple hashtags simultaneously...
for objectVar in track:

    # Pass parameters to thread
    thread = Thread(target=start_stream, args=(auth, objectVar))
    # Start the created thread
    thread.start()