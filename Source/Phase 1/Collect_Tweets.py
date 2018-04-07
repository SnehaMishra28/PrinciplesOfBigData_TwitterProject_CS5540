# ----------------------------------------------------------------------------
#  * CS5540: Principles of Big Data Management
#  * Project Phase 1
#  * Team #7: Avni Mehta, Sneha Mishra, Arvind Tota
# ----------------------------------------------------------------------------

# Import necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

# Import other libraries
import datetime


# This is a basic listener for received tweets to stdout.
class TwitterListener(StreamListener):

    tweet_number = 0                      # class variable

    def __init__(self,max_tweets):
        super().__init__()
        self.max_tweets=max_tweets           # max number of tweets
        self.last = datetime.datetime.now()  # last time for status

    def on_data(self, data):

        # Write to file
        with open('twitter_data.txt', 'a') as my_file:
            my_file.write(data)

        # Count no. of tweets collected
        self.tweet_number += 1

        # Stop when the limit is reached
        if self.tweet_number >= self.max_tweets:
            print('\tStopping data collection : Limit of '+str(self.max_tweets)+' tweets reached.')
            # Return False to the listener's on_data() of streaming.py API
            return False

        self.status(self.tweet_number)
        return True

    # Status method prints out tweet counts every ten minutes
    def status(self, tweetCount):
        now = datetime.datetime.now()
        # Print status every 10 mins
        if (now - self.last).total_seconds() > 600:
            print('\t' + str(tweetCount) + ' tweets collected..')
            self.last = now

    # On error, print error status
    def on_error(self, status):
        """ Handles the response error status. """
        print('Error status code : ' + str(status))


# Main Activity
if __name__ == '__main__':
    print('\n--------------------------------------------------------------------------------------')
    print('Big Data Project :')
    print('Collect 100K+ tweets, extract hashtags and urls and run wordcount using Hadoop & Spark')
    print('--------------------------------------------------------------------------------------')

    # Variables that contains the user credentials to access Twitter API
    access_token = "712180562-wnFa9ahIaiR7mFZrHyodaOmYepgl0cL2Rsr2bGfs"
    access_token_secret = "GHYZH6CpQouvUS3EeRSbrGMOtxHPkBfLM9r6dvX9MYHAW"
    consumer_key = "bR6Laeo9MOxlvqCVNvBlwPuvn"
    consumer_secret = "fmjeR54JsgF5yQouBBcmubWZCtGn602zNy8G2KEJFZk9G3Ahli"

    n = 10000  # no of tweets to be collected

    # Twitter authentication and the connection to Twitter Streaming API
    listener = TwitterListener(n)
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, listener)

    print('\nStep 1. Collecting tweets..')

    # Filter twitter streams to capture data by the specified keywords and languages
    stream.filter(track=['MachineLearning', 'BigData'], languages=["en"])

