# ----------------------------------------------------------------------------
#  * CS5540: Principles of Big Data Management
#  * Project Phase 1
#  * Team #7: Avni Mehta, Sneha Mishra, Arvind Tota
# ----------------------------------------------------------------------------

# Import libraries
import json


# This functions takes .txt file as input
# and returns tweets as a list of json
def parse_tweets():

    tweets_data_path = 'twitter_data.txt'
    twt_data = []
    tweets_file = open(tweets_data_path, "r")
    for line in tweets_file:
        try:
            tweet = json.loads(line)
            twt_data.append(tweet)
        except:
            continue

    print('\tSuccessfully parsed data into json format')
    return twt_data


# This function extracts hashtags and urls from tweets data
def extract_hashtags_urls(twt_data):

    # Extract hashtags and urls into separate files
    htfile = 'hashtags.txt'
    urlfile = 'urls.txt'

    htoutfile = open(htfile, 'w')
    urloutfile = open(urlfile, 'w')

    for i in range(len(twt_data)):

        # Extracting hashtags
        ht = twt_data[i].get('entities').get('hashtags')
        for j in range(len(ht)):
            htoutfile.write(ht[j].get('text'))
            htoutfile.write(' ')

        # Extracting urls (expanded urls only)
        url = twt_data[i].get('entities').get('urls')
        for k in range(len(url)):
            urloutfile.write(url[k].get('expanded_url'))
            urloutfile.write(' ')

    # Print once extraction is complete
    print('\tExtracted hashtags in file - ' + htfile)
    print('\tExtracted urls in file - ' + urlfile)
    print('\nExiting the application.')


# Main Activity
if __name__ == '__main__':

    print('\n--------------------------------------------------------------------------------------')
    print('Big Data Project :')
    print('Collect 100K+ tweets, extract hashtags and urls and run wordcount using Hadoop & Spark')
    print('--------------------------------------------------------------------------------------')

    # Step 2: Parsing the data
    print('\nStep 2. Parsing data..')
    tweets_data = parse_tweets()

    # Step 3: Extracting hashtags and urls
    print('\nStep 3. Extracting hashtags and urls..')
    extract_hashtags_urls(tweets_data)