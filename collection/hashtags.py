import os
import csv
import time
import logging
import requests
import dateutil

from time import sleep
from dateutil import parser
from dates import date_list
from dotenv import load_dotenv

# log file
logging.basicConfig(filename='app.log', filemode='a', format='%(asctime)s - %(levelname)s - %(message)s', level=logging.DEBUG)

load_dotenv()

def auth():
    return os.getenv('bearer_token')

def create_headers(bearer_token):
    headers = {"Authorization": f"Bearer {bearer_token}"}
    return headers

def create_url(query_, start_date, end_date, max_results):
    search_url = "https://api.twitter.com/2/tweets/search/all"
    query_params = {
        'query': f'{query_}',
        'start_time': start_date,
        'end_time': end_date,
        'max_results': max_results,
        'expansions': 'author_id,in_reply_to_user_id,geo.place_id',
        'tweet.fields': 'id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,public_metrics,referenced_tweets,reply_settings,source',
        'user.fields': 'id,name,username,created_at,description,public_metrics,verified',
        'place.fields': 'full_name,id,country,country_code,geo,name,place_type',
        'next_token': {}
    }
    return (search_url, query_params)

def connect_to_endpoint(url, headers, params, next_token = None):
    params['next_token'] = next_token   #params object received from create_url function
    response = requests.request("GET", url, headers = headers, params = params)
    print("Endpoint Response Code: " + str(response.status_code))
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

def get_place(json_response):
    dict = {}
    places = json_response
    for i in places:
        id = i['id']
        country = i['country']
        dict[id] = country
    return dict

def get_user_details(json_response):
    dict = {}
    for user in json_response:
        user_id = int(user['id'])
        name = str(user['username'])
        user_created_at = dateutil.parser.parse(user['created_at'])
        followers = int(user['public_metrics']['followers_count'])
        followings = int(user['public_metrics']['following_count'])
        verified = user['verified']
        data = [
            user_id, name, user_created_at, 
            followers, followings, verified
        ]
        dict[user_id] = (data)
    return dict

def append_to_csv(json_response, filename):
    # A counter variable
    counter = 0
    try:
        user_data = get_user_details(json_response['includes']['users'])
        place_data = get_place(json_response['includes']['places']) if 'places' in json_response['includes'] else None
        location = place_data if place_data is not None else None

        #Open OR create the target CSV file
        csvFile = open(filename, "a", newline="", encoding='utf-8', )
        csvWriter = csv.writer(csvFile)
        # Loop through each tweet
        for tweet in json_response['data']:
            # We will create a variable for each since some of the keys might not exist for some tweets
            # So we will account for that
            tweet_id = tweet['id']
            tweet_url = f'https://twitter.com/twitter/status/{int(tweet_id)}'
            author_id = tweet['author_id']
            conv_id = f"conv_id: {int(tweet['conversation_id'])}"
            
            # User dict lookup by author_id
            user_details = user_data[int(author_id)]
            user_id = user_details[0]
            user_page = f' https://twitter.com/i/user/{int(user_id)}'
            username = user_details[1]
            user_created_at = user_details[2]
            user_followers = user_details[3]
            user_followings = user_details[4]
            verified_user = user_details[5]

            # user location
            if location is not None:
                if 'geo' in tweet:
                    user_location_id = tweet['geo']['place_id']
                    user_country = location[user_location_id]
                else:
                    user_country = ''
            else:
                user_country = ''
            # tweet field
            tweet_created_at = dateutil.parser.parse(tweet['created_at'])
            tweet_text = tweet['text']        
            is_retweet = 'True' if str(tweet['text']).startswith('RT ') else 'False'
            
            tweet_lang = tweet['lang']
            retweet_count = tweet['public_metrics']['retweet_count']
            like_count = tweet['public_metrics']['like_count']
            quote_count = tweet['public_metrics']['quote_count']

            # Assemble all data in a list
            res = [
                user_page, username, user_created_at, user_followers,
                user_followings, verified_user, tweet_url, conv_id,
                user_country, tweet_created_at, tweet_text, is_retweet,
                tweet_lang, retweet_count, like_count, quote_count
            ]
            # Append the result to the CSV file
            csvWriter.writerow(res)
            counter += 1
        # Closing the CSV file
        csvFile.close()
        # Print the number of tweets for this iteration
    except:
        pass
    
    
def main(keyword):
    print('Function called')
    #Inputs for tweets
    bearer_token = auth()
    headers = create_headers(bearer_token)

    max_results = 500
    total_tweets = 0

    # Iterate over dates inputs
    for i in range(0, len(date_list)):
        
        year = date_list[i].split(',')[0].split('-')[0]
        # filename
        filename = f'./data/hashtag_data/{year}/{keyword}_data.csv'
        
        is_file_created = os.path.exists(filename)
        
        # Create new file with headers
        if is_file_created == False:
            
            csvFile = open(filename, "a", newline="", encoding='utf-8')
            csvWriter = csv.writer(csvFile)
            
            csvWriter.writerow(
                [   
                    'user_page', 'username', 'user_created_at', 'user_followers',
                    'user_followings', 'verified_user', 'tweet_url', 'conv_id',
                    'user_country', 'tweet_created_at', 'tweet_text', 'is_retweet',
                    'tweet_lang', 'retweet_count', 'like_count', 'quote_count'
                ]
            )
            csvFile.close()
        # Do not create file with headers
        else:
            pass
        
        start_time = time.time()
        logging.debug(f"Extracting tweets for start_date = {date_list[i].split(',')[0]} and end_date = {date_list[i].split(',')[1]}")
        # Inputs
        count = 0 # Counting tweets per time period
        flag = True
        next_token = None
        # Check if flag is true
        print(f'Extracting tweets for start_date = {date_list[i].split(",")[0]} and end_date = {date_list[i].split(",")[1]}')
        while flag:
            # Check if max_count reached
            print("-------------------")
            print("Token: ", next_token)
            url = create_url(keyword, date_list[i].split(',')[0].strip(), date_list[i].split(',')[1].strip(), max_results)
            json_response = connect_to_endpoint(url[0], headers, url[1], next_token)         
            result_count = json_response['meta']['result_count']
            
            if result_count == 0:
                print(json_response)
            
            append_to_csv(json_response, filename)
            # iterating over next_token data
            if 'next_token' in json_response['meta']:
                # Save the token to use for next call
                next_token = json_response['meta']['next_token']
                print("Next Token: ", next_token)
                if result_count is not None and result_count > 0 and next_token is not None:
                    append_to_csv(json_response, filename)
                    count += result_count
                    total_tweets += result_count
                    print("Total # of Tweets added: ", total_tweets)
                    print("-------------------")
                    sleep(4)          
            # If no next token exists
            else:
                if result_count is not None and result_count > 0:
                    print("-------------------")
                    append_to_csv(json_response, filename)
                    count += result_count
                    total_tweets += result_count
                    print("Total # of Tweets added: ", total_tweets)
                    print("-------------------")
                    sleep(4)
                #Since this is the final request, turn flag to false to move to the next time period.
                flag = False
                next_token = None
            sleep(3)
        print("Total number of results: ", total_tweets)
        logging.debug(f"Total Tweets collected for start_date = {date_list[i].split(',')[0]} and end_date = {date_list[i].split(',')[1]} is {count}")
        logging.debug(f"Tweets data for start_date = {date_list[i].split(',')[0]} and end_date = {date_list[i].split(',')[1]} collected in --- {round(time.time() - start_time, 2)} seconds ---")
        logging.debug('\n')
        print()
        # calling the preprocessor function

# calling the main methond
hashtags = [
    "#Lavajato",
    "#Lava_jato",
    "#lavajatointocavel",
    "#operação_lava_jato",
    "#Odebrecht",
    "#delação"
]

for hashtag in hashtags:
    print(hashtag)
    main(hashtag)