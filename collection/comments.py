
import os
import csv
import requests
import dateutil
import pandas as pd
from time import sleep
from dotenv import load_dotenv


# load env
load_dotenv('.env')

def auth():
    return os.getenv('bearer_token')

def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers

# def search_tweet_id_url(id):
    
#     search_url = f"https://api.twitter.com/2/tweets?ids={id}" #Change to the endpoint you want to collect data from

#     #change params based on the endpoint you are using
#     query_params = {
#         'expansions': 'author_id,in_reply_to_user_id,geo.place_id',
#         'tweet.fields': 'id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,public_metrics,referenced_tweets,reply_settings,source',
#         'user.fields': 'id,name,username,created_at,description,public_metrics,verified',
#         'place.fields': 'full_name,id,country,country_code,geo,name,place_type',
#         'next_token': {}
#     }
#     return (search_url, query_params)


def conversation_id_url(conversation_id, start_date, end_date, max_result):    
    search_url = f"https://api.twitter.com/2/tweets/search/all?query=conversation_id:{conversation_id}" #Change to the endpoint you want to collect data from

    #change params based on the endpoint you are using
    query_params = {
        'start_time': start_date,
        'end_time': end_date,
        'max_results': max_result,
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

def append_to_csv(json_response, filename, conv_id):
    # A counter variable
    counter = 0
    conv_id = 'conv_id: ' + str(conv_id)
    
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
        # print("# of Tweets added from this response: ", counter)
    except:
        pass

def paginate(conv_id, start_date, end_date, filename):
    bearer_token = auth()
    headers = create_headers(bearer_token)

    max_results = 100
    # Total number of tweets we collected from the loop
    total_tweets = 0
    count = 0 # Counting tweets per time period
    flag = True
    next_token = None
    
    # ./data/2018_MPF_PGR_data.csv
    year = filename.split('/')[3].split('_')[0]
    filename = f'./data/replies/{year}_DELTANMD_replies_data.csv'
        
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
    
    while flag:
        # Code to get tweet_id data
        print("-------------------")
        print("Token: ", next_token)
        conversation_url = conversation_id_url(conv_id, start_date, end_date, max_results)
        conversation_id_response = connect_to_endpoint(conversation_url[0], headers, conversation_url[1], next_token)
        # current tweet result count
        result_count = conversation_id_response['meta']['result_count']
        append_to_csv(conversation_id_response, filename, conv_id)
        # condition to check for next_token in tweet body
        if 'next_token' in conversation_id_response['meta']:
            # Save the token to use for next call
            next_token = conversation_id_response['meta']['next_token']
            print("Next Token: ", next_token)
            if result_count is not None and result_count > 0 and next_token is not None:
                append_to_csv(conversation_id_response, filename, conv_id)
                count += result_count
                total_tweets += result_count
                print("Total Number of Tweets added: ", total_tweets)
                print("-------------------")
                sleep(4)               
        # If no next token exists
        else:
            if result_count is not None and result_count > 0:
                print("-------------------")
                append_to_csv(conversation_id_response, filename, conv_id)
                count += result_count
                total_tweets += result_count
                print("Total Number of Tweets added: ", total_tweets)
                print("-------------------")
                sleep(4)
            
            #Since this is the final request, turn flag to false to move to the next time period.
            flag = False
            next_token = None
        sleep(3)
    print("Total number of results: ", total_tweets)
    

def main():
    files = [
        './data/account_tweets/2021_DELTANMD_data.csv',
        './data/account_tweets/2022_DELTANMD_data.csv',
    ]
    
    for file in files:

        year = file.split('/')[3].split('_')[0]
        
        print(file)
                        
        start_date = f'{year}-01-01T00:00:00.000Z'
        end_date = '2023-02-23T00:00:00.000Z'
        
        print(start_date, end_date)
        
        df = pd.read_csv(file)
        twt_ids = df['conv_id'].to_list()
        
        for twt_id in twt_ids:
            conv_id = int(twt_id.split(': ')[1])
            print(conv_id)
            # call to paginate function
            paginate(int(conv_id), start_date, end_date, file)
    
if __name__ == '__main__':
    main()
