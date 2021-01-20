from components.data_cleaning import tweet_cleaning_for_sentiment_analysis
import json
#import nltk
#nltk.download('punkt')

"""
For each line - Read each json object, extract the desired fields, 
clean tweet text and write as new json object.
"""

def transform_tweet(tweet_json, json_form):
    if not tweet_json["id_str"] or not tweet_json["full_text"]:
        return {}
    json_form["tweet_id"] = tweet_json["id_str"]
    json_form["created_at"] = tweet_json["created_at"] + "#" + json_form["tweet_id"]
    json_form["screen_name"] = tweet_json["user"]["screen_name"] if tweet_json["user"]["screen_name"] else "Unknown"
    cleaned_tweet = tweet_cleaning_for_sentiment_analysis(tweet_json["full_text"].lower())
    if cleaned_tweet:
        json_form["text"] = cleaned_tweet
    else:
        return {}
    json_form["location"] = tweet_json["user"]["location"] if tweet_json["user"]["location"] else "N/A"
    json_form["favorite_count"] = tweet_json["favorite_count"] if tweet_json["favorite_count"] else "0"
    json_form["retweet_count"] = tweet_json["retweet_count"] if tweet_json["retweet_count"] else "0"
    json_form["topic"] = tweet_json["topic"] if tweet_json["topic"] else "N/A"
    return json_form


def preprocess(input, output, keep=1):
    with open(input, 'r', encoding='utf-8') as input_file, open(output, 'w', encoding='utf-8') as output_file:
        line = input_file.readline()
        while line:
            json_form = {
                "tweet_id":"","created_at": "","screen_name": "","text": "",
                "location": "","favorite_count": "","retweet_count": "","topic": ""
            }
            tweet_json = json.loads(line)
            row = transform_tweet(tweet_json, json_form)
            if row:
                output_file.write(json.dumps(row) + '\n')
            line = input_file.readline()
        output_file.close()
        input_file.close()