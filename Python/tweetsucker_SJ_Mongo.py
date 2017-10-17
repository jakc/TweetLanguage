## tweetsucker
## pulls tweets from around melbourne from twitter streaming api &
## shoves ones with a valid lat/lon into a running MongoDB instance.

import tweetstream
import pymongo
import datetime
import cld
import codecs
import re

from dateutil.parser import parse

def has_swearing(some_text):
    badwords = set(['anal', 'anus', 'arse', 'ass', 'ballsack', 'balls',
                    'bastard', 'bitch', 'biatch', 'bloody', 'blowjob',
                     'blow job', 'bollock', 'bollok', 'boner', 'boob',
                    'bugger', 'bum', 'butt', 'buttplug', 'clitoris', 'cock',
                    'coon', 'crap', 'cunt', 'damn', 'dick', 'dildo', 'dyke',
                    'fag', 'feck', 'fellate', 'fellatio', 'felching', 'fuck',
                    'f u c k', 'fudgepacker', 'fudge packer', 'flange', 'shit',
                    'goddamn', 'god damn', 'hell', 'homo', 'jerk', 'jizz',
                    'knobend', 'knob end', 'labia', 'muff', 'nigger', 'nigga'])
    words = set(word.strip().lower() for word in some_text.split())
    has_swearing = True if words & badwords else False
    return has_swearing

def lang(some_text):
    lang = cld.detect(some_text, pickSummaryLanguage=True, removeWeakMatches=False)
    # check the reliable property
    if lang[2] is True:
        return lang[0]
    else:
        return "unreliable"

def parse_tweet(in_t):
    t = {}
    #strip out words with prefix of @ or # as these mislead CLD
    #Thom - please make this more elegant, so i can learn...
    s = in_t["text"]
    clean_text = re.sub(r'(?:^|\s)[@#].*?([,;:.!?]|\s|$)', r'\1', s)
    final_text = clean_text.encode('utf-8')
    print "original text:", in_t["text"], "final_text:", final_text
    t["text"] = final_text
    t["shape"] = in_t["coordinates"]["coordinates"][0], in_t["coordinates"]["coordinates"][1]
    t["date_text"] = in_t["created_at"]
    t["datetime"] = parse(in_t["created_at"]) # that's parse as in dateutil
    t["language"] = lang(final_text)
    t["has_swearing"] = has_swearing(in_t["text"])
    return t

connection = pymongo.Connection("localhost", 27017)
db = connection.gcTweets

tweet_collection = db.datemelgeotweets

uname = ''
passwd = ''
extent =["144.0,-39.0,146.0,-37.0"] # a pretty big bbox around melbourne.

# while True loop to keep reconnecting after a ConnectionError.
# This is probably a terrible idea, but seems to work.
while True:
    try:
        with tweetstream.FilterStream(uname, passwd, locations=extent) as stream:
            for tweet in stream:
                # not having the key and having an empty key are both possible.
                # and they both mean "no geolocation." Although if the 'locations'
                # filter parameter is working properly, this should never happen.
                if "coordinates" in tweet and tweet["coordinates"]:
                    tweet_collection.save(parse_tweet(tweet))

    except tweetstream.ConnectionError as e:
        pass
