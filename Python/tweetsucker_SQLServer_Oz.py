## tweetsucker
## pulls tweets from around melbourne from twitter streaming api &
## shoves ones with a valid lat/lon into a running SQL Server instance.

## ToDo:
##  - Use v3 method os string substitution for INSERT
## - Dates are wrong.  Looking at DB, the date recorded is a good half day behind?  Is this US time?

import tweetstream
import pymssql
import datetime
import cld
import codecs
import re
import pyproj

from dateutil.parser import parse
from time import sleep

swear_list = []

def main():
    # Populate list with profanity
    build_swear_list()

    # Get tweets from Streaming API
    uname = ''
    passwd = ''
    extent =["107.4,-46.4,162.0,-9.5"] # Oz
    #extent =["144.0,-39.0,146.0,-37.0"] # Melb
	
    # while True loop to keep reconnecting after a ConnectionError
    while True:
        try:
            with tweetstream.FilterStream(uname, passwd, locations=extent) as stream:
                for tweet in stream:
                    # not having the key and having an empty key are both possible
                    # and they both mean "no geolocation." Although if the 'locations'
                    # filter parameter is working properly, this should never happen
                    if "coordinates" in tweet and tweet["coordinates"]:
                        parse_tweet(tweet)

        except tweetstream.ConnectionError as e:
            now = datetime.datetime.now()
            print "ConnectionError"
            print tweetstream.ConnectionError.details
            print e.message + " time: " + str(now)
            # Wait some time before reconnect.
            # This avoids Twitter 420 errors and rate limiting with Streaming API
            sleep(150)
            pass
        except tweetstream.AuthenticationError as e:
            # Cant use OAuth with tstream.
            # This is used for testing 420 scenario.
            print "Authentication Error"
            now = datetime.datetime.now()
            print e.message  + " time: " + str(now)
            sleep(200)
            pass

def parse_tweet(in_t):
    # connect to SQL Server instance
    conn = pymssql.connect(host='localhost', user='sa', password='lispB612', database='tweets')

    # commits every transaction automatically and setup cursor
    conn.autocommit(True)
    cur = conn.cursor()

    raw = in_t["text"]
    textDB = raw.encode('utf-8')
    textDB2 = textDB.replace("'", "")
    textDB3 = textDB2[0:168]  #ToDo Tidy this up
    clean = clean_text(raw)
    lan = lang(clean)
    lat = in_t["coordinates"]["coordinates"][0]
    lon = in_t["coordinates"]["coordinates"][1]
    d = parse(in_t["created_at"]) # that's parse as in dateutil
    date_str = in_t["created_at"]
    swearing = detect_swearing(in_t["text"])
    user_id = in_t["user"]["id_str"]
    user_id_str = str(user_id)
    tweet_id = in_t["id_str"]
    tweet_id_str = str(tweet_id)
	
    print textDB	
	
    # Project Lat/Lons to WGS84 Web Mercator with pyproj
    coords = project(lat, lon)
    x = coords[0]
    y = coords[1]

    # Store projected coords in a GEOMETRY type field
	# geom_type = "'POINT(%s %s)'" % (x, y)
    geom_type = "geometry::STPointFromText('POINT(%s %s)', 3857)" % (x, y)
    try:
        cur.execute("INSERT INTO tweets (geom, text, x, y, swearing, language, date, userid, tweetid) VALUES (%s, '%s', '%s', '%s', '%i', '%s', '%s', '%s', '%s')" % (geom_type, textDB3, x, y, int(swearing), lan, d, user_id_str, tweet_id_str))
    except TypeError:
        print "Could not INSERT", clean

    conn.close()

def project(lat, lon):
    p1 = pyproj.Proj(init='epsg:4326')
    p2 = pyproj.Proj(init='epsg:3857')
    coords = pyproj.transform(p1, p2, lat, lon)
    return coords

def build_swear_list():
    global swear_list
    swear_list = open('dontreadme.txt', 'r').read().splitlines()

def detect_swearing(some_text):
    global swear_list
    words = set(word.strip().lower() for word in some_text.split())
    has_swearing = True if words & set(swear_list) else False
    return has_swearing

def lang(some_text):
    lang = cld.detect(some_text, pickSummaryLanguage=False, removeWeakMatches=True)
    # some_text is unicode at this point. CLD wants UTF-8
    # lang = cld.detect(codecs.getencoder('UTF-8')(some_text)[0])
    # print lang
    # check the reliable property
    if lang[2] is True:
        return lang[0]
    else:
        return "unreliable"

def clean_text(dirty_text):
    try:
        # strip out hashtags, URLs and special characters as these mislead CLD.
        # cant use a-z, as need to accomodate non ascii fonts (e.g. asia).
        just_text = re.sub(r'(?:@\S*|#\S*|http(?=.*://)\S*|[\'{}\(\)\^$&._%#!@=<>:;,~`"\?\*\?\/\+\|\[\\\\])', r'', dirty_text)

        # Convert to UTF8
        text_UTF8 = just_text.encode('utf-8')

        # Ensure less than 160 characters
        text_160chars = text_UTF8[0:160]
        return text_160chars

    except:
        print "Exception"
        traceback.print_exception

if __name__ == '__main__':
    main()


