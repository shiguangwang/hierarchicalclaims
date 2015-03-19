import cjson
import sys
import codecs
from apollo_lib import util
import xlsxwriter

tweets = util.read_and_parse_tweets_from_file(sys.argv[1], True)
workbook = xlsxwriter.Workbook(sys.argv[2])
worksheet = workbook.add_worksheet()
my_format = workbook.add_format({'font_color': 'black'})

worksheet.write(0, 0, "created_at")
worksheet.write(0, 1, "user_name")
worksheet.write(0, 2, "user_id")
worksheet.write(0, 3, "tweet_id")
worksheet.write(0, 4, "tweet_coordinates")
worksheet.write(0, 5, "tweet_geo")
worksheet.write(0, 6, "user_location")
worksheet.write(0, 7, "user_profile_location") 
worksheet.write(0, 8, "text")

idx = 0
for tweet in tweets:
    idx += 1
    text = util.renderTweetForPlain(util.get_tweet_text(tweet))
    tweet_id = util.get_tweet_id_str(tweet)
    source_id = util.get_tweet_source_id_str(tweet)
    created_at = util.get_tweet_created_at(tweet)
    tweet_coordinates = str(util.get_tweet_coordinates(tweet))
    tweet_geo = str(util.get_tweet_geo(tweet))
    user_location = util.get_user_location(tweet)
    user_profile_location = str(util.get_user_profile_location(tweet))
    try:
        source_name = util.get_tweet_source_name(tweet)
    except Exception:
        source_name = ""
    worksheet.write(idx, 0, created_at)
    worksheet.write(idx, 1, source_name)
    worksheet.write(idx, 2, source_id)
    worksheet.write(idx, 3, tweet_id)
    worksheet.write(idx, 4, tweet_coordinates)
    worksheet.write(idx, 5, tweet_geo)
    worksheet.write(idx, 6, user_location)
    worksheet.write(idx, 7, user_profile_location)
    worksheet.write(idx, 8, text)

workbook.close()
