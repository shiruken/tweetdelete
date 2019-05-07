""" 
Attempt to remove "broken" likes from tweets that you like according
to the Twitter Archive but were not "favorited" according to the API.
This requires liking and unliking the tweet to completely remove it.

The repeated liking/unliking of content has the potential to be extremely
annoying. A minimum follower threshold is applied to avoid targeting users
with smaller follower counts. Users with many followers likely have notifications
turned off so this script will not bother them.

The Twitter API only allows for 1000 requests per 24 hours, so this
script will likely need to be run multiple times. Pass in optional command 
line arguments to control the minimum follower threshold (defaults to 100,000)
and the starting tweet id (processed from oldest to newest).

python remove_broken_likes.py [-n MIN_FOLLOWERS] [-id START_TWEET_ID]

"""

from twython import Twython, TwythonRateLimitError
from argparse import ArgumentParser
import pymysql
import json
import time

parser = ArgumentParser()
parser.add_argument("-n", dest="min_followers", type=int, default=100000, 
                    help="Minimum number of followers for users")
parser.add_argument("-id", dest="start_tweet_id", type=int, default=0, 
                    help="Starting tweet id (processed from oldest to newest")
args = parser.parse_args()
min_followers = args.min_followers
start_tweet_id = args.start_tweet_id

keys = json.load(open('tokens.json'))
twitter = Twython(**keys)

db = pymysql.connect(read_default_file='~/.raspberry.cnf', database='twitter')
c = db.cursor()

# Get list of likes that were not 'favorited' according to the API
sql = """
      SELECT * FROM tweetdelete
      WHERE
        kind=1 AND
        favorited=0 AND
        id >= %s AND
        created_at < DATE_SUB(CURDATE(), INTERVAL 3 MONTH)
      """
c.execute(sql, start_tweet_id)
likes = c.fetchall()

count = 0
for like in likes:
    tweet_id = like[0]
    tweet = twitter.show_status(id=tweet_id)
    user = tweet['user']['name']
    followers = tweet['user']['followers_count']

    # Only attempt the unliking for users above the follower count threshold
    if followers >= min_followers:
        msg = f'{tweet_id} from {user} ({followers:,}): '
        try:
            twitter.create_favorite(id=tweet_id)
            msg += 'Liked the status. '
            twitter.destroy_favorite(id=tweet_id)
            msg += 'Unliked the status. '
            count += 1
            try:
                c.execute("""DELETE FROM tweetdelete WHERE id=%s""", tweet_id)
                db.commit()
                msg += 'Removed from the database.'
            except Exception as e:
                db.rollback()
                msg += f'Error removing from database: {e}'
        except TwythonRateLimitError:
            print(f'{msg}POST favorites/create rate limit exceeded. Stopping.')
            break
        except Exception as e:
            msg += f'Error unliking {tweet_id}: {e}'
        print(msg)

    else:
        print(f'{tweet_id} from {user} does not meet the follower threshold ({followers:,})')
        
    time.sleep(1)

print(f'Unliked {count} tweets')
