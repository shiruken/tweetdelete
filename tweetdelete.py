#!/home/pi/berryconda3/envs/tweetdelete/bin/python

"""
Delete Twitter activity (tweets + likes) older than 3 months old.

Stores tweets in MySQL table with the following format:
    CREATE TABLE `tweetdelete` (
    `id` bigint(20) unsigned NOT NULL,
    `created_at` datetime NOT NULL,
    `kind` tinyint(1) unsigned NOT NULL COMMENT '0=Tweet, 1=Like',
    `favorited` tinyint(1) unsigned DEFAULT NULL COMMENT '1=Liked according to API',
    `error` tinyint(1) unsigned DEFAULT NULL COMMENT 'Flag tweet as inaccesible via API',
    PRIMARY KEY (`id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

"""

import os
import sys
from twython import Twython
import pymysql
import json
from dateutil import parser
from time import sleep, gmtime


class tweetdelete:

    def __init__(self, token_file, read_default_file, database):

        # Initialize Twitter
        if not os.path.isfile(token_file):
            print(f'Could not find {token_file}. We need to reauthenticate.')
            self.authenticate(token_file)
        self.keys = json.load(open(token_file))
        self.twitter = Twython(**self.keys)

        # Open connection to database
        self.db = pymysql.connect(read_default_file=read_default_file, database=database)
        self.c = self.db.cursor()

        # Check if database is empty, indicating this is a first run
        # Direct user to initial setup using the Twitter archive
        if not self.c.execute("""SELECT * FROM tweetdelete"""):
            self.initialize_from_archive()


    def authenticate(self, token_file):
        """ Authenticate with Twitter and save the OAUTH tokens"""

        keys = {}
        keys['app_key'] = input('Enter the App Consumer Key: ').strip()
        keys['app_secret'] = input('Enter the App Consumer Secret: ').strip()

        twitter = Twython(**keys)
        auth = twitter.get_authentication_tokens()

        keys['oauth_token'] = auth['oauth_token']
        keys['oauth_token_secret'] = auth['oauth_token_secret']
        auth_url = auth['auth_url']
        print(f'\nVisit this url to authorize the application: {auth_url}\n')
        oauth_verifier = input('Enter the PIN code: ')

        twitter = Twython(**keys)
        auth = twitter.get_authorized_tokens(oauth_verifier)
        keys['oauth_token'] = auth['oauth_token']
        keys['oauth_token_secret'] = auth['oauth_token_secret']

        with open(token_file, 'w') as fp:
            json.dump(keys, fp)

        print(f'The authorized user tokens were saved to {token_file}')


    def run(self, sql, data):
        """Execute SQL command with data payload"""
        try:
            self.c.execute(sql, data)
            self.db.commit()
            return True
        except Exception as e:
            self.db.rollback()
            print(f'Error: {e}')
            return False


    def flag_error(self, tweet_id):
        """Flag item as inaccesible via the API in the database"""
        sql = """UPDATE tweetdelete SET error=1 WHERE id=%s"""
        if self.run(sql, tweet_id):
            return ' Flagged in database.'
 

    def initialize_from_archive(self):
        """Use Twitter archive to initialize the database"""

        msg = "\nThe `tweetdelete` database is currently empty. Because Twitter restricts API access " \
              "to older content we need to use the account archive to access the full account history. " \
              "Follow the instructions at the following link to download and extract your full archive: " \
              "\nhttps://help.twitter.com/en/managing-your-account/how-to-download-your-twitter-archive"
        path = input(f'{msg}\n\nEnter path to Twitter archive: ').strip()

        tweet_path = os.path.join(path, 'tweet.js')
        if not os.path.exists(tweet_path):
            sys.exit(f'tweet.js not found in {path}. Please try again with valid Twitter archive.')

        like_path = os.path.join(path, 'like.js')
        if not os.path.exists(like_path):
            sys.exit(f'like.js not found in {path}. Please try again with valid Twitter archive.')

        # Load the tweets and add to the database
        print('Parsing tweet.js')
        with open(tweet_path, 'r') as fp:
            tweets = fp.read()
        tweets = tweets.replace('window.YTD.tweet.part0 = ', '')
        count_tweets = 0
        for tweet in json.loads(tweets):
            tweet_id = int(tweet['id_str'])
            created_at = parser.parse(tweet['created_at'])

            sql = """INSERT INTO tweetdelete (id,created_at,kind) VALUES (%s,%s,0)"""
            data = (tweet_id, created_at)
            if self.run(sql, data):
                print(f'{tweet_id} added to database.')
                count_tweets += 1

        # Load the likes and add to the database
        # Note whether the tweet is actually favorited according to the API
        print('Parsing like.js')
        with open(like_path, 'r') as fp:
            likes = fp.read()
        likes = likes.replace('window.YTD.like.part0 = ', '')
        count_likes = 0
        for like in json.loads(likes):
            tweet_id = int(like['like']['tweetId'])

            try:            
                # likes.js does not contain timestamps, use tweet as proxy
                tweet = self.twitter.show_status(id=tweet_id)
                created_at = parser.parse(tweet['created_at'])

                sql = """INSERT INTO tweetdelete (id,created_at,kind,favorited) VALUES (%s,%s,1,%s)"""
                data = (tweet_id, created_at, tweet['favorited'])
                if self.run(sql, data):
                    print(f'{tweet_id} added to database.')
                    count_likes += 1

            except Exception as e:
                print(f'Error retrieving {tweet_id}: {e}')

            sleep(1)
            
        print(f'Added {count_tweets} tweets and {count_likes} likes to the database')


    def add_new_tweets(self):
        """Iterate through user timeline and add tweets to database."""

        # Most recent tweet in database is used to stop iteration
        ret = self.c.execute("""SELECT id FROM tweetdelete WHERE kind=0 ORDER BY id DESC LIMIT 1""")
        max_id_db = self.c.fetchone()[0] if ret else 0

        keep_going = True
        max_id = None
        count = 0

        while keep_going:
            print(f'Loading tweets before: {max_id}')
            timeline = self.twitter.get_user_timeline(count=200, max_id=max_id, include_rts=True, trim_user=True)

            if len(timeline) == 0: # Reached end of timeline
                break

            for tweet in timeline:
                tweet_id = int(tweet['id_str'])
                created_at = parser.parse(tweet['created_at'])

                if tweet_id <= max_id_db:
                    keep_going = False
                    break
                
                sql = """INSERT INTO tweetdelete (id,created_at,kind) VALUES (%s,%s,0)"""
                data = (tweet_id, created_at)
                if self.run(sql, data):
                    print(f'{tweet_id} added to database.')
                    count += 1

            max_id = tweet_id - 1
            sleep(1)

        print(f'Added {count} tweets to the database')


    def add_new_likes(self):
        """Iterate through user likes and add tweets to database."""

        # Most recent like in database is used to stop iteration
        ret = self.c.execute("""SELECT id FROM tweetdelete WHERE kind=1 ORDER BY id DESC LIMIT 1""")
        max_id_db = self.c.fetchone()[0] if ret else 0

        keep_going = True
        max_id = None
        count = 0

        while keep_going:
            print(f'Loading likes before: {max_id}')
            likes = self.twitter.get_favorites(count=200, max_id=max_id, include_entities=False)

            if len(likes) == 0: # Reached end of likes
                break

            for like in likes:
                tweet_id = int(like['id_str'])

                if tweet_id <= max_id_db:
                    keep_going = False
                    break

                # Since the timestamp of likes is not available, we use the current timestamp
                created_at = gmtime()

                sql = """INSERT INTO tweetdelete (id,created_at,kind,favorited) VALUES (%s,%s,1,%s)"""
                data = (tweet_id, created_at, like['favorited'])
                if self.run(sql, data):
                    print(f'{tweet_id} added to database.')
                    count += 1

            max_id = tweet_id - 1
            sleep(1)

        print(f'Added {count} likes to the database')


    def cull_tweets(self):
        """Delete tweets more than 3 months old"""

        # Get all tweets in database more than 3 months old
        sql = """
              SELECT id FROM tweetdelete
              WHERE
                kind=0 AND
                error!=1 AND
                created_at < DATE_SUB(CURDATE(), INTERVAL 3 MONTH)
              """
        self.c.execute(sql)
        tweets = self.c.fetchall()

        count = 0
        for tweet in tweets:
            tweet_id = tweet[0]
            msg = ''

            # Delete the tweet and remove from database
            try:
                self.twitter.destroy_status(id=tweet_id)
                count += 1
                msg += f'Deleted {tweet_id}.'
                if self.run("""DELETE FROM tweetdelete WHERE id=%s""", tweet_id):
                    msg += ' Removed from database.'
            except Exception as e:
                msg += f'Error deleting {tweet_id} ({e}).'
                msg += self.flag_error(tweet_id)

            print(msg)
            sleep(1)
            
        print(f'Deleted {count} tweets')


    def cull_likes(self):
        """Unlike tweets more than 3 months old"""

        # Get all likes in database more than 3 months old that are
        # favorited according to the API
        sql = """
              SELECT id FROM tweetdelete
              WHERE
                kind=1 AND
                favorited=1 AND
                error!=1 AND
                created_at < DATE_SUB(CURDATE(), INTERVAL 3 MONTH)
              """
        self.c.execute(sql)
        likes = self.c.fetchall()

        count = 0
        for like in likes:
            tweet_id = like[0]
            msg = ''

            # Unlike the tweet
            try:
                self.twitter.destroy_favorite(id=tweet_id)
                count += 1
                msg += f'Unliked {tweet_id}.'
                if self.run("""DELETE FROM tweetdelete WHERE id=%s""", tweet_id):
                    msg += ' Removed from database.'
            except Exception as e:
                msg += f'Error unliking {tweet_id} ({e}).'
                msg += self.flag_error(tweet_id)

            print(msg)
            sleep(1)
            
        print(f'Unliked {count} tweets')


if __name__ == '__main__':

    token_file = '/home/pi/tweetdelete/tokens.json'
    read_default_file = '~/.my.cnf'
    database = 'twitter'

    td = tweetdelete(token_file, read_default_file, database)
    td.add_new_tweets()
    td.add_new_likes()
    td.cull_tweets()
    td.cull_likes()
