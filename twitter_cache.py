#!/usr/bin/env python
import os
import re

from twitter import *
from psycopg2 import extras
from datetime import datetime, timedelta
from textblob import TextBlob
import spacy
import psycopg2

class TwitterDB:
    def __init__(self):
        try:
            self.conn = psycopg2.connect("dbname='postgres' user='postgres' host='localhost' password='postgres'")
            self.cur = self.conn.cursor()

        except:
            print("I am unable to connect to the database")

    def write_db(self, word, twitter_results, recent=True):
        # Process results
        processed_results_list = []
        last_tweet_id = 9999999999999999999

        for result in twitter_results:
            # Create dictionary
            list_result = []
            list_result.append(result['id_str'])                         # tweet id
            list_result.append(word)                                     # tweet search term
            list_result.append(int(result['retweet_count']))             # tweet retweet count
            list_result.append(result['full_text'])                      # tweet text
            list_result.append(int(result['user']['followers_count']))   # user followers ct
            list_result.append(int(result['user']['friends_count']))     # user user_friends_ct ct
            list_result.append(int(result['user']['favourites_count']))  # user_favorites_ct
            list_result.append(int(result['user']['statuses_count']))    # user_statuses_ct followers ct
            list_result.append(result['user']['created_at'])             # user_created_at
            list_result.append(result['created_at'])                     # tweet_created_at
            list_result.append(result['user']['id_str'])                 # user id
            list_result.append(result['user']['screen_name'])            # user name
            if result['id'] < last_tweet_id:
                last_tweet_id = result['id']

            # Add to list
            processed_results_list.append(list_result)

        print('Writing ' + str(len(processed_results_list)) + ' records for (' + word + ') Last ID: ' + str(last_tweet_id))

        # Write to DB
        try:
            extras.execute_values(self.cur, """INSERT INTO twitter2(tweet_id, tweet_search_term, tweet_retweet_ct, tweet_text, user_followers_ct, user_friends_ct, user_favorites_ct, user_statuses_ct, user_created_at, tweet_created_at, user_id, user_name) VALUES %s ON CONFLICT DO NOTHING""", processed_results_list)
            if recent:
                self.cur.execute("""INSERT INTO twitter_id(tweet_search_term, token) VALUES (%s, %s) ON CONFLICT (tweet_search_term) DO UPDATE SET token = %s""", (word, str(last_tweet_id), str(last_tweet_id)))
            else:
                self.cur.execute("""INSERT INTO popular_id(tweet_search_term, token) VALUES (%s, %s) ON CONFLICT (tweet_search_term) DO UPDATE SET token = %s""", (word, str(last_tweet_id), str(last_tweet_id)))
            self.conn.commit()

        except Exception as e:
            print("DB Error:" + str(e))

    def get_token(self, word):
        try:
            self.cur.execute("""SELECT token from twitter_id WHERE tweet_search_term=%s""", (word,))
            row = self.cur.fetchone()
            if row:
                return row[0]
        except Exception as e:
            print("DB Error:" + str(e))

        return '0'

    def get_popular_token(self, word):
        try:
            self.cur.execute("""SELECT token from popular_id WHERE tweet_search_term=%s""", (word,))
            row = self.cur.fetchone()
            if row:
                return row[0]
        except Exception as e:
            print("DB Error:" + str(e))

        return '0'

    def write_db_done(self, word):
        self.cur.execute("""INSERT INTO popular_id(tweet_search_term, token) VALUES (%s, %s) ON CONFLICT (tweet_search_term) DO UPDATE SET token = %s""", (word, 'Done', 'Done'))
        self.conn.commit()

    def pull_keyword_entries(self):
        try:
            self.cur.execute("""SELECT tweet_id, tweet_text from twitter WHERE tweet_keywords is NULL LIMIT 200""")
            row = self.cur.fetchall()
            if row:
                return row
        except Exception as e:
            print("DB Error:" + str(e))

        return []

    def write_calculations(self, db_data):
        try:
            print('Writing ' + str(len(db_data)) + ' records at ' + str(datetime.now().strftime('%m/%d/%Y %I:%M:%S%p')))
            extras.execute_values(self.cur, """UPDATE twitter SET tweet_keywords=data.v1, tweet_length=data.v2, polarity=data.v3, subjectivity=data.v4 FROM (VALUES %s) AS data (id, v1, v2, v3, v4) WHERE twitter.tweet_id=data.id""", db_data)
            self.conn.commit()
        except Exception as e:
            print("DB Error:" + str(e))

    def finalize(self):
        self.cur.close()
        self.conn.close()


class TwitterProcess:
    def __init__(self):
        self.db = TwitterDB()
        self.nlp = spacy.load('en_core_web_sm')
        self.stop_words = self.load_stopwords()
        self.apostrophe_words = self.load_stopwords()
        self.memory_ct = 0

    def get_tweet_sentiment(self, tweet):
        # Create text string from tokens
        analysis = TextBlob(tweet)
        return analysis.sentiment.polarity, analysis.sentiment.subjectivity

    def load_stopwords(self):
        # Load stop words list from file
        with open('StopWords.txt', 'r') as stopwords_file:
            stopwords = stopwords_file.readlines()
        return [x.strip().lower() for x in stopwords]

    def load_apostrophe_words(self):
        with open('ApostropheWords.txt', 'r') as ApostropheWordsFile:
            apostrophe_words = ApostropheWordsFile.readlines()
        return [x.strip() for x in apostrophe_words]

    def skip_token(self, word, tag):
        if tag == 'PUNCT' or tag == 'SYM' or tag == 'X' or tag == 'NUM':
            return True

        if word.lower() in self.stop_words:
            return True

        if word.lower() in self.apostrophe_words:
            return True

        if len(word.strip()) <= 2:
            return True

        if 'http' in word:
            return True

        return False

    def process_token(self, token):
        replacements = [('w/', 'with'),
                        ('\o/', ''),
                        ("i'm", ''),
                        ('mt.', 'mount'),
                        ("’re", 'are'),
                        ("'re", 'are'),
                        ("’ve", 'have'),
                        ("'ve", 'have'),
                        ("n’t", 'not'),
                        ("n't", 'not'),
                        ("’ll", 'will'),
                        ("'ll", 'will')
                        ]
        word = token.text
        for k, v in replacements:
            word = word.replace(k, v)

        # Strip non-alpha
        word = re.sub(r'\W+', '', word)

        return word.lower()

    def process_tokens(self, text):
        # Spacy memory issues - Reload spacy
        self.memory_ct += 1
        if self.memory_ct > 5000:
            self.memory_ct = 0
            self.nlp = spacy.load('en_core_web_sm')

        doc = self.nlp(text)
        tokens = ""
        for token in doc:
            word = self.process_token(token)
            if self.skip_token(word, token.pos_):
                continue

            tokens += word + " "

        return tokens.strip()

    def process_keywords(self):
        while True:
            data = self.db.pull_keyword_entries()
            if not data:
                break

            db_data = []
            # Process each record
            for tweet_id, text in data:
                tokens = self.process_tokens(text)
                tweet_length = len(text)
                polarity, subjectivity = self.get_tweet_sentiment(text)
                db_data.append([tweet_id, tokens, tweet_length, polarity, subjectivity])

            self.db.write_calculations(db_data)

class TwitterCache:
    def __init__(self):
        self.ACCESS_TOKEN = os.environ['ACCESS_TOKEN']
        self.ACCESS_SECRET = os.environ['ACCESS_SECRET']
        self.CONSUMER_KEY = os.environ['CONSUMER_KEY']
        self.CONSUMER_SECRET = os.environ['CONSUMER_SECRET']
        self.t = Twitter(retry=True, auth=OAuth(self.ACCESS_TOKEN, self.ACCESS_SECRET, self.CONSUMER_KEY, self.CONSUMER_SECRET))
        self.db = TwitterDB()
        self.count = 100
        self.days = 3

    def get_starting_date(self):
        oldest_date = datetime.now() - timedelta(days=self.days)
        oldest_date = oldest_date.strftime("%Y-%m-%d")
        return oldest_date

    def tweet_search(self, search_term, token_id='0', search_type='recent', restart=True):
        if token_id == '0':
            until = self.get_starting_date()
            tweet_results = self.t.search.tweets(q=search_term + "' -filter:retweets AND -filter:replies'", until=until, result_type=search_type, count=self.count, lang='en', tweet_mode='extended')['statuses']
        else:
            tweet_results = self.t.search.tweets(q=search_term + "' -filter:retweets AND -filter:replies'", max_id=int(token_id) - 1, result_type=search_type, count=self.count, lang='en', tweet_mode='extended')['statuses']

        if len(tweet_results) == 0 and restart:
            print("No results for word (" + search_term + "), type (" + search_type + ") starting from Now() - # days")
            return self.tweet_search(search_term, '0', search_type)

        return tweet_results

    @staticmethod
    def read_common_words(file):
        # Read common words
        common_words_file = open(file, 'r')
        words = common_words_file.readlines()
        words = [x.strip() for x in words]
        common_words_file.close()
        return words

    def run(self):
        while True:
            # Loop through top 100 common words
            for word in self.read_common_words('10words.txt'):
                # Find latest processed ID of the word from DB (Recent)
                token_id = self.db.get_token(word)
                tweet_results = self.tweet_search(word, token_id)
                if tweet_results:
                    self.db.write_db(word, tweet_results)

            for word in self.read_common_words('15words.txt'):
                # Find latest processed ID of the word from DB (Popular)
                popular_id = self.db.get_popular_token(word)
                if popular_id != 'Done':
                    tweet_results = self.tweet_search(word, popular_id, 'popular', False)
                    if tweet_results:
                        self.db.write_db(word, tweet_results, False)
                    else:
                        self.db.write_db_done(word)

# Main Execution
#cache = TwitterCache()
#cache.run()
process = TwitterProcess()
process.process_keywords()
