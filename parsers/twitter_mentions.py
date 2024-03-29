"""
Contains a class that supports retrieval of tweets mentioning papers.
"""

import twint
import re
import json
from datetime import datetime, timedelta, date
from mongoengine import connect, Document, StringField, ListField, DateTimeField, BooleanField, ReferenceField, IntField, DictField
from mongoengine.queryset.visitor import Q
import pymongo
import os

class TweetDocument(Document):
    meta = {"collection": "tweets",
            "allow_inheritance": False
    }    

    # Tweet information
    tweet_text = StringField(required=True)
    tweet_id = StringField(required=True)
    urls = ListField(StringField(required=True), default=lambda: [])
    link = StringField(required=True)
    is_retweet = BooleanField(required=True)
    votes = IntField(default=None)
    tweet_date = DateTimeField(required=True)

    # User information
    username = StringField(required=True)
    user_id = StringField(required=True)
    profile_image_url = StringField(required=True)

    date_updated = DateTimeField(required=True)
    is_queried_tweet = BooleanField(required=True)

    # Thread id
    conversation_id = StringField(required=True)

    # Paper that the tweet mentions
    entry = DictField(required=True)

class TwitterMentions(object):
    """
    Class that supports retrieval of tweets mentioning papers.
    """
    def query_paper_identifiers(self, title, doi, pubmed_id, pmcid, fetch_threads):
        """
        Query tweets with title, doi, pubmed_id and pmcid.
        """
        if title == None or len(title) < 8:
            tweets_title = []
        else:
            tweets_title = self.query(title)
        # tweets_title = []

        if doi == None:
            tweets_doi = []
        else:
            tweets_doi = self.query(doi)

        if pubmed_id == None:
            tweets_pubmed_id = []
        else:
            tweets_pubmed_id = self.query(pubmed_id)

        if pmcid == None:
            tweets_pmcid = []
        else:
            tweets_pmcid = self.query(pmcid)

        # Add up all tweets belonging to the current paper
        tweets_list_query = tweets_title + tweets_doi + tweets_pubmed_id + tweets_pmcid
        return tweets_list_query
        # self.get_tweet_info(tweets_list_query, title, doi, pubmed_id, pmcid, fetch_threads)

    def query(self, query):
        """
        Search for tweets that contain "query".
        """
        tweets = []
        c = twint.Config()
        c.Search = '"' + query + '"'
        c.Lang = 'en'
        c.Store_object = True
        c.Store_object_tweets_list = tweets
        twint.run.Search(c)

        return tweets

    def get_votes_and_profile_image(self, tweet, full_thread_text=None, title=None, doi=None, pubmed_id=None, pmcid=None, return_votes=True):
        """
        Get profile image of tweeter and compute votes of tweet.
        """
        # Inspired by https://github.com/karpathy/arxiv-sanity-preserver
        def tprepro(tweet_text):
            # take tweet, return set of words
            t = tweet_text.lower()
            t = re.sub(r'[^\w\s@]','',t) # remove punctuation
            ws = set([w for w in t.split() if not (w.startswith('#') or w.startswith('@'))])
            return ws

        # Lookup the profile of the user
        users_list = []
        c = twint.Config()
        c.Username = tweet.username
        c.Store_object = True
        c.Store_object_users_list = users_list
        twint.run.Lookup(c)

        # Get number of followers and profile image url
        try:
            num_followers = users_list[0].followers
            profile_image_url = users_list[0].avatar
            bio = users_list[0].bio
        except IndexError:
            num_followers = 0
            profile_image_url = ""
            bio = ""

        if return_votes == False:
            return None, profile_image_url

        # Give low weight to retweets, tweets without comments and tweets with short length
        thread_words = set()
        if full_thread_text:
            for part in full_thread_text:
                thread_words = thread_words | tprepro(part)
        else:
            thread_words = thread_words | tprepro(tweet.tweet)

        query_words = set()
        for identifier in [title, doi, pubmed_id, pmcid]:
            if identifier is not None:
                query_words = query_words | tprepro(identifier)

        for url in tweet.urls:
            query_words = query_words | tprepro(url)

        comments = thread_words - query_words
        isok = int(not(tweet.retweet or len(tweet.tweet) < 40) and len(comments) >= 5)
        tweet_sort_bonus = 10000 if isok else 0

        research_bonus = 0
        # If bio contains keywords such as research/professor, give additional points
        if re.search(r'.*researcher.*', bio, re.IGNORECASE) or re.search(r'.*professor.*', bio, re.IGNORECASE) or re.search(r'.*phd.*', bio, re.IGNORECASE) or re.search(r'.*postdoc.*', bio, re.IGNORECASE) or re.search(r'.*scientist.*', bio, re.IGNORECASE):
            research_bonus += 500

        # Add up all contributing factors
        votes = int(tweet.likes_count) + int(tweet.retweets_count) + tweet_sort_bonus + num_followers + research_bonus

        return votes, profile_image_url

    def save_document(self, tweet, profile_image_url,
                      title, doi, pubmed_id, pmcid, is_queried_tweet, votes=None):
        """
        Save document into mongodb database.
        """
        # Check if paper is already in papers database. Else, update weight by 1.
        if PaperDocument.objects(doi=doi):
            paper = PaperDocument.objects.get(doi=doi)
            paper.weight += 1
            paper.save()
        else:
            paper = PaperDocument(title=title, doi=doi, pubmed_id=pubmed_id, pmcid=pmcid, weight=1).save()

        # Check if tweet already exists, else create new tweet document.
        if not TweetDocument.objects(tweet_id=tweet.id_str):
            TweetDocument(tweet_text=tweet.tweet, tweet_id=tweet.id_str, urls=tweet.urls, link=tweet.link,
                          is_retweet=tweet.retweet, votes=votes, tweet_date=datetime.fromisoformat(tweet.datestamp + ' ' + tweet.timestamp),
                          username=tweet.username, user_id=tweet.user_id_str, profile_image_url=profile_image_url, date_updated=datetime.now(),
                          conversation_id=tweet.conversation_id, is_queried_tweet=is_queried_tweet, paper=paper).save()

    def get_thread_tweets_info(self, thread_tweets, thread_text, title, doi, pubmed_id, pmcid, queried_tweet_id):
        """
        Loop through all tweets in a thread and save in database.
        """
        for tweet in thread_tweets:
            is_queried_tweet = False
            return_votes = False

            if tweet.id == queried_tweet_id:
                is_queried_tweet = True
                return_votes = True
                votes, profile_image_url = self.get_votes_and_profile_image(tweet, thread_text, title, doi, pubmed_id, pmcid, return_votes=return_votes)
            else:
                votes, profile_image_url = self.get_votes_and_profile_image(tweet, return_votes=return_votes)

            TweetDocument(tweet_text=tweet.tweet, tweet_id=tweet.id_str, urls=tweet.urls, link=tweet.link,
                          is_retweet=tweet.retweet, votes=votes, tweet_date=datetime.fromisoformat(tweet.datestamp + ' ' + tweet.timestamp),
                          username=tweet.username, user_id=tweet.user_id_str, profile_image_url=profile_image_url, date_updated=datetime.now(),
                          conversation_id=tweet.conversation_id, is_queried_tweet=is_queried_tweet, paper=paper).save()

            self.save_document(tweet, profile_image_url, title, doi, pubmed_id, pmcid, is_queried_tweet, votes)

    def get_tweet_info(self, tweets, title, doi, pubmed_id, pmcid, fetch_threads):
        """
        Loop through tweets and collect information required in database.
        """
        for tweet in tweets:
            if not fetch_threads:
                # If we want just the queried tweets
                is_queried_tweet = True
                votes, profile_image_url = self.get_votes_and_profile_image(tweet, [tweet.tweet], title, doi, pubmed_id, pmcid, return_votes=True)
                self.save_document(tweet, profile_image_url, title, doi, pubmed_id, pmcid, is_queried_tweet, votes)
            else:
                # If we want to unroll threads
                queried_tweet_id = tweet.id
                is_first_tweet_in_thread = True
                if int(tweet.conversation_id) != int(tweet.id):
                    is_first_tweet_in_thread = False

                # Unroll threads
                thread_tweets, thread_text = self.unroll_thread(tweet, is_first_tweet_in_thread)
                self.get_thread_tweets_info(thread_tweets, thread_text, title, doi, pubmed_id, pmcid, queried_tweet_id)

    def unroll_thread(self, tweet, is_first_tweet_in_thread):
        """
        Unroll thread.
        """
        tweets_list_user = []
        c = twint.Config()
        c.Search = "@" + tweet.username
        if not is_first_tweet_in_thread:
            c.Since = str(datetime.fromisoformat(tweet.datestamp + ' ' + tweet.timestamp) - timedelta(hours=10))
            c.Until = str(datetime.fromisoformat(tweet.datestamp + ' ' + tweet.timestamp) + timedelta(hours=10))
        else:
            c.Since = str(datetime.fromisoformat(tweet.datestamp + ' ' + tweet.timestamp))
            c.Until = str(datetime.fromisoformat(tweet.datestamp + ' ' + tweet.timestamp) + timedelta(hours=30))
        c.Lang = 'en'
        c.Store_object = True
        c.Store_object_tweets_list = tweets_list_user
        twint.run.Search(c)

        conversation_tweets = []
        conversation_tweets_text = []
        for tw in tweets_list_user:
            if tw.conversation_id == tweet.conversation_id:
                conversation_tweets.append(tw)
                conversation_tweets_text.append(tw.tweet)

        return conversation_tweets[::-1], conversation_tweets_text[::-1]

    def query_doc(self, document):
        fetch_threads = False

        title = document.to_mongo().get('title', None)
        doi = document.to_mongo().get('doi', None)
        pubmed_id = document.to_mongo().get('pubmed_id', None)
        pmcid = document.to_mongo().get('pmcid', None)
        entry_dict = {"title": title, "doi": doi, "pubmed_id": pubmed_id, "pmcid": pmcid}

        # twitlen = int(len(self.query_paper_identifiers(title, doi, pubmed_id, pmcid, fetch_threads))) 
        # print(twitlen)
        # print(title, doi, pubmed_id, pmcid)
        tweets = []
        for tweet in self.query_paper_identifiers(title, doi, pubmed_id, pmcid, fetch_threads):
            votes, profile_image_url = self.get_votes_and_profile_image(tweet, full_thread_text=None, title=title, doi=doi, pubmed_id=pubmed_id, pmcid=pmcid, return_votes=True)
            # print(tweet.datestamp + ' ' + tweet.timestamp)
            tweetdoc = TweetDocument(tweet_text=tweet.tweet, tweet_id=tweet.id_str, urls=tweet.urls, link=tweet.link,
                  is_retweet=tweet.retweet, votes=votes, tweet_date=datetime.strptime(tweet.datestamp + ' ' + tweet.timestamp, "%Y-%m-%d %H:%M:%S"),
                  username=tweet.username, user_id=tweet.user_id_str, profile_image_url=profile_image_url, date_updated=datetime.now(),
                  conversation_id=tweet.conversation_id, is_queried_tweet=True, entry=entry_dict)
            tweets.append(tweetdoc)
            tweetdoc.save()
        document.tweets = tweets
        document.last_twitter_search = datetime.now()
        document.save()

        # print('\n')


if __name__ == "__main__":
    def init_mongoengine():
        connect(db=os.getenv("COVID_DB"),
                name=os.getenv("COVID_DB"),
                host=os.getenv("COVID_HOST"),
                username=os.getenv("COVID_USER"),
                password=os.getenv("COVID_PASS"),
                authentication_source=os.getenv("COVID_DB"),
                )

    init_mongoengine()

    twitter_mentions = TwitterMentions()
    for doc in EntriesDocument.objects(Q(last_twitter_search__not__exists=True)):
        twitter_mentions.query_doc(doc)
