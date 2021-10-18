from __future__ import print_function
from types import SimpleNamespace
from urllib.error import HTTPError
from warnings import simplefilter
from tweepy.errors import Forbidden, NotFound, TooManyRequests, TweepyException, TwitterServerError
import twitter
import os
import json
import time
import requests
from collections import  deque
from datetime import datetime
from pathlib import Path
import sched
from . import settings as s


class TList:

    def __init__(self, list, config, destination_p):
        self.list_name = list.name
        self.last_tweets_a = [None for i in range(100)]
        self.config =config
        self.data_path = destination_p
        self.list = list
        self.since_id = None

        time_sn = 'saved_json' + time.ctime().replace(' ', '_').replace(':', '') + '.json'
        try:
            if(self.list_name == "home"):
                new_tweets = []
                new_tweets_ob = s.api.home_timeline( tweet_mode = "extended")
                if len(new_tweets_ob ) == 0:
                    return
                self.since_id = new_tweets_ob.since_id
                max_id = new_tweets_ob.max_id
                new_tweets += [tweet._json for tweet in new_tweets_ob]
                for i in range(0):
                    new_tweets_ob = s.api.home_timeline(max_id = max_id, tweet_mode = "extended")
                    max_id = new_tweets_ob.max_id
                    new_tweets += [tweet._json for tweet in new_tweets_ob]
            else:
                new_tweets = s.api.list_timeline(owner_screen_name=s.api.verify_credentials().screen_name, list_id=self.list.id, include_rts=True,
                                            count=5000, tweet_mode = 'extended')
                new_tweets = [tweet._json for tweet in new_tweets]
            std_tweets = sorted(new_tweets, key=lambda x: datetime.strptime(x['created_at'], '%a %b %d %H:%M:%S %z %Y'))
        except TooManyRequests as err:
            print("error at _init_: " + str(err) )
        fd = open(Path.joinpath(self.data_path, self.list_name + "_"+ time_sn), 'w')
        fd.write(json.dumps(std_tweets))
        fd.close()
        self.saved_till = datetime.strptime(std_tweets[-1]['created_at'], '%a %b %d %H:%M:%S %z %Y')
        self.last_last_dt = datetime.strptime(std_tweets[-1]['created_at'], '%a %b %d %H:%M:%S %z %Y')
        self.save_mark = 0

        # for tweet in new_tweets:
        #     if tweet.get('is_quote_status', False):
        #         try:
        #             self.get_qt(int(tweet['quoted_status_id_str']), 1, new_tweets)
        #         except KeyError as e:
        #             print(tweet)

        s.db.load_json(new_tweets)

    def api_store_list(self):
        return_tweets = None
        iter_save_mark = 0
        time_sn = 'saved_json' + time.ctime().replace(' ', '_').replace(':', '') + '.json'
        new_tweets = []
        try:
            if(self.list_name == "home"):
                
                new_tweets = []
                new_tweets_ob = s.api.home_timeline(since_id = self.since_id,
                 tweet_mode = "extended")
                if len(new_tweets_ob ) == 0:
                    return
                since_id = self.since_id
                if new_tweets_ob.since_id is not None:
                    since_id = new_tweets_ob.since_id
                new_tweets += [tweet._json for tweet in new_tweets_ob]
                try: 
                    while new_tweets_ob.since_id is not None and len(new_tweets_ob) == 20:
                        new_tweets_ob = s.api.home_timeline(since_id = new_tweets_ob.since_id, tweet_mode = "extended")
                        since_id = new_tweets_ob.since_id
                        new_tweets += [tweet._json for tweet in new_tweets_ob]
                    self.since_id = since_id
                except TooManyRequests as err:
                    print(f"too many requests at home_timeline\
                        some tweets after {since_id} may be missed")
                    self.since_id = None

            else:
                max_id = None
                for i in range(1):
                    new_tweets_ob = s.api.list_timeline(owner_screen_name=s.api.verify_credentials().screen_name, list_id=self.list.id, include_rts=True,
                                         count=80, tweet_mode = 'extended')
                    # max_id = int(new_tweets_ob[18].id_str)
                    new_tweets += [tweet._json for tweet in new_tweets_ob]
                
        except requests.exceptions.ConnectionError as err:
            print(err)
            print('error handeled')
        except TooManyRequests as err:
            print("handeled too many requests at api_store_list. error: " + str(err))

        nt_l = sorted(new_tweets, key=lambda x: datetime.strptime(x['created_at'], '%a %b %d %H:%M:%S %z %Y'))

        new_last_index = 0
        for i, pj_dic in enumerate(nt_l):
            if datetime.strptime(pj_dic['created_at'], '%a %b %d %H:%M:%S %z %Y') < self.last_last_dt:
                new_last_index =  i 
            else:
                break
        index_start = new_last_index + 2 if new_last_index else new_last_index
        new_tbs = nt_l[index_start:]

        # for tweet in new_tbs:
        #     if tweet.get('is_quote_status', False):
        #         try:
        #             self.get_qt(int(tweet['quoted_status_id_str']), 1, new_tbs)
        #         except KeyError as e:
        #             print(tweet)

        s.db.load_json(new_tbs)

        print("new_tbs size : {0}".format(len(new_tbs)))
        if len(new_tbs) > 100:
            print("100 limit exceeded, new_tbs large({0}) at index {1} in nt_l:".format(len(new_tbs), new_last_index))
            for i, t in enumerate(nt_l):
                print('{0} {1} \n'.format(i, t['created_at']))
        # print('new ones:(' + self.list_name + ') \n')
        for pj_dic in new_tbs:
            # print(' ' + pj_dic['text'] + '\n')
            self.last_tweets_a[self.save_mark] = pj_dic
            self.save_mark += 1
            if self.save_mark == 100:
                # for tweet in self.last_tweets_a:
                #     if tweet.get('is_quote_status', False):
                #         try:
                #             self.get_qt(int(tweet['quoted_status_id_str']), 1)
                #         except KeyError as e:
                #             print(tweet)
                st_name = self.saved_till.strftime("%Y-%b-%d_%H%M%S")
                self.last_last_dt = datetime.strptime(pj_dic['created_at'], '%a %b %d %H:%M:%S %z %Y')
                last_last_dt_name = self.last_last_dt.strftime("%Y-%b-%d_%H%M%S")
                fd = open(Path.joinpath(self.data_path, self.list_name + "_" \
                                        + st_name + 'to' + last_last_dt_name + '.json'), 'w')
                fd.write(json.dumps(self.last_tweets_a))
                fd.close()
                print('saved {0} tweets from list {1} at'.format(self.save_mark, self.list_name) + time.ctime() + '\n')
                self.saved_till = self.last_last_dt
                self.save_mark = 0
                return_tweets = self.last_tweets_a
                self.last_tweets_a = [None for i in range(100)]
        print('save_mark: (' + self.list_name + ') ' +  str(self.save_mark) + '\n')

        if nt_l != []:
            self.last_last_dt = datetime.strptime(nt_l[-1]['created_at'], '%a %b %d %H:%M:%S %z %Y')
        return return_tweets
        
    def get_qt(self, qt_id, depth, new_tweets):
        depth += 1
        if depth > 5: return
        try: 
            tweet = None
            tweet = s.api.get_status(id=qt_id, tweet_mode = 'extended')._json
            new_tweets.append(tweet)
        except twitter.error.TwitterError as err:
            print(err)
            pass
        except NotFound as e:
            print("Quoted status error:" + str(e))
        except Forbidden as e:
            print("Quoted status error:" + str(e))

        if tweet != None and tweet.get('is_quote_status', False):
            self.get_qt( tweet['quoted_status_id'], depth, new_tweets)


    
def get_list_tls( config, destination_p, users, destination_u):
    tlists = []
    lists = s.api.get_lists()

    # list_ids = [list.id for list in lists]
    all_list_members = []

    # friends_list = s.api.get_friends(user_id = s.api.verify_credentials().id)
    # s.db.insert_list_tags({'name': 'home})
    # s.db.insert_list_tags_users(friends_list['users'], 'home')
    # while 1:
    #         friends_list = s.api.get_friends(user_id = s.api.verify_credentials().id, cursor = friends_list.next_cursor)
    #         if(len(friends_list) == 0):
    #             break
    #         s.db.insert_list_tags_users(friends_list['users'], 'home')

    for twitter_list in lists:
        
        list_members = s.api.get_list_members(owner_screen_name=s.api.verify_credentials().screen_name,
                                         list_id=twitter_list.id, count = 5000)
        members_in_tweets = [{'user': i._json} for i in list_members]
        s.db.insert_users(members_in_tweets)
        s.db.insert_list_tags([{'name': twitter_list.name}])
        s.db.insert_list_tags_users(list_members, twitter_list.name)
        list_members_names = [member.screen_name for member in list_members]
        list_mem_dics = [{'screen_name': i} for i in list_members_names ]
        all_list_members += list_mem_dics
    home = SimpleNamespace()
    home.name = 'home'
    lists.insert(0, home)
    for twitter_list in lists:
        if twitter_list.name in config['lists']:
            tlists.append(TList(twitter_list, config, destination_p))

    get_lists(tlists)
    get_user_timeline( None, users, 1, 10)

    user_win_start = time.time()
    all_list_members_chunks = chunks(all_list_members, 3)
    n_loops = 0
    while 1:
        wait_sec = 60
        if(len(users)> 0):
            get_user_timeline( None, users, 1, 5)
        get_lists(tlists)
        if(time.time() > user_win_start):
            while 1:
                n_loops += 1
                if(n_loops < 9):
                    try:
                        get_user_timeline( None, next(all_list_members_chunks), 33, 100)
                    except StopIteration:
                        all_list_members_chunks = chunks(all_list_members, 100)
                
                else:
                    n_loops = 0
                    user_win_start += 900
                    wait_sec = 0
                    break
                if(n_loops % 2 == 0):
                    wait_sec = 0
                    break

        time.sleep(wait_sec)


def get_list_tls2(config, destination_p, users, destination_u):
    get_user_timeline( None, users, 1, 10)


def get_lists(tlists):
    
    for tlist in tlists:
        new_tweets = tlist.api_store_list()
        # if new_tweets != None:
        #     s.db.load_json(new_tweets)
    print('checked at' + time.ctime())
    # get_user_timeline(api, destination_u, users)

    # time.sleep(600)


def get_user_timeline( destination, users, range_num, max_results):
    
    for user in users:
        
        # try:
        #     max_id = None
        #     if(user.get('id_str') is not None):
        #         user_tweets = s.api.user_timeline(id = int(user['id_str']), tweet_mode = 'extended')
        #     else:
        #         for i in range(2):
        #             user_tweets = s.api.user_timeline(max_id = max_id, screen_name=user['screen_name'], tweet_mode = 'extended')
        #             max_id = int(user_tweets[18].id_str)
        #             user_tweets += user_tweets
        #         # id = user_tweets[0]["id"]
        #         # user_tweets = s.api.user_timeline(id = id)
        #     user_tweets = [i._json for i in user_tweets]
        #     s.db.load_user_json(user_tweets)
            
        # except requests.exceptions.ConnectionError as err:
        #     print(err)
        #     print('connection error handeled at user timelines')
        # except twitter.error.TwitterError as err:
        #     print("error at user: {0}".format(user) + '\n')
        #     print(err)
        #     print('twitter error handeled at user timelines')
        # print(user_tweets[0])


        # dt_name = datetime.now().strftime("%Y-%b-%d_%H%M%S" + ".json")
        # tweet = s.apiv2.get_tweet(id = 1446736600889659397, media_fields = s.media_fields,  place_fields = s.place_fields,
        #                                 tweet_fields =  s.tweet_fields, user_fields = s.user_fields,
        #                                 expansions=s.expansions)

        try:
            if(user.get('id_str') is None):
                user_obj = s.apiv2.get_user(username=user['screen_name'],
                    user_fields = s.user_fields,
                     tweet_fields = s.tweet_fields )
                user_obj = json.loads(user_obj.text)
                username = user['screen_name']
                id = int(user_obj['data']['id'])
            else:
                 id = int(user['id_str'])
                 user_obj = s.apiv2.get_user(id=id,user_fields = s.user_fields,
                    tweet_fields = s.tweet_fields)
                 user_obj = json.loads(user_obj.text)
                 username = user_obj['data']['username']
                 
            p_token = None

            s.db.cursor.execute(f"select count(*) from tweets\
                where author_id = {id}")
            tweet_count = s.db.cursor.fetchone()[0]
            if tweet_count >= 3200 or tweet_count >= user_obj['data']['public_metrics']['tweet_count']:
                continue
            for i in range(range_num):
                response = s.apiv2.get_users_tweets(id = id, max_results = max_results,
                                        pagination_token = p_token,
                                        media_fields = s.media_fields,  place_fields = s.place_fields,
                                        tweet_fields =  s.tweet_fields, user_fields = s.user_fields,
                                        expansions=s.expansions)
            # user_tweets_dic = [tweet._json for tweet in user_tweets]
            # with open(Path.joinpath(destination,  user + "_"+ dt_name ), 'w') as f:
            #     f.write(json.dumps(user_tweets_dic))
                user_tweets = json.loads(response.text)
                if user_tweets['meta']['result_count'] == 0:
                    break
                for user_tweet in user_tweets['data']:
                    user_tweet['username'] = username
                s.db.load_user_timeline_v2(user_tweets)

                if user_tweets['meta'].get('next_token'):
                    p_token = user_tweets['meta']['next_token']
                else: break

        except requests.exceptions.ConnectionError as err:
            print(err)
            print('connection error handeled at user timelines')
        except twitter.error.TwitterError as err:
            print("error at user: {0}".format(user) + '\n')
            print(err)
            print('twitter error handeled at user timelines')
        except TwitterServerError as err:
            print("error at get_user_timeline: " + str(err))
        except TooManyRequests as err:
            print("error at get_user_timeline: " + str(err))
        # print(user_tweets[0])




def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]