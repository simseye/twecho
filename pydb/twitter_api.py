from __future__ import print_function
import twitter
import os
import json
import time
import requests
from collections import  deque
from datetime import datetime
from . import twitter_db
from pathlib import Path
class TList:



    def __init__(self, list, api,config, destination_p):
        self.list_name = list.name
        self.api = api
        self.last_tweets_a = [None for i in range(100)]
        self.config =config
        self.data_path = destination_p
        self.list = list
        time_sn = 'saved_json' + time.ctime().replace(' ', '_').replace(':', '') + '.json'
        if(self.list_name == "home"):
                new_tweets = self.api.GetHomeTimeline(count=200)
                new_tweets = [tweet._json for tweet in new_tweets]
                # new_tweets = json.dumps(new_tweets)
        else:
            new_tweets = self.api.GetListTimeline(owner_screen_name=self.config['owner_screen_name'], list_id=self.list.id, include_rts=True,
                                        count=200, return_json=True)
        std_tweets = sorted(new_tweets, key=lambda x: datetime.strptime(x['created_at'], '%a %b %d %H:%M:%S %z %Y'))

        fd = open(Path.joinpath(self.data_path, self.list_name + "_"+ time_sn), 'w')
        fd.write(json.dumps(std_tweets))
        fd.close()
        self.saved_till = datetime.strptime(std_tweets[-1]['created_at'], '%a %b %d %H:%M:%S %z %Y')
        self.last_last_dt = datetime.strptime(std_tweets[-1]['created_at'], '%a %b %d %H:%M:%S %z %Y')
        self.save_mark = 0
        twitter_db.load_json(new_tweets)

    def api_store_list(self):
        return_tweets = None
        iter_save_mark = 0
        time_sn = 'saved_json' + time.ctime().replace(' ', '_').replace(':', '') + '.json'
        new_tweets = []
        try:
            if(self.list_name == "home"):
                new_tweets = self.api.GetHomeTimeline(count=200)
                new_tweets = [tweet._json for tweet in new_tweets]
            else:
                new_tweets = self.api.GetListTimeline(owner_screen_name=self.config['owner_screen_name'], list_id=self.list.id, include_rts=True,
                                         count=200, return_json=True)
        except requests.exceptions.ConnectionError as err:
            print(err)
            print('error handeled')
        nt_l = sorted(new_tweets, key=lambda x: datetime.strptime(x['created_at'], '%a %b %d %H:%M:%S %z %Y'))

        new_last_index = 0
        for i, pj_dic in enumerate(nt_l):
            if datetime.strptime(pj_dic['created_at'], '%a %b %d %H:%M:%S %z %Y') < self.last_last_dt:
                new_last_index =  i 
            else:
                break
        index_start = new_last_index + 2 if new_last_index else new_last_index
        new_tbs = nt_l[index_start:]
        twitter_db.load_json(new_tbs)

        print("new_tbs size : {0}".format(len(new_tbs)))
        if len(new_tbs) > 100:
            print("new_tbs large({0}) at index {1} in nt_l:".format(len(new_tbs), new_last_index))
            for i, t in enumerate(nt_l):
                print('{0} {1} \n'.format(i, t['created_at']))
        # print('new ones:(' + self.list_name + ') \n')
        for pj_dic in new_tbs:
            # print(' ' + pj_dic['text'] + '\n')
            self.last_tweets_a[self.save_mark] = pj_dic
            self.save_mark += 1
            if self.save_mark == 100:
                for tweet in self.last_tweets_a:
                    if tweet.get('is_quote_status', False):
                        try:
                            self.get_qt(int(tweet['quoted_status_id_str']), 1)
                        except KeyError as e:
                            print(tweet)
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
        
    def get_qt(self, qt_id, depth):
        depth += 1
        if depth > 5: return
        try: 
            tweet = None
            tweet = self.api.GetStatus(status_id=qt_id).AsDict()
        except twitter.error.TwitterError as err:
            print(err)
            pass
        if tweet != None and tweet.get('is_quote_status', False):
            self.get_qt( tweet['quoted_status_id'], depth)


def get_list_tls(api, config, destination_p, db_password):
    tlists = []
    twitter_db.initializedb(db_password)
    lists = api.GetLists()
    list_ids = [list.id for list in lists]
    for list in lists:
        
        list_members = api.GetListMembers(owner_screen_name=config['owner_screen_name'],
                                         list_id=list.id)
        twitter_db.insert_list_tags(list_members, list.name)

    for list in lists:
        if list.name in config['lists']:
            tlists.append(TList(list, api,config, destination_p))

    while 1:
        for tlist in tlists:
            new_tweets = tlist.api_store_list()
            # if new_tweets != None:
            #     twitter_db.load_json(new_tweets)
        print('checked at' + time.ctime())
        time.sleep(600)

