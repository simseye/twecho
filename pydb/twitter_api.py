from __future__ import print_function
import twitter
import os
import json
import time
from collections import  deque
from datetime import datetime



class TList:



    def __init__(self, list_name):
        self.list_name = list_name
        # self.last_tweets = deque([], 200)
        self.last_tweets_a = [None for i in range(100)]

        time_sn = 'saved_json' + time.ctime().replace(' ', '_').replace(':', '') + '.json'
        new_tweets = api.GetListTimeline(owner_screen_name=config['owner_screen_name'], slug=list_name, include_rts=True,
                                         count=200, return_json=True)
        std_tweets = sorted(new_tweets, key=lambda x: datetime.strptime(x['created_at'], '%a %b %d %H:%M:%S %z %Y'))

        fd = open(os.path.abspath(data_path + list_name + "_" + time_sn), 'w')
        fd.write(json.dumps(std_tweets))
        fd.close()
        self.saved_till = datetime.strptime(std_tweets[-1]['created_at'], '%a %b %d %H:%M:%S %z %Y')
        self.last_last_dt = datetime.strptime(std_tweets[-1]['created_at'], '%a %b %d %H:%M:%S %z %Y')
        # self.last_text = std_tweets[0]['text']
        self.save_mark = 0
    def api_store_list(self):
        iter_save_mark = 0
        time_sn = 'saved_json' + time.ctime().replace(' ', '_').replace(':', '') + '.json'
        new_tweets = api.GetListTimeline(owner_screen_name=config['owner_screen_name'], slug=self.list_name, include_rts=True,
                                         count=200, return_json=True)

        # nt_q = deque(sorted(new_tweets, key=lambda x: datetime.strptime(x['created_at'], '%a %b %d %H:%M:%S %z %Y')))
        nt_l = sorted(new_tweets, key=lambda x: datetime.strptime(x['created_at'], '%a %b %d %H:%M:%S %z %Y'))
        # sorted_ds = [t["created_at"] for t in nt_l]
        # list.reverse(nt_l)
        new_last_index = 0
        for i, pj_dic in enumerate(nt_l):
            if datetime.strptime(pj_dic['created_at'], '%a %b %d %H:%M:%S %z %Y') == self.last_last_dt:
                # print('new: ' + pj_dic["text"] + ', Old: ' + self.last_text + '\n')
                new_last_index =  i 
                # iter_save_mark = self.save_mark
                # self.save_mark += i + 1
                break

        new_tbs = nt_l[new_last_index + 1:]
        print('new ones:(' + self.list_name + ') \n')
        for pj_dic in new_tbs:
            print(' ' + pj_dic['text'] + '\n')
            self.last_tweets_a[self.save_mark] = pj_dic
            self.save_mark += 1
            if self.save_mark == 100:
                st_name = self.saved_till.strftime("%Y-%b-%d_%H%M%S")
                last_last_dt_name = self.last_last_dt.strftime("%Y-%b-%d_%H%M%S")
                fd = open(os.path.abspath(data_path + self.list_name + "_" \
                                        + st_name + 'to' + last_last_dt_name + '.txt'), 'w')
                fd.write(json.dumps(list(self.last_tweets_a)))
                fd.close()
                print('saved {0} tweets from list {1} at'.format(self.save_mark, self.list_name) + time.ctime() + '\n')
                self.saved_till = self.last_last_dt
                self.save_mark = 0
                # self.last_tweets.clear()
        print('save_mark: (' + self.list_name + ') ' +  str(self.save_mark) + '\n')


        self.last_last_dt = datetime.strptime(nt_l[-1]['created_at'], '%a %b %d %H:%M:%S %z %Y')
        
        # if self.save_mark >= 120:
        #     st_name = self.saved_till.strftime("%Y-%b-%d_%H%M%S")
        #     last_last_dt_name = self.last_last_dt.strftime("%Y-%b-%d_%H%M%S")
        #     fd = open(os.path.abspath(data_path + self.list_name + "_" \
        #                               + st_name + 'to' + last_last_dt_name + '.txt'), 'w')
        #     fd.write(json.dumps(list(self.last_tweets)))
        #     fd.close()
        #     print('saved {0} tweets from list {1} at'.format(iter_save_mark, self.list_name) + time.ctime() + '\n')
        #     self.saved_till = self.last_last_dt
        #     self.save_mark = 0
        # new_tbs = nt_l[:new_last_index]
        # print('new ones:(' + self.list_name + ') \n')
        # for pj_dic in new_tbs:
        #     print(' ' + pj_dic['text'] + '\n')
        #     self.last_tweets.appendleft(pj_dic)




def api_store():
    last_tweets = {}
    for list_name in config['lists']:
        last_tweets[list_name] = deque([], 200)

    for list_name in config['lists']:
        time_sn = 'saved_json' + time.ctime().replace(' ', '_').replace(':', '') + '.txt'
        new_tweets = api.GetListTimeline(owner_screen_name=config['owner_screen_name'], slug=list_name, include_rts=True,
                                                     count=200, return_json=True)
        std_tweets = sorted(new_tweets, key=lambda x: datetime.strptime(x['created_at'], '%a %b %d %H:%M:%S %z %Y'))
        for tweet in std_tweets:
            last_tweets[list_name].appendleft(tweet)

        fd = open(os.path.abspath(data_path + list_name + "_" + time_sn), 'w')
        fd.write(json.dumps(list(last_tweets[list_name])))
        fd.close()
    saved_till = datetime.strptime(last_tweets[list_name][0]['created_at'], '%a %b %d %H:%M:%S %z %Y')
    last_last_dt = datetime.strptime(last_tweets[list_name][0]['created_at'], '%a %b %d %H:%M:%S %z %Y')
    save_mark = 0
    while 1:
        for list_name in config['lists']:
            time_sn = 'saved_json' + time.ctime().replace(' ', '_').replace(':', '') + '.txt'
            new_tweets = api.GetListTimeline(owner_screen_name=config['owner_screen_name'], slug=list_name, include_rts=True, count=200, return_json=True)

            nt_q = deque(sorted(new_tweets, key=lambda x: datetime.strptime(x['created_at'], '%a %b %d %H:%M:%S %z %Y')))
            nt_l = sorted(new_tweets, key=lambda x: datetime.strptime(x['created_at'], '%a %b %d %H:%M:%S %z %Y'))
            list.reverse(nt_l)
            for i, pj_dic in enumerate(nt_l):
                if datetime.strptime(pj_dic['created_at'], '%a %b %d %H:%M:%S %z %Y') == last_last_dt:
                    new_last_index = len(nt_l) - i - 1
                    save_mark += i + 1
                    break
                else:
                    new_last_index = len(nt_l)

            if save_mark >= 20:
                st_name = saved_till.strftime("%Y-%b-%d_%H%M%S")
                last_last_dt_name = last_last_dt.strftime("%Y-%b-%d_%H%M%S")
                fd = open(os.path.abspath(data_path + list_name + "_"\
                                          + st_name + 'to' + last_last_dt_name + '.txt'), 'w')
                fd.write(json.dumps(list(last_tweets[list_name])))
                fd.close()
                saved_till = last_last_dt
                save_mark = 0
            new_tbs = nt_l[:new_last_index]
            for pj_dic in new_tbs:
                last_tweets[list_name].appendleft(pj_dic)

            last_last_dt = datetime.strptime(new_tweets[0]['created_at'], '%a %b %d %H:%M:%S %z %Y')

        print(time.ctime())
        time.sleep(60)



def load_files(path):
    files =  os.listdir(path)



def last_get_list():
    last_tweets = {}
    for list_name in config['lists']:
        last_tweets[list_name] = {}
    for list_name in config['lists']:
        time_sn = 'saved_json' + time.ctime().replace(' ', '_').replace(':', '') + '.txt'
        last_tweets[list_name] = api.GetListTimeline(owner_screen_name=config['owner_screen_name'], slug=list_name, include_rts=True,
                                                     count=200, return_json=True)
        fd = open(os.path.abspath(data_path + list_name + "_" + time_sn), 'w')
        fd.write(json.dumps(last_tweets[list_name]))
        fd.close()
    print(time.ctime())
    time.sleep(240)
    to_flush = 1
    flush = False
    while 1:
        for list_name in config['lists']:
            time_sn = 'saved_json' + time.ctime().replace(' ', '_').replace(':', '') + '.txt'
            new_tweets = api.GetListTimeline(owner_screen_name=config['owner_screen_name'], slug=list_name, include_rts=True, count=200,
                                             return_json=True)
            print(len(new_tweets))

            temp_list = []
            dup = False
            for tweet in new_tweets:
                dic = last_tweets[list_name]
                for l_tweet in last_tweets[list_name]:
                    if tweet['id'] == l_tweet['id']:
                        dup = True
                        break
                if not dup: temp_list.append(tweet)
                dup = False
            if flush:
                last_tweets[list_name] = new_tweets
            else:
                last_tweets[list_name] += temp_list

            fd = open(os.path.abspath(data_path + list_name + "_" + time_sn), 'w')
            fd.write(json.dumps(temp_list))
            fd.close()
        to_flush += 1
        if to_flush % 5 != 0:
            flush = False
        else:
            flush = True

        print(time.ctime())
        time.sleep(20)



def get_list_tls():
    tlists = []

    for list_name in config['lists']:
        tlists.append(TList(list_name))
    for tlist in tlists:
        tlist.api_store_list()
    while 1:
        for tlist in tlists:
            tlist.api_store_list()
        print('checked at' + time.ctime())
        time.sleep(100)


def load_config(config_file, path):
    global config
    global data_path
    with open(config_file) as cf:
        config = json.load(cf)[0]
    data_path = path
    
    
def create_api_instance():
    global CONSUMER_KEY
    global CONSUMER_SECRET
    global ACCESS_TOKEN
    global ACCESS_TOKEN_SECRET

    CONSUMER_KEY = config['auth']['CONSUMER_KEY']
    CONSUMER_SECRET = config['auth']['CONSUMER_SECRET']
    ACCESS_TOKEN = config['auth']['ACCESS_TOKEN']
    ACCESS_TOKEN_SECRET = config['auth']['ACCESS_TOKEN_SECRET']

    global api
    api = twitter.Api(consumer_key=CONSUMER_KEY,
                      consumer_secret=CONSUMER_SECRET,
                      access_token_key=ACCESS_TOKEN,
                      access_token_secret=ACCESS_TOKEN_SECRET)


