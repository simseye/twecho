#! /usr/bin/env python
from pydb import twitter_api, utilities
# import GetOldTweets3 as got
from pathlib import Path
import twitter
import json
import argparse
import os
from pydb import settings as s

def create_api_instance():
    """
    json format: '[{"lists": [],
                    "auth": {"CONSUMER_KEY" : "",
                             "CONSUMER_SECRET":  "",
                             "ACCESS_TOKEN": "",
                             "ACCESS_TOKEN_SECRET": ""},
                    "owner_screen_name": ""}]'
    
    save json in config file as pydb_config.txt in root folder
    """
    global CONSUMER_KEY
    global CONSUMER_SECRET
    global ACCESS_TOKEN
    global ACCESS_TOKEN_SECRET

    CONSUMER_KEY = config['auth']['CONSUMER_KEY']
    CONSUMER_SECRET = config['auth']['CONSUMER_SECRET']
    ACCESS_TOKEN = config['auth']['ACCESS_TOKEN']
    ACCESS_TOKEN_SECRET = config['auth']['ACCESS_TOKEN_SECRET']

    
    api = twitter.Api(consumer_key=CONSUMER_KEY,
                      consumer_secret=CONSUMER_SECRET,
                      access_token_key=ACCESS_TOKEN,
                      access_token_secret=ACCESS_TOKEN_SECRET,
                      tweet_mode='extended')
    return api


def load_config(config_file, path):
    global config
    global data_path
    with open(config_file) as cf:
        config = json.load(cf)
    data_path = path


def options():
    ap = argparse.ArgumentParser(prog="twitter-archiver",
                                 usage="python3 %(prog)s [options]",
                                 description="Archive tweets from home timeline and lists to mysql")
    ap.add_argument("-p", "--password", help="password of mysql db")
    ap.add_argument('-dbu', '--db_username', help='database user name')
    ap.add_argument('-dbp', '--db_port', help='database port')
    ap.add_argument('-dbh', '--db_host', help='database host')
    ap.add_argument('-dbn', '--db_name', help="database name")
    ap.add_argument('-dbs', '--db_server', help='database server type: \"mysql\" or \"postgres\"' )
    args = ap.parse_args()

    return args


def main():

    file_path = os.getcwd()
    destination_p = Path.joinpath(Path(file_path).parent, "twitter-data")
    destination_pa = Path.joinpath(Path(file_path).parent, "twitter-archive")
    path_config =  Path.joinpath(Path(file_path), "pydb_config2.json")
    destination_user_tl = Path.joinpath( destination_p , "user-tls")

    load_config(path_config, destination_p)
    args = options()

    s.set_db(args, config)

    if(config.get("users_track", None) is not None):
        users = config["users_track"]
    else: users = []

    # api = create_api_instance()
    s.set_apiv1(config)
    s.set_apiv2(config)
    s.db.initialize_db()
    # utilities.deduplicate_urls()
    # utilities.check_after_url_dup()
    # utilities.deduplicate_place()
    # utilities.clone_lists()
    twitter_api.get_list_tls( config, destination_p, users, destination_user_tl)
    # twitter_api.get_user_timeline(api, destination_user_tl, users)

    # for f in listdir(source_p):
    #     f_p = source_p + '\\' + f
    #     # isfile(join(source_p, f))
    #     shutil.move(f_p, destination_p)

if(__name__ == "__main__"):
    main()
