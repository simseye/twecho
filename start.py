#! /usr/bin/env python
from pydb import twitter_api, utilities
# import GetOldTweets3 as got
from pathlib import Path
import twitter
import json
import argparse
import os
from pydb import settings as s
import twint

def create_api_instance():
    """
    json format: {
        "lists": [
        ],
        "auth": {
            "CONSUMER_KEY": "",
            "CONSUMER_SECRET": "",
            "ACCESS_TOKEN": "",
            "ACCESS_TOKEN_SECRET": "",
            "BEARER_TOKEN": ""
        },
        "owner_screen_name": "",
        "db_server": "postgres or mysql",
        "db_args": {
            "db_user": "",
            "db_password": "",
            "db_host": ""
        },
        "users_track": [

            {
                "screen_name": "",
                "id_str": ""
            }
        ],
        "data_dir": "",
    
        "only_users": false
    }
    
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


def load_config(config_file):
    global config
    with open(config_file) as cf:
        config = json.load(cf)


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
    ap.add_argument('-c', '--config_file', help='config file path')
    args = ap.parse_args()

    return args


def main():

    args = options()
    file_path = os.getcwd()
    # destination_p = Path.joinpath(Path(file_path).parent, "twitter-data")
    destination_pa = Path.joinpath(Path(file_path).parent, "twitter-archive")
    path_config =  Path.joinpath(Path(file_path), args.config_file)
    # destination_user_tl = Path.joinpath( destination_p , "user-tls")

    load_config(path_config )

    destination_p = Path(config['data_dir'])

    s.set_db(args, config)

    if(config.get("users_track", None) is not None):
        users = config["users_track"]
    else: users = []

    # api = create_api_instance()
    s.set_apiv1(config)
    # user = s.api.verify_credentials()
    s.set_apiv2(config)
    s.db.initialize_db()
    # utilities.deduplicate_urls()
    # utilities.check_after_url_dup()
    # utilities.deduplicate_place()
    # utilities.clone_lists()
    # get_twint()
    user_timeline = twitter_api.get_user_timeline
    if(config['only_users']):
        twitter_api.get_list_tls2( config, destination_p, users, user_timeline)
    else:
        twitter_api.get_list_tls( config, destination_p, users, user_timeline)
    
    # twitter_api.get_user_timeline(api, destination_user_tl, users)

    # for f in listdir(source_p):
    #     f_p = source_p + '\\' + f
    #     # isfile(join(source_p, f))
    #     shutil.move(f_p, destination_p)


def get_twint():
    c = twint.Config()
    c.username = ''
    c.Limit = 20
    twint.run.Profile(c)


if(__name__ == "__main__"):
    main()
