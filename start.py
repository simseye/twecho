from pydb import twitter_db
from pydb import twitter_api
# import GetOldTweets3 as got
from pydb import twitter_db
import shutil
from pathlib import Path
import twitter
import json
import argparse

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
                      access_token_secret=ACCESS_TOKEN_SECRET)
    return api
def load_config(config_file, path):
    global config
    global data_path
    with open(config_file) as cf:
        config = json.load(cf)[0]
    data_path = path

def options():
    ap = argparse.ArgumentParser(prog="twitter-archiver",
                                 usage="python3 %(prog)s [options]",
                                 description="Archive tweets from home timeline and lists to mysql")
    ap.add_argument("-p", "--password", help="password of mysql db")

    args = ap.parse_args()

    return args


def main():
    args = options()
    db_password = args.password

    destination_p = Path.joinpath(Path(__file__).parent.parent, "twitter-data")
    destination_pa = Path.joinpath(Path(__file__).parent.parent, "twitter-archive")
    path_config =  Path.joinpath(Path(__file__).parent, "pydb_config.txt")

    load_config(path_config, destination_p)
    api = create_api_instance()
    twitter_api.get_list_tls(api, config, destination_p, db_password)


    # for f in listdir(source_p):
    #     f_p = source_p + '\\' + f
    #     # isfile(join(source_p, f))
    #     shutil.move(f_p, destination_p)

if(__name__ == "__main__"):
    main()
