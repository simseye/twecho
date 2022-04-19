import tweepy
import requests

def db_args_mysql(args, config):
    db_args = {}
    if(args.password):
        db_args['passwd'] = args.password
    elif(config.get("db_args") is not None):
        if(config.get('db_args').get('db_password')):
            db_args['passwd'] = config['db_args']["db_password"]

    if(args.db_username):
        db_args['user'] = args.db_user
    elif(config.get("db_args").get('db_user') is not None):
        db_args['user'] = config['db_args']["db_user"]

    if(args.db_host):
        db_args['host'] = args.db_host
    elif(config.get('db_args').get('db_host') is not None):
        db_args['host'] = config['db_args']["db_host"]

    if(args.db_name):
        db_args['database'] = args.db_host
    elif(config.get('db_args').get('db_name') is not None):
        db_args['database'] = config['db_args']["db_name"]
    else:
        db_args['database'] = 'twitterapi'

    return db_args


def db_args_postgres(args, config):
    global db_name
    db_args = {}
    if(args.password):
        db_args['password'] = args.password
    elif(config.get("db_args") is not None):
        if(config.get('db_args').get('db_password')):
            db_args['password'] = config['db_args']["db_password"]

    if(args.db_username):
        db_args['user'] = args.db_user
    elif(config.get("db_args").get('db_user') is not None):
        db_args['user'] = config['db_args']["db_user"]

    if(args.db_host):
        db_args['host'] = args.db_host
    elif(config.get('db_args').get('db_host') is not None):
        db_args['host'] = config['db_args']["db_host"]

    if(args.db_name):
        db_name = args.db_name
    elif(config.get('db_args').get('db_name') is not None):
        db_name = config['db_args']["db_name"]
    else:
        db_name = 'twitterapi'

    return db_args


def set_db(args, config):
    global db
    global db_args
    
    if(args.db_server == 'mysql' or (config.get('db_server') is not None and config['db_server'] == 'mysql')):
        db_args = db_args_mysql(args, config)
        from . import db_mysql as db
    else:
        db_args = db_args_postgres(args, config)
        from . import db_postgres as db


def set_apiv2(config):
    global user_fields
    global tweet_fields
    global place_fields
    global media_fields
    global expansions
    global apiv2
    user_fields = ["created_at", "description", "entities", "id", "location", "name",
                    "pinned_tweet_id","profile_image_url", "protected", "url",
                    "username", "verified", "withheld", "public_metrics"]
    tweet_fields = ["attachments", "author_id", "conversation_id", "created_at",
                    "id", "lang", "possibly_sensitive", "referenced_tweets",
                    "source", "text", "withheld", "public_metrics",
                    "in_reply_to_user_id", "geo", 
                    "context_annotations", "entities" ]
    media_fields = [ "duration_ms", "height", "media_key", "preview_image_url",
                    "type", "url", "width", "public_metrics", "non_public_metrics",
                    "organic_metrics", "promoted_metrics", "alt_text"]
    place_fields = ["contained_within", "country", "country_code", "full_name",
                    "geo", "id", "name", "place_type"]
    expansions = [ 'attachments.poll_ids', 'attachments.media_keys', 'author_id',
                    'entities.mentions.username', 'geo.place_id', 'in_reply_to_user_id',
                    'referenced_tweets.id', 'referenced_tweets.id.author_id']

    apiv2 = tweepy.Client(config['auth']['BEARER_TOKEN'],
             config['auth']['CONSUMER_KEY'],
             config['auth']['CONSUMER_SECRET'],
             config['auth']['ACCESS_TOKEN'],
             config['auth']['ACCESS_TOKEN_SECRET'],
             return_type = requests.Response,
             wait_on_rate_limit=False)

def set_apiv1(config):
    global api
    auth = tweepy.OAuthHandler(config['auth']['CONSUMER_KEY'], config['auth']['CONSUMER_SECRET'])
    auth.set_access_token(config['auth']['ACCESS_TOKEN'], config['auth']['ACCESS_TOKEN_SECRET'])
    api = tweepy.API(auth, wait_on_rate_limit=False)