


from __future__ import print_function
import twitter
import os
import json
import time
import mysql.connector
from datetime import datetime
from twitter.error import TwitterError
import calendar

#use prepared statements or parameters
#tweets = [tdic['text'] for tdic in jarray]
#sizes = [len(tweet) for tweet in tweets]
#tweetse = [tweet.encode('utf-8') for tweet in tweets]
#sizes_b = [len(tweet) for tweet in tweetse]
#n_tweets = len(jarray)

# cnx.cmd_query('alter table jsont drop column created_at')
# cnx.cmd_query('alter table jsont add column created_at datetime')

def create_tweets():
    cnx.cmd_query('create table if not exists tweets(\
               created_at datetime,\
               id bigint,\
               text varchar(300) character set utf8mb4 ,\
               truncated bool,\
               screen_name varchar(15) character set utf8mb4,\
               retweet_count int,\
               favorite_count int,\
               reply_count int,\
               source varchar(100),\
               user_id bigint,\
               in_reply_to_status_id bigint,\
               in_reply_to_user_id bigint,\
               in_reply_to_screen_name varchar(15),\
               coordinates_long decimal(9,7),\
               coordinates_lat decimal(9,7),\
               is_quote_status bool,\
               quoted_status_id bigint,\
               quoted_status varchar(280),\
               rewteeted_id bigint,\
               favorited bool,\
               retweeted bool,\
               lang varchar(6),\
               primary key(id))')

def create_users():
    cnx.cmd_query('create table if not exists users(\
               created_at datetime,\
               id bigint,\
               name char(50) character set utf8mb4,\
               screen_name char(15) character set utf8mb4,\
               location varchar(100) character set utf8mb4,\
               description varchar(280) character set utf8mb4,\
               url char(24),\
               protected bool,\
               followers_count int,\
               friends_count int,\
               favourites_count int,\
               utc_offset int,\
               geo_enabled bool,\
               time_zone int,\
               verified bool,\
               statuses_count int,\
               lang char(6),\
               contributors_enabled bool,\
               is_translator bool,\
               is_translation_enabled bool,\
               profile_background_color char(6),\
               profile_background_image_url varchar(300),\
               profile_background_image_url_https varchar(300),\
               profile_background_tile bool,\
               profile_image_url varchar(300),\
               profile_image_url_https varchar(300),\
               profile_banner_url varchar(300),\
               profile_link_color char(6),\
               profile_sidebar_border_color char(6),\
               profile_sidebar_fill_color char(6),\
               profile_text_color char(6),\
               profile_use_background_image bool,\
               has_extended_profile bool,\
               default_profile bool,\
               default_profile_image bool,\
               following bool,\
               follow_request_sent bool,\
               notifications bool,\
               translator_type char(30),\
               primary key(id))')


def create_enhanced_urls():
    cnx.cmd_query('create table if not exists urls_enhanced(\
                     id bigint auto_increment,\
                     index_begin smallint,\
                     index_end smallint,\
                     url_short varchar(50),\
                     expanded_url varchar(2083),\
                     display_url varchar(50),\
                     unwound_url varchar(2083),\
                     unwound_status int,\
                     unwound_title varchar(60),\
                     unwound_description varchar(2083),\
                     primary key (id)\
                     )')


def create_urls():
    cnx.cmd_query('create table if not exists urls(\
                     id bigint auto_increment,\
                     index_begin smallint,\
                     index_end smallint,\
                     url_short varchar(50),\
                     expanded_url varchar(2083),\
                     display_url varchar(50),\
                     primary key (id)\
                     )')
    cnx.cmd_query('create table if not exists urls_tid(\
                    tweet_id bigint,\
                    url_id bigint,\
                    foreign key(tweet_id) references tweets(id),\
                    foreign key(url_id) references urls(id),\
                    primary key(url_id, tweet_id)\
                  )')



def create_med_size():
    cnx.cmd_query('create table if not exists media_size(\
                   thumb_h int,\
                   thumb_w int,\
                   thumb_resize varchar(5),\
                   large_h int,\
                   large_resize varchar(5),\
                   large_w varchar(5),\
                   medium_h int,\
                   medium_resize varchar(5),\
                   medium_w int,\
                   size_medium int,\
                   small_h int,\
                   small_resize varchar(5),\
                   small_w int,\
                   media_id bigint,\
                   foreign key(media_id) references media(id)\
                   )')


def create_media():
    cnx.cmd_query('create table if not exists media(\
                index_begin smallint,\
                index_end smallint,\
                url varchar(50),\
                media_url varchar(200),\
                display_url varchar(50),\
                id bigint,\
                expanded_url varchar(2083),\
                media_url_https varchar(201),\
                primary key (id)\
                )')
    create_med_size()
    cnx.cmd_query('create table if not exists media_tid(\
                   tweet_id bigint,\
                   media_id bigint,\
                   foreign key(media_id) references media(id),\
                   foreign key(tweet_id) references tweets(id),\
                   primary key (media_id, tweet_id )\
            )')



def create_users_mentions():
    cnx.cmd_query('create table if not exists user_mentions(\
                  name varchar(50),\
                  index_begin smallint,\
                  index_end smallint,\
                  screen_name varchar(15),\
                  id bigint,\
                  primary key (id)\
                  )')


def create_hashtags():
    cnx.cmd_query('create table if not exists hashtags(\
                   id bigint auto_increment,\
                   index_begin smallint,\
                   index_end smallint,\
                   text varchar(280),\
                   primary key (id)\
                   )')
    cnx.cmd_query('create table if not exists hashtag_tweet(\
                   hashtag_id bigint,\
                   tweet_id bigint,\
                   foreign key(tweet_id) references tweets(id),\
                   foreign key(hashtag_id) references hashtags(id),\
                   primary key(hashtag_id, tweet_id)\
                   )')


def create_rt_tweet():
    cnx.cmd_query('create table if not exists rt_tweet(\
                   retweet_id bigint,\
                   tweet_id bigint,\
                   foreign key(tweet_id) references tweets(id),\
                   foreign key(retweet_id) references tweets(id),\
                   primary key(retweet_id, tweet_id)\
                   )')


def create_qt_tweet():
    cnx.cmd_query('create table if not exists qt_tweet(\
                   quote_tweet_id bigint,\
                   tweet_id bigint,\
                   foreign key(tweet_id) references tweets(id),\
                   foreign key(quote_tweet_id) references tweets(id),\
                   primary key(quote_tweet_id, tweet_id)\
                   )')

def create_place():
    cnx.cmd_query('create table if not exists place(\
                    tweet_id bigint,\
                    id varchar(20),\
                    url varchar(100),\
                    place_type varchar(10),\
                    city_name varchar(80),\
                    full_name varchar(100),\
                    country_code char(2),\
                    country varchar(60),\
                    bb_a_long decimal(9,6),\
                    bb_a_lat decimal(9,6),\
                    bb_b_long decimal(9,6),\
                    bb_b_lat decimal(9,6),\
                    bb_c_long decimal(9,6),\
                    bb_c_lat decimal(9,6),\
                    bb_d_long decimal(9,6),\
                    bb_d_lat decimal(9,6),\
                    bb_type varchar(20),\
                    foreign key(tweet_id) references tweets(id),\
                    primary key(tweet_id, id)\
                    )')
                    



def insert_tweets(jarray):
    for tj_dic in jarray:
        tweet_dt = datetime.strptime(tj_dic['created_at'], '%a %b %d %H:%M:%S %z %Y')
        ddb_dt = tweet_dt.strftime("%Y-%b-%d %H:%M:%S")
        ddb_dt = ddb_dt[:5] + str(tweet_dt.month) + ddb_dt[8:]

        if tj_dic.get('in_reply_to_screen_name', None):
            irtsn = '"' + str(tj_dic['in_reply_to_screen_name'].replace('"', '\\"')) + '"'
        else:
            irtsn = "NULL"

        if tj_dic.get('place', None):
            place = '"' + str(tj_dic['place']) + '"'
        else:
            place = "NULL"

        if tj_dic.get('coordinates', None):
            coords_long = tj_dic['coordinates']['coordinates'][0] 
            coords_lat = tj_dic['coordinates']['coordinates'][1] 
        else:
            coords_long = "NULL"
            coords_lat = "NULL"

        if tj_dic.get('source', None):
            source= '"' + str(tj_dic['source'].replace('"', '\\"')) + '"'
        else:
            source = "NULL"

        insert_string = ''
        insert_string = \
            'insert ignore into tweets (\
            created_at,\
            id,\
            text,\
            screen_name,\
            retweet_count,\
            favorite_count ,\
            source ,\
            user_id ,\
            in_reply_to_status_id,\
            in_reply_to_user_id ,\
            in_reply_to_screen_name ,\
            coordinates_long ,\
            coordinates_lat,\
            is_quote_status,\
            quoted_status_id,\
            favorited ,\
            retweeted ,\
            lang, \
            reply_count )\
            values("{0}", {1}, "{2}", "{3}", {4},\
            {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12}, {13}, {14}, {15}, {16}, "{17}", {18})'.format(
                ddb_dt,
                int(tj_dic['id_str']),
                tj_dic['text'].replace('"', '\\"'),
                tj_dic['user']['screen_name'].replace('"', '\\"'),
                tj_dic.get('retweet_count', "NULL"),
                tj_dic.get('favorite_count', "NULL"),
                source,
                tj_dic['user']['id'],
                tj_dic.get('in_reply_to_status_id', None) if tj_dic.get('in_reply_to_status_id', None) else "NULL",
                tj_dic.get('in_reply_to_user_id', None) if tj_dic.get('in_reply_to_user_id', None) else "NULL",
                irtsn,
                coords_long,
                coords_lat,
                tj_dic.get('is_quote_status', "NULL"),
                tj_dic.get('quoted_status_id', "NULL"),
                tj_dic.get('favorited', "NULL"),
                tj_dic.get('retweeted', "NULL"),
                tj_dic['lang'],
                tj_dic.get('reply_count', "NULL")
            )
        cnx.cmd_query(insert_string)

        if  tj_dic.get('is_quote_status', None):
            # depth += 1
            # if depth > 5: return
            # try:

            #     insert_tweet( api.GetStatus(status_id=tj_dic['quoted_status_id']).AsDict(), depth)
            # except TwitterError:
            #     pass
            insert_qt_tweet(tj_dic['quoted_status_id'], int(tj_dic['id_str']))
        if tj_dic.get('retweeted_status', None) is not None:
            retweet_id = tj_dic['retweeted_status']['id']
            # insert_tweet(tj_dic['retweeted_status'], 1)
            insert_rt_tweet(retweet_id, int(tj_dic['id_str']))

def insert_users(jarray):
    for  tj_dic in jarray:
        tweet_dtu = datetime.strptime(tj_dic['created_at'], '%a %b %d %H:%M:%S %z %Y' )
        ddb_dtu = tweet_dtu.strftime("%Y-%b-%d %H:%M:%S")
        ddb_dtu = ddb_dtu[:5] +  str(tweet_dtu.month) + ddb_dtu[8:]
        instr_user= \
            'insert ignore into  users(\
               created_at ,\
               id ,\
               name ,\
               screen_name ,\
               location ,\
               description ,\
               url ,\
               protected ,\
               followers_count ,\
               friends_count ,\
               favourites_count ,\
               utc_offset ,\
               geo_enabled ,\
               time_zone ,\
               verified ,\
               statuses_count ,\
               lang ,\
               contributors_enabled ,\
               is_translator ,\
               is_translation_enabled ,\
               profile_background_color ,\
               profile_background_image_url ,\
               profile_background_image_url_https ,\
               profile_background_tile ,\
               profile_image_url ,\
               profile_image_url_https ,\
               profile_banner_url ,\
               profile_link_color ,\
               profile_sidebar_border_color ,\
               profile_sidebar_fill_color ,\
               profile_text_color ,\
               profile_use_background_image ,\
               has_extended_profile ,\
               default_profile ,\
               default_profile_image ,\
               following ,\
               follow_request_sent ,\
               notifications ,\
               translator_type)\
           values( "{0}", "{1}", "{2}", "{3}", "{4}", "{5}", "{6}", {7}, {8}, {9}, {10},\
            {11}, {12}, {13}, {14}, {15}, "{16}", {17}, {18}, {19}, "{20}", "{21}",\
             "{22}", {23}, "{24}", "{25}", "{26}", "{27}", "{28}", "{29}", "{30}", {31}, {32},\
              {33}, {34}, {35}, {36}, {37}, "{38}")'.format(
                ddb_dtu,
                tj_dic['user']['id'] if tj_dic['user']['id'] else "NULL",
                tj_dic['user']['name'].replace('"', '\\"') if tj_dic['user']['name'].replace('"', '\\"') else "NULL",
                tj_dic['user']['screen_name'].replace('"', '\\"') if tj_dic['user']['screen_name'].replace('"', '\\"') else "NULL",
                tj_dic['user']['location'].replace('"', '\\"') if tj_dic['user']['location'] else "NULL",
                tj_dic['user']['description'].replace('"', '\\"') if tj_dic['user']['description'].replace('"', '\\"') else "NULL",
                tj_dic['user']['url'] if tj_dic['user']['url'] else "NULL",
                tj_dic['user']['protected'] if tj_dic['user']['protected'] else "NULL",
                tj_dic['user']['followers_count'] if tj_dic['user']['followers_count'] else "NULL",
                tj_dic['user']['friends_count'] if tj_dic['user']['friends_count'] else "NULL",
                tj_dic['user']['favourites_count'] if tj_dic['user']['favourites_count'] else "NULL",
                tj_dic['user']['utc_offset'] if tj_dic['user']['utc_offset'] else "NULL",
                tj_dic['user']['geo_enabled'] if tj_dic['user']['geo_enabled'] else "NULL",
                tj_dic['user']['time_zone'] if tj_dic['user']['time_zone'] else "NULL",
                tj_dic['user']['verified'] if tj_dic['user']['verified'] else "NULL",
                tj_dic['user']['statuses_count'] if tj_dic['user']['statuses_count'] else "NULL",
                tj_dic['user']['lang'] if tj_dic['user']['lang'] else "NULL",
                tj_dic['user']['contributors_enabled'] if tj_dic['user']['contributors_enabled'] else "NULL",
                tj_dic['user']['is_translator'] if tj_dic['user']['is_translator'] else "NULL",
                tj_dic['user']['is_translation_enabled'] if tj_dic['user']['is_translation_enabled'] else "NULL",
                tj_dic['user']['profile_background_color'] if tj_dic['user']['profile_background_color'] else "NULL",
                tj_dic['user']['profile_background_image_url'] if tj_dic['user']['profile_background_image_url'] else "NULL",
                tj_dic['user']['profile_background_image_url_https'] if tj_dic['user'][
                    'profile_background_image_url_https'] else "NULL",
                tj_dic['user']['profile_background_tile'] if tj_dic['user']['profile_background_tile'] else "NULL",
                tj_dic['user']['profile_image_url'] if tj_dic['user']['profile_image_url'] else "NULL",
                tj_dic['user']['profile_image_url_https'] if tj_dic['user']['profile_image_url_https'] else "NULL",
                tj_dic['user'].get('profile_banner_url', "NULL"),
                tj_dic['user']['profile_link_color'] if tj_dic['user']['profile_link_color'] else "NULL",
                tj_dic['user']['profile_sidebar_border_color'] if tj_dic['user']['profile_sidebar_border_color'] else "NULL",
                tj_dic['user']['profile_sidebar_fill_color'] if tj_dic['user']['profile_sidebar_fill_color'] else "NULL",
                tj_dic['user']['profile_text_color'] if tj_dic['user']['profile_text_color'] else "NULL",
                tj_dic['user']['profile_use_background_image'] if tj_dic['user']['profile_use_background_image'] else "NULL",
                tj_dic['user']['has_extended_profile'] if tj_dic['user']['has_extended_profile'] else "NULL",
                tj_dic['user']['default_profile'] if tj_dic['user']['default_profile'] else "NULL",
                tj_dic['user']['default_profile_image'] if tj_dic['user']['default_profile_image'] else "NULL",
                tj_dic['user']['following'] if tj_dic['user']['following'] else "NULL",
                tj_dic['user']['follow_request_sent'] if tj_dic['user']['follow_request_sent'] else "NULL",
                tj_dic['user']['notifications'] if tj_dic['user']['notifications'] else "NULL",
                tj_dic['user']['translator_type'] if tj_dic['user']['translator_type'] else "NULL"
            )

        cnx.cmd_query( instr_user)


def insert_urls(jarray):
    for i, tj_dic in enumerate(jarray):
        for url in tj_dic['entities']['urls']:
            url_string =\
            'insert ignore into  urls(\
            index_begin,\
            index_end,\
            url_short,\
            expanded_url,\
            display_url)\
            values({0}, {1}, "{2}", "{3}", "{4}")'\
            .format(
            url['indices'][0],
            url['indices'][1],
            url['url'],
            url['expanded_url'],
            url['display_url'])

            cnx.cmd_query(url_string)

            url_tid_string =\
            'insert ignore into  urls_tid( \
            tweet_id, \
            url_id) \
            values({0}, last_insert_id());'\
            .format(
            int(tj_dic['id_str'])
            )
            cnx.cmd_query(url_tid_string)



def insert_media(jarray):
        # month_adig = {name: i for i, name in enumerate(calendar.month_abbr)}
    for i, tj_dic in enumerate(jarray):
        for media in tj_dic['entities'].get('media', []):
            med_size_string =\
                'insert ignore into  media_size(\
                 thumb_h,\
                 thumb_w,\
                 thumb_resize,\
                 large_h,\
                 large_resize,\
                 large_w,\
                 medium_h ,\
                 medium_resize,\
                 medium_w ,\
                 small_h ,\
                 small_resize,\
                 small_w \
                 )\
                values({0},{1},"{2}",{3},"{4}",{5},{6},"{7}",{8},{9},"{10}",{11})'.\
                format(
                media['sizes']['thumb']['h'],
                media['sizes']['thumb']['w'],
                media['sizes']['thumb']['resize'],
                media['sizes']['large']['h'],
                media['sizes']['large']['resize'],
                media['sizes']['large']['w'],
                media['sizes']['medium']['h'],
                media['sizes']['medium']['resize'],
                media['sizes']['medium']['w'],
                media['sizes']['small']['h'],
                media['sizes']['small']['resize'],
                media['sizes']['small']['w']
                )
            cnx.cmd_query(med_size_string)


            media_string=\
                'insert ignore into  media(\
                index_begin ,\
                index_end ,\
                url ,\
                media_url ,\
                display_url ,\
                id ,\
                expanded_url ,\
                media_url_https \
                )\
                values({0},{1},"{2}","{3}","{4}",{5},"{6}","{7}")'.format(
                media['indices'][0],
                media['indices'][1],
                media['url'],
                media['media_url'],
                media['display_url'],
                media['id'],
                media['expanded_url'],
                media['media_url_https']
                )
            cnx.cmd_query(media_string)

            media_tid_string =\
                'insert ignore into  media_tid(\
                tweet_id,\
                media_id \
                )\
                values({0}, {1})'.format(
                int(tj_dic['id_str']),
                media['id']
                )
            cnx.cmd_query(media_tid_string)


def insert_hashtags(jarray):
    for i, tj_dic in enumerate(jarray):
        for hashtag in tj_dic['entities'].get('hashtag', []):
            ht_string =\
                'insert ignore into  hashtags(\
                index_begin ,\
                index_end ,\
                text ,\
                )\
                values({0}, {1}, {2})'.format(
                hashtag['indices'][0],
                hashtag['indices'][1],
                hashtag['text']
                )
            cnx.cmd_query(ht_string)

            hashtag_tweet=\
                'replace hashtag_tweet(\
                hashtag_id ,\
                tweet_id ,\
                )\
                values(last_insert_id(), {0})'.format(
                int(tj_dic['id_str'])
                )
            cnx.cmd_query(hashtag_tweet)


def insert_rt_tweet(rt_id, t_id):
    rt_tweet_s=\
        'insert ignore into  rt_tweet(\
        retweet_id ,\
        tweet_id \
        )\
        values({0}, {1})'.format(
        rt_id,
        t_id
        )
    cnx.cmd_query(rt_tweet_s)

def insert_qt_tweet(qt_id, t_id):
    qt_tweet_s=\
        'insert ignore into  qt_tweet(\
        quote_tweet_id ,\
        tweet_id \
        )\
        values({0}, {1})'.format(
        qt_id,
        t_id
        )
    cnx.cmd_query(qt_tweet_s)

def insert_place(jarray):
    for tj_dic in jarray:
        if tj_dic['place'] is not None:
            insert_place =\
                'insert ignore into place(\
                    tweet_id,\
                    id,\
                    url,\
                    place_type,\
                    city_name,\
                    full_name,\
                    country_code,\
                    country,\
                    bb_a_long ,\
                    bb_a_lat ,\
                    bb_b_long,\
                    bb_b_lat ,\
                    bb_c_long,\
                    bb_c_lat ,\
                    bb_d_long,\
                    bb_d_lat,\
                    bb_type)\
                values({0}, "{1}", "{2}", "{3}", "{4}",\
                    "{5}", "{6}", "{7}", {8}, {9}, {10},\
                    {11}, {12}, {13}, {14}, {15}, "{16}")'.\
                    format(
                        int(tj_dic['id_str']),
                        tj_dic['place']['id'],
                        tj_dic['place']['url'],
                        tj_dic['place']['place_type'],
                        tj_dic['place']['name'],
                        tj_dic['place']['full_name'],
                        tj_dic['place']['country_code'],
                        tj_dic['place']['country'],
                        tj_dic['place']['bounding_box']['coordinates'][0][0][0],
                        tj_dic['place']['bounding_box']['coordinates'][0][0][1],
                        tj_dic['place']['bounding_box']['coordinates'][0][1][0],
                        tj_dic['place']['bounding_box']['coordinates'][0][1][1],
                        tj_dic['place']['bounding_box']['coordinates'][0][2][0],
                        tj_dic['place']['bounding_box']['coordinates'][0][2][1],
                        tj_dic['place']['bounding_box']['coordinates'][0][3][0],
                        tj_dic['place']['bounding_box']['coordinates'][0][3][1],
                        tj_dic['place']['bounding_box']['type']
                    )
            cnx.cmd_query(insert_place)

def load_file(path):
    api_s = open(path, 'r', encoding='utf-8')
    jstring = api_s.read()
    if jstring != "":
        j_array = json.loads(jstring)
    else:
        return
    insert_tweets(j_array)
    cnx.commit()
    insert_users(j_array)
    cnx.commit()
    insert_urls(j_array)
    cnx.commit()
    insert_media(j_array)
    cnx.commit()
    insert_hashtags(j_array)
    cnx.commit()
    insert_place(j_array)

    cnx.commit()

def initializedb(password):
    global cnx
    cnx = mysql.connector.connect(user='root', passwd=password, host='127.1.1.1', database='twitterapi', charset='utf8mb4')
    cursor = cnx.cursor(buffered=True)

    create_tweets()
    create_users()
    create_media()
    create_med_size()
    create_enhanced_urls()
    create_hashtags()
    create_qt_tweet()
    create_rt_tweet()
    create_urls()
    create_users_mentions()
    create_place()
    cnx.commit()

def disconnect():
    cnx.disconnect()

