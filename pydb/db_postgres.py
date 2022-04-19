import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from . import settings as s 
import json
from datetime import datetime

def create_tweets():
    cursor.execute("create table if not exists tweets(\
               created_at timestamp,\
               id bigint ,\
               tweet_text text  ,\
               screen_name varchar(20) ,\
               retweet_count int ,\
               favorite_count int ,\
               reply_count int ,\
               quote_count int ,\
               source varchar(150),\
               author_id bigint,\
               in_reply_to_status_id bigint ,\
               in_reply_to_user_id bigint ,\
               in_reply_to_screen_name varchar(20),\
               place_full_name varchar(40),\
               place_country varchar(60),\
               is_quote_status bool,\
               quoted_status_id bigint ,\
               quoted_status varchar(280),\
               retweeted_id bigint ,\
               favorited bool,\
               retweeted bool,\
               lang varchar(6),\
               conversation_id bigint ,\
               place_id varchar(20),\
               possibly_sensitive bool,\
               primary key(id))")


def create_users():
    cursor.execute("create table if not exists users(\
               created_at timestamp,\
               id bigint ,\
               name varchar(50) ,\
               screen_name varchar(20) ,\
               location varchar(150) ,\
               description varchar(280) ,\
               url varchar(65),\
               protected bool,\
               followers_count int ,\
               friends_count int ,\
               favourites_count int ,\
               listed_count int ,\
               verified bool,\
               statuses_count int ,\
               profile_image_url_https varchar(300),\
               profile_banner_url varchar(300),\
               default_profile bool,\
               default_profile_image bool,\
               following bool,\
               primary key(id))")


def create_enhanced_urls():
    cursor.execute('create table if not exists urls_enhanced(\
                     url_name varchar(25),\
                     index_begin smallint,\
                     index_end smallint,\
                     url_short varchar(50),\
                     expanded_url varchar(2083),\
                     display_url varchar(50),\
                     unwound_url varchar(2083),\
                     unwound_status int,\
                     unwound_title varchar(60),\
                     unwound_description varchar(2083),\
                     primary key (url_name)\
                     )')


def create_urls_deprecated():
    cursor.execute('create table if not exists urls(\
                     id bigint  auto_increment,\
                     index_begin smallint,\
                     index_end smallint,\
                     url_short varchar(50),\
                     expanded_url varchar(2083),\
                     display_url varchar(50),\
                     primary key (id)\
                     )')
    cursor.execute('create table if not exists urls_tid(\
                    tweet_id bigint ,\
                    url_id bigint ,\
                    foreign key(tweet_id) references tweets(id),\
                    foreign key(url_id) references urls(id),\
                    primary key(tweet_id, url_id)\
                  )')


def create_urls_new():
    cursor.execute('create table if not exists urls_new(\
                     url_name varchar(25) ,\
                     url_short varchar(50),\
                     expanded_url varchar(2500),\
                     display_url varchar(100),\
                     primary key (url_name)\
                     )')
    cursor.execute('create table if not exists urls_tid_new(\
                    tweet_id bigint ,\
                    url_name varchar(25),\
                    index_begin smallint,\
                    index_end smallint,\
                    foreign key(tweet_id) references tweets(id),\
                    foreign key(url_name) references urls_new(url_name),\
                    primary key(tweet_id, url_name )\
                  )')


def create_med_size():
    cursor.execute('create table if not exists media_size(\
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
                   media_id bigint ,\
                   foreign key(media_id) references media(id),\
                   primary key (media_id)\
                   )')


def create_media():
    cursor.execute("create table if not exists media(\
                media_type varchar(40),\
                media_key varchar(30),\
                index_begin smallint,\
                index_end smallint,\
                height smallint ,\
                width smallint ,\
                duration_ms int ,\
                preview_image_url varchar(100),\
                url varchar(50),\
                media_url varchar(200),\
                display_url varchar(50),\
                id bigint ,\
                expanded_url varchar(2083),\
                media_url_https varchar(2083),\
                met_playback_0 int ,\
                met_playback_25 int ,\
                met_playback_50 int ,\
                met_playback_75 int ,\
                met_playback_100 int ,\
                view_count int ,\
                alt_text text,\
                primary key (id)\
                )")
    create_med_size()
    cursor.execute('create table if not exists media_tid(\
                   tweet_id bigint ,\
                   media_id bigint ,\
                   media_key varchar(30),\
                   foreign key(tweet_id) references tweets(id),\
                   primary key (media_id, tweet_id )\
            )')



def create_users_mentions():
    cursor.execute('create table if not exists user_mentions(\
                  name varchar(50),\
                  index_begin smallint,\
                  index_end smallint,\
                  screen_name varchar(20),\
                  id bigint ,\
                  primary key (id)\
                  )')


def create_hashtags():
    cursor.execute("create table if not exists hashtags(\
                   tag varchar(140),\
                   primary key (tag)\
                   )")
    cursor.execute("create table if not exists hashtag_tweet(\
                   tag varchar(140),\
                   tweet_id bigint ,\
                   index_begin smallint,\
                   index_end smallint,\
                   foreign key(tweet_id) references tweets(id),\
                   foreign key(tag) references hashtags(tag),\
                   primary key(tag, tweet_id)\
                   )")


def create_rt_tweet():
    cursor.execute('create table if not exists rt_tweet(\
                   tweet_id bigint ,\
                   retweet_id bigint ,\
                   foreign key(tweet_id) references tweets(id),\
                   foreign key(retweet_id) references tweets(id),\
                   primary key(tweet_id, retweet_id)\
                   )')


def create_qt_tweet():
    cursor.execute('create table if not exists qt_tweet(\
                   quote_tweet_id bigint ,\
                   tweet_id bigint ,\
                   foreign key(tweet_id) references tweets(id),\
                   foreign key(quote_tweet_id) references tweets(id),\
                   primary key(quote_tweet_id, tweet_id)\
                   )')

def create_place_deprecated():
    cursor.execute('create table if not exists place(\
                    tweet_id bigint ,\
                    id varchar(20),\
                    url varchar(100),\
                    place_type varchar(10),\
                    city_name varchar(80),\
                    full_name varchar(100),\
                    country_code char(2),\
                    country varchar(60),\
                    centroid_long decimal(9,6),\
                    centroid_lat decimal(9,6),\
                    geo_coord_long decimal(9,6),\
                    geo_coord_lat decimal(9,6),\
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
                    

def create_place_new():
    cursor.execute("create table if not exists place_new(\
                    id varchar(20),\
                    url varchar(100),\
                    place_type varchar(20),\
                    city_name varchar(80),\
                    full_name varchar(100),\
                    country_code char(2),\
                    country varchar(60),\
                    centroid_long decimal(9,6),\
                    centroid_lat decimal(9,6),\
                    geo_coord_long decimal(9,6),\
                    geo_coord_lat decimal(9,6),\
                    bb_a_long decimal(9,6),\
                    bb_a_lat decimal(9,6),\
                    bb_b_long decimal(9,6),\
                    bb_b_lat decimal(9,6),\
                    bb_c_long decimal(9,6),\
                    bb_c_lat decimal(9,6),\
                    bb_d_long decimal(9,6),\
                    bb_d_lat decimal(9,6),\
                    bb_type varchar(20),\
                    primary key(id)\
                    )")

def create_place_tweet():
    cursor.execute("create table if not exists place_tweet(\
                    tweet_id bigint ,\
                    place_id varchar(20),\
                    foreign key(tweet_id) references tweets(id),\
                    foreign key(place_id) references place_new(id),\
                    primary key ( place_id, tweet_id)\
                    )")


def create_list_tags_users():
    cursor.execute("create table if not exists list_tags_users(\
                    user_id bigint ,\
                    tag_name varchar(45),\
                    foreign key (user_id) references users(id),\
                    foreign key(tag_name) references list_tags(tag_name),\
                    primary key(tag_name, user_id)\
                    )")


def create_list_tags():
    cursor.execute("create table if not exists list_tags(\
                    tag_name varchar(150),\
                    primary key(tag_name)\
                    )")


def create_context_annotations():
    cursor.execute("create table if not exists context_annotations(\
                    id smallint ,\
                    name varchar(100),\
                    description varchar(200),\
                    primary key(id)\
                        )")


def create_context_entities():
    cursor.execute("create table if not exists context_entities(\
                    id bigint ,\
                    name varchar(200),\
                    primary key (id)\
                        )")


def create_context_tweets():
    cursor.execute("create table if not exists context_tweets(\
                    tweet_id bigint ,\
                    annotation_id smallint ,\
                    entity_id bigint ,\
                    foreign key (tweet_id) references tweets(id),\
                    foreign key (annotation_id) references context_annotations(id),\
                    foreign key (entity_id) references context_entities(id),\
                    primary key (tweet_id, annotation_id, entity_id)\
                        )")


def insert_tweets(jarray):
    for tj_dic in jarray:
        tweet_dt = datetime.strptime(tj_dic['created_at'], '%a %b %d %H:%M:%S %z %Y')
        ddb_dt = tweet_dt.strftime("%Y-%b-%d %H:%M:%S")
        ddb_dt = ddb_dt[:5] + str(tweet_dt.month) + ddb_dt[8:]


        insert_string = \
            "insert  into tweets (\
            created_at,\
            id,\
            tweet_text,\
            screen_name,\
            retweet_count,\
            favorite_count ,\
            quote_count,\
            source ,\
            author_id ,\
            in_reply_to_status_id,\
            in_reply_to_user_id ,\
            in_reply_to_screen_name ,\
            is_quote_status,\
            quoted_status_id,\
            retweeted_id,\
            favorited ,\
            retweeted ,\
            lang, \
            reply_count,\
            place_id,\
            possibly_sensitive )\
            values(%s, %s, %s, %s, %s,\
                %s, %s, %s, %s, %s,\
                %s, %s, %s, %s, %s,\
                %s, %s, %s, %s, %s,\
                %s)\
            on conflict(id) do update set \
            created_at = %s,\
            id = %s,\
            tweet_text = %s,\
            screen_name = %s,\
            retweet_count = %s,\
            favorite_count  = %s,\
            quote_count = %s,\
            author_id = %s ,\
            favorited = %s ,\
            retweeted = %s ,\
            reply_count = %s"
        values = (
                ddb_dt,
                int(tj_dic['id_str']),
                tj_dic['full_text'],
                tj_dic['user']['screen_name'],
                tj_dic.get('retweet_count'),
                tj_dic.get('favorite_count'),
                tj_dic.get('quote_count'),
                tj_dic['source'],
                tj_dic['user']['id'],
                tj_dic.get('in_reply_to_status_id'),
                tj_dic.get('in_reply_to_user_id'),
                tj_dic['in_reply_to_screen_name'],
                tj_dic.get('is_quote_status'),
                tj_dic.get('quoted_status_id_str'),
                int(tj_dic["retweeted_status"]["id_str"]) if tj_dic.get('retweeted_status') else None,
                tj_dic.get('favorited'),
                tj_dic.get('retweeted'),
                tj_dic['lang'],
                tj_dic.get('reply_count'),
                tj_dic['place']['id'] if tj_dic.get('place') else None,
                tj_dic.get('possibly_sensitive'),

                ddb_dt,
                int(tj_dic['id_str']),
                tj_dic['full_text'],
                tj_dic['user']['screen_name'],
                tj_dic.get('retweet_count'),
                tj_dic.get('favorite_count'),
                tj_dic.get('quote_count'),
                tj_dic['user']['id'],
                tj_dic.get('favorited'),
                tj_dic.get('retweeted'),
                tj_dic.get('reply_count')
            )
        try:
            cursor.execute(insert_string, values)
        except psycopg2.errors.StringDataRightTruncation as err:
                print(f"expanded_url: {tj_dic['user']['screen_name']}\n\
                tweet: {tj_dic} \n" + str(err))
                exit()
        # if  tj_dic.get('is_quote_status', None):
            # depth += 1
            # if depth > 5: return
            # try:

            #     insert_tweet( api.GetStatus(status_id=tj_dic['quoted_status_id']).AsDict(), depth)
            # except TwitterError:
            #     pass
            # if tj_dic.get('quoted_status_id', False):
                # insert_qt_tweet(int(tj_dic['quoted_status_id_str']), int(tj_dic['id_str']))
        # if tj_dic.get('retweeted_status', None) is not None:
        #     retweet_id = int(tj_dic['retweeted_status']['id_str'])
        #     # insert_tweet(tj_dic['retweeted_status'], 1)
        #     insert_rt_tweet(retweet_id, int(tj_dic['id_str']))
        #     # conn.commit()


def insert_tweets_v2(tweets):

    for tweet in tweets:
        tweet_dt = datetime.strptime(tweet['created_at'], '%Y-%m-%dT%H:%M:%S.%fZ')
        ddb_dt = tweet_dt.strftime("%Y-%b-%d %H:%M:%S")
        ddb_dt = ddb_dt[:5] + str(tweet_dt.month) + ddb_dt[8:]
        ref_tweets = tweet.get('referenced_tweets')
        replied_to_id = None 
        retweeted_id = None
        quoted_id = None
        if(ref_tweets):
            for ref_tweet in ref_tweets:
                if(ref_tweet['type'] == 'replied_to'):
                    replied_to_id = ref_tweet['id'] 
                elif(ref_tweet['type'] == 'retweeted'):
                    retweeted_id = ref_tweet['id']
                elif(ref_tweet['type'] == 'quoted'):
                    quoted_id = ref_tweet['id']
        insert_string = \
            "insert into tweets (\
            created_at,\
            id,\
            tweet_text,\
            screen_name,\
            retweet_count,\
            favorite_count ,\
            quote_count,\
            source ,\
            author_id ,\
            in_reply_to_status_id,\
            in_reply_to_user_id ,\
            quoted_status_id,\
            retweeted_id,\
            lang, \
            reply_count,\
            place_id,\
            possibly_sensitive,\
            conversation_id )\
            values(%s, %s, %s, %s, %s,\
                %s, %s, %s, %s, %s,\
                %s, %s, %s, %s, %s,\
                %s, %s, %s)\
            on conflict(id) do update set \
            created_at = %s,\
            id = %s,\
            tweet_text = %s,\
            retweet_count = %s,\
            favorite_count  = %s,\
            quote_count = %s,\
            author_id = %s ,\
            reply_count = %s,\
            conversation_id = %s"
        values = (
                ddb_dt,
                tweet['id'],
                tweet['text'],
                tweet.get('username'),
                tweet.get('public_metrics').get('retweet_count'),
                tweet.get('public_metrics').get('like_count'),
                tweet.get('public_metrics').get('quote_count'),
                tweet['source'],
                tweet['author_id'],
                replied_to_id,
                tweet.get('in_reply_to_user_id'),
                quoted_id,
                retweeted_id,
                tweet['lang'],
                tweet.get('public_metrics').get('reply_count'),
                tweet.get('geo').get('place_id') if tweet.get('geo') else None,
                tweet['possibly_sensitive'],
                tweet['conversation_id'],

                ddb_dt,
                tweet['id'],
                tweet['text'],
                tweet.get('public_metrics').get('retweet_count'),
                tweet.get('public_metrics').get('favorite_count'),
                tweet.get('public_metrics').get('quote_count'),
                tweet['author_id'],
                tweet.get('public_metrics').get('reply_count'),
                tweet['conversation_id']
            )
        try:
            cursor.execute(insert_string, values)
        except psycopg2.errors.StringDataRightTruncation as err:
                print(f"expanded_url: {tweet['screen_name']}\n\
                tweet: {tweet} \n" + str(err))
                exit()



def insert_users(jarray):
    for  tj_dic in jarray:
        location = tj_dic['user']['location'] if len(tj_dic['user']['location']) > 0 else None
        tweet_dtu = datetime.strptime(tj_dic['user']['created_at'], '%a %b %d %H:%M:%S %z %Y' )
        ddb_dtu = tweet_dtu.strftime("%Y-%b-%d %H:%M:%S")
        ddb_dtu = ddb_dtu[:5] +  str(tweet_dtu.month) + ddb_dtu[8:]
        instr_user= \
            "insert  into  users(\
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
               listed_count,\
               verified ,\
               statuses_count ,\
               profile_image_url_https ,\
               profile_banner_url ,\
               default_profile ,\
               default_profile_image)\
           values( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,\
                    %s, %s, %s, %s, %s, %s, %s)\
            on conflict(id) do update set \
               created_at = %s ,\
               name = %s ,\
               screen_name  = %s,\
               location = %s ,\
               description = %s ,\
               url = %s ,\
               protected  = %s,\
               followers_count  = %s,\
               friends_count = %s ,\
               favourites_count = %s ,\
               listed_count = %s,\
               verified = %s ,\
               statuses_count  = %s,\
               profile_image_url_https = %s ,\
               profile_banner_url = %s ,\
               default_profile = %s ,\
               default_profile_image = %s"
        values = (
            ddb_dtu,
            tj_dic['user']['id'] ,
            tj_dic['user']['name'] ,
            tj_dic['user']['screen_name'],
            tj_dic['user']['location'],
            tj_dic['user']['description'],
            tj_dic['user']['url'] ,
            tj_dic['user']['protected'] ,
            tj_dic['user']['followers_count'] ,
            tj_dic['user']['friends_count'] ,
            tj_dic['user']['favourites_count'] ,
            tj_dic['user']['listed_count'],
            tj_dic['user']['verified'] ,
            tj_dic['user']['statuses_count'] ,
            tj_dic['user']['profile_image_url_https'] ,
            tj_dic['user'].get('profile_banner_url', "NULL"),
            tj_dic['user']['default_profile'] ,
            tj_dic['user']['default_profile_image'] ,

            ddb_dtu,
            tj_dic['user']['name'] ,
            tj_dic['user']['screen_name'],
            tj_dic['user']['location'] ,
            tj_dic['user']['description'],
            tj_dic['user']['url'] ,
            tj_dic['user']['protected'] ,
            tj_dic['user']['followers_count'] ,
            tj_dic['user']['friends_count'] ,
            tj_dic['user']['favourites_count'] ,
            tj_dic['user']['listed_count'],
            tj_dic['user']['verified'] ,
            tj_dic['user']['statuses_count'] ,
            tj_dic['user']['profile_image_url_https'] ,
            tj_dic['user'].get('profile_banner_url', "NULL"),
            tj_dic['user']['default_profile'] ,
            tj_dic['user']['default_profile_image'] ,
        )

        cursor.execute( instr_user, values)



def insert_users_v2(includes):
    if includes.get('users') is None:
        return
    for user in includes['users']:
        tweet_dtu = datetime.strptime(user['created_at'], '%Y-%m-%dT%H:%M:%S.%fZ')
        ddb_dtu = tweet_dtu.strftime("%Y-%b-%d %H:%M:%S")
        ddb_dtu = ddb_dtu[:5] +  str(tweet_dtu.month) + ddb_dtu[8:]
        instr_user= \
            "insert  into  users(\
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
               listed_count,\
               verified ,\
               statuses_count ,\
               profile_image_url_https)\
           values( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,\
                    %s, %s, %s)\
            on conflict(id) do  update set \
               created_at=%s,\
               name =%s,\
               screen_name =%s,\
               location =%s,\
               description =%s,\
               url =%s,\
               protected =%s,\
               followers_count =%s,\
               friends_count =%s,\
               listed_count = %s,\
               verified =%s,\
               statuses_count =%s,\
               profile_image_url_https =%s"
        values = (
            ddb_dtu,
            user['id'] ,
            user['name'] ,
            user['username'],
            user.get('location') ,
            user['description'],
            user['url'] ,
            user['protected'] ,
            user['public_metrics']['followers_count'] ,
            user['public_metrics']['following_count'] ,
            user['public_metrics']['listed_count'],
            user['verified'] ,
            user['public_metrics']['tweet_count'] ,
            user['profile_image_url'],

            ddb_dtu,
            user['name'] ,
            user['username'],
            user.get('location') ,
            user['description'],
            user['url'] ,
            user['protected'] ,
            user['public_metrics']['followers_count'] ,
            user['public_metrics']['following_count'] ,
            user['public_metrics']['listed_count'],
            user['verified'] ,
            user['public_metrics']['tweet_count'] ,
            user['profile_image_url'] ,
        )

        try:
            cursor.execute(instr_user, values)
        except psycopg2.errors.StringDataRightTruncation as err:
                print(f"expanded_url: {user['screen_name']}\n\
                tweet: {user} \n" + str(err))
                exit()

def insert_urls_deprecated(jarray):
    """
    this function kept adding duplicate urls
    the composite key was of no use for url_tid either as it
    would get a new auto id - tweet_id pair
    """
    for i, tj_dic in enumerate(jarray):
        for url in tj_dic['entities']['urls']:
            url_string =\
            "insert ignore into  urls(\
            index_begin,\
            index_end,\
            url_short,\
            expanded_url,\
            display_url)\
            values(%s, %s, %s, %s, %s)"
            values = (
            url['indices'][0],
            url['indices'][1],
            url['url'],
            url['expanded_url'],
            url['display_url'])

            cursor.execute(url_string, values)

            url_tid_string =\
            "insert ignore into  urls_tid( \
            tweet_id, \
            url_id) \
            values(%s, last_insert_id());"
            values = (
            int(tj_dic['id_str'])
            )
            cursor.execute(url_tid_string, values)


def insert_urls_new(tweets):
    for i, tweet in enumerate(tweets):
        if tweet.get('entities') is None:
            return
        for url in tweet['entities'].get('urls', []):
            url_string =\
            "insert into urls_new(\
            url_name,\
            url_short,\
            expanded_url,\
            display_url)\
            values(%s, %s, %s, %s)\
                on conflict(url_name) do nothing"
            values = (
            url['url'].split('/')[3],
            url['url'],
            url['expanded_url'],
            url['display_url'])
            try:
                cursor.execute(url_string, values)
            except psycopg2.errors.StringDataRightTruncation as err:
                print(f"expanded_url: {url['expanded_url']}\n\
                    tweet: {tweet} \n" + str(err))
                exit()
            url_tid_string =\
            "insert  into urls_tid_new( \
            tweet_id, \
            url_name, \
            index_begin,\
            index_end)\
            values(%s, %s, %s, %s)\
                on conflict(tweet_id, url_name ) do nothing"
            values= (
            int(tweet['id_str']) if tweet.get('id_str') else tweet['id'],
            url['url'].split('/')[3],
            url['indices'][0] if url.get('indices') else url['start'],
            url['indices'][1]  if url.get('indices') else url['end']
            )
            cursor.execute(url_tid_string, values)


def insert_media(jarray):
        # month_adig = {name: i for i, name in enumerate(calendar.month_abbr)}
    for i, tj_dic in enumerate(jarray):
        for media in tj_dic['entities'].get('media', []):

            media_string=\
                "insert  into  media(\
                media_type, \
                index_begin ,\
                index_end ,\
                height,\
                width,\
                media_url ,\
                url,\
                id ,\
                media_url_https,\
                expanded_url,\
                display_url \
                )\
                values(%s, %s, %s , %s, %s, %s,\
                        %s, %s, %s, %s , %s)\
                on conflict(id) do update set\
                index_begin  = %s,\
                index_end  = %s,\
                media_url  = %s,\
                url = %s,\
                id  = %s,\
                media_url_https = %s,\
                expanded_url = %s,\
                display_url = %s "
                # values('%s', %s, %s ,'%s', '%s', '%s', %s ,'%s')"
            values = (
                media.get('type', 'NULL'),
                media['indices'][0] if media.get('indices') is not None else 'NULL',
                media['indices'][1] if media.get('indices') is not None else 'NULL',
                media['sizes']['large']['h'],
                media['sizes']['large']['w'],
                media['media_url'], #change earlier http to https 7sept
                #delete media_url_https and even display_url and url?
                media['url'],
                media['id'],
                media.get('media_url_https'),
                media.get('expanded_url'),
                media['display_url'],
                media['indices'][0] if media.get('indices') is not None else 'NULL',
                media['indices'][1] if media.get('indices') is not None else 'NULL',
                media['media_url'], #change earlier http to https 7sept
                #delete media_url_https and even display_url and url?
                media['url'],
                media['id'],
                media.get('media_url_https'),
                media.get('expanded_url'),
                media['display_url']
                )
            cursor.execute(media_string, values)

            media_tid_string =\
                "insert  into  media_tid(\
                tweet_id,\
                media_id \
                )\
                values(%s, %s)\
                on conflict(media_id, tweet_id ) do nothing"
            values = (
                int(tj_dic['id_str']),
                media['id']
            )
            cursor.execute(media_tid_string, values)

            med_size_string =\
                "insert into  media_size(\
                 media_id,\
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
                values(%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s)\
                    on conflict(media_id) do nothing"
            values = (
            media['id_str'],
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
            cursor.execute(med_size_string, values)





def insert_mediav2_includes(includes):
        # month_adig = {name: i for i, name in enumerate(calendar.month_abbr)}
    # medias = [m['data'] for m in includes['media']]
    if includes.get('media') is None:
        return
    for i, media in enumerate(includes['media']):

        met_playback_0  = None
        met_playback_25  = None
        met_playback_50  = None
        met_playback_75  = None
        met_playback_100 = None
        if(media.get('organic_metrics') is not None):
            met_playback_0 = media['organic_metrics'][' met_playback_0 ']
            met_playback_25  = media['organic_metrics']['met_playback_25 ']
            met_playback_50  = media['organic_metrics']['met_playback_50 ']
            met_playback_75  = media['organic_metrics']['met_playback_75 ']
            met_playback_100 = media['organic_metrics']['met_playback_100']
        media_string=\
            "insert into  media(\
            media_key,\
            id ,\
            media_type, \
            height,\
            width,\
            duration_ms,\
            preview_image_url,\
            media_url ,\
            met_playback_0 ,\
            met_playback_25 ,\
            met_playback_50 ,\
            met_playback_75 ,\
            met_playback_100,\
            view_count,\
            alt_text)\
            values(%s, %s, %s , %s, %s, %s, %s ,%s,\
                    %s , %s, %s, %s, %s ,%s, %s)\
            on conflict(id) do update set \
            media_key = %s,\
            media_type = %s, \
            height = %s,\
            width = %s,\
            duration_ms = %s,\
            preview_image_url = %s,\
            media_url  = %s,\
            met_playback_0  = %s,\
            met_playback_25  = %s,\
            met_playback_50  = %s,\
            met_playback_75  = %s,\
            met_playback_100 = %s,\
            view_count = %s,\
            alt_text = %s"
            # values('%s', %s, %s ,'%s', '%s', '%s', %s ,'%s')"
        values = (
            media['media_key'],
            int(media['media_key'].split('_')[1]),
            media['type'],
            media['height'],
            media['width'],
            media.get('duration_ms'),
            media.get('preview_image_url'),
            media.get('url'),
            met_playback_0 ,
            met_playback_25 ,
            met_playback_50 ,
            met_playback_75 ,
            met_playback_100,
            media['public_metrics']['view_count'] if media.get('public_metrics') else None,
            media.get('alt_text'),

            media['media_key'],
            media['type'],
            media['height'],
            media['width'],
            media.get('duration_ms'),
            media.get('preview_image_url'),
            media.get('url'),
            met_playback_0 ,
            met_playback_25 ,
            met_playback_50 ,
            met_playback_75 ,
            met_playback_100,
            media['public_metrics']['view_count'] if media.get('public_metrics') else None,
            media.get('alt_text'),
            )
        cursor.execute(media_string, values)

        
def insert_mediav2_tid(tweets):
    for tweet in tweets:
        if tweet.get('attachments') is None:
            return
        if tweet['attachments'].get('media_keys') is None:
            return
        for media_key in tweet['attachments']['media_keys']:
            media_tid_string =\
                "insert into  media_tid(\
                tweet_id,\
                media_id, \
                media_key\
                )\
                values(%s, %s, %s)\
                on conflict(media_id, tweet_id ) do update set \
                media_key = %s"
            values = (
                tweet['id'],
                media_key.split('_')[1],
                media_key,
                media_key
                )
            if media_key is None:
                print("media_key none at: tweet: {tweet['id']}\
                    media: {} ")
            cursor.execute(media_tid_string, values)


def insert_hashtags(tweets):
    for i, tweet in enumerate(tweets):
        if tweet.get('entities') is None:
            return
        for hashtag in tweet['entities'].get('hashtag', []):
            ht_string =\
                "insert  into  hashtags(\
                tag\
                )\
                values(%s)\
                on conflict(tag) do nothing"
            values =( 
                hashtag['text'] if hashtag.get('text') else hashtag['tag'],
                )
            cursor.execute(ht_string, values)

            hashtag_tweet=\
                "insert hashtag_tweet(\
                tag ,\
                tweet_id ,\
                index_begin ,\
                index_end ,\
                )\
                values(%s, %s, %s, %s)"
            values = (                
                hashtag['text'] if hashtag.get('text') else hashtag['tag'],
                int(tweet['id_str']) if tweet.get('id_str') else tweet['id'],
                hashtag['indices'][0] if hashtag.get('indices') else hashtag['start'],
                hashtag['indices'][1] if hashtag.get('indices') else hashtag['end'],
                )
            cursor.execute(hashtag_tweet, values)


def insert_rt_tweet_deprecated(rt_id, t_id):
    rt_tweet_s=\
        'insert  into  rt_tweet(\
        tweet_id ,\
        retweet_id \
        )\
        values(%s, %s)\
        on conflict(tweet_id, retweet_id) do nothing'
    values = (
        t_id,
        rt_id
        )
    cursor.execute(rt_tweet_s, values)


def insert_rt_qt_tweetsv2_not_needed(tweets):
#incomplete
    for i, tweet in enumerate(tweets):
        for ref_tweet in tweet['referenced_tweets']:
            if ( ref_tweet.get('type') == 'quoted'):
                insert_qt_tweet(int(ref_tweet['id']), int(tweet['id_str']))

            if ref_tweet.get('type') == 'retweeted':
                retweet_id = int(ref_tweet['id'])
                insert_tweets([tweet['retweeted_status']])
                insert_rt_tweet(retweet_id, int(tweet['id_str']))
                insert_users([tweet['retweeted_status']])


def insert_rt_qt_tweets(tweets):

    for i, tweet in enumerate(tweets):

        # if  tweet.get('is_quote_status'):
        #     insert_qt_tweet(int(tweet['quoted_status_id_str']), int(tweet['id_str']))

        if tweet.get('retweeted_status') is not None:
            retweet_id = int(tweet['retweeted_status']['id_str'])
            insert_tweets([tweet['retweeted_status']])
            # insert_rt_tweet(retweet_id, int(tweet['id_str']))
            insert_users([tweet['retweeted_status']])


def insert_qt_tweet_deprecated(qt_id, t_id):
    qt_tweet_s=\
        "insert into  qt_tweet(\
        quote_tweet_id ,\
        tweet_id \
        )\
        values(%s, %s)\
        on conflict(quote_tweet_id, tweet_id) do nothing "
    values =(
        qt_id,
        t_id
        )
    cursor.execute(qt_tweet_s, values)

def insert_place(jarray):
    for tj_dic in jarray:
        if tj_dic['place'] is None:
            continue
        insert_place =\
            "insert  into place_new(\
                id,\
                url,\
                place_type,\
                city_name,\
                full_name,\
                country_code,\
                country,\
                centroid_long,\
                centroid_lat,\
                geo_coord_long,\
                geo_coord_lat,\
                bb_a_long ,\
                bb_a_lat ,\
                bb_b_long,\
                bb_b_lat ,\
                bb_c_long,\
                bb_c_lat ,\
                bb_d_long,\
                bb_d_lat,\
                bb_type)\
            values(%s, %s, %s, %s, %s, %s, %s,\
                    %s, %s, %s, %s, %s, %s, %s,\
                    %s, %s, %s, %s, %s, %s)\
                    on conflict(id) do nothing"
        values = (
            tj_dic['place']['id'],
            tj_dic['place']['url'],
            tj_dic['place']['place_type'],
            tj_dic['place']['name'],
            tj_dic['place']['full_name'],
            tj_dic['place']['country_code'],
            tj_dic['place']['country'],
            tj_dic['place'].get('centroid')[0] if tj_dic.get('place').get('centroid') else None,
            tj_dic['place'].get('centroid')[1] if tj_dic.get('place').get('centroid') else None,
            tj_dic['place'].get('coordinates')['cordinates'][0] if tj_dic['place'].get('coordinates') else None,
            tj_dic['place'].get('coordinates')['cordinates'][1] if tj_dic['place'].get('coordinates') else None,
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
        cursor.execute(insert_place, values)




def insert_place_temp(jarray):
    for tj_dic in jarray:
        insert_place =\
            "insert ignore into place_new(\
                id,\
                url,\
                place_type,\
                city_name,\
                full_name,\
                country_code,\
                country,\
                centroid_long,\
                centroid_lat,\
                geo_coord_long,\
                geo_coord_lat,\
                bb_a_long ,\
                bb_a_lat ,\
                bb_b_long,\
                bb_b_lat ,\
                bb_c_long,\
                bb_c_lat ,\
                bb_d_long,\
                bb_d_lat,\
                bb_type)\
            values(%s, %s, %s, %s, %s, %s, %s,\
                    %s, %s, %s, %s, %s, %s, %s,\
                    %s, %s, %s, %s, %s, %s)"
        values = (
                tj_dic['id'],
                tj_dic['url'],
                tj_dic['place_type'],
                tj_dic['city_name'],
                tj_dic['full_name'],
                tj_dic['country_code'],
                tj_dic['country'],
                tj_dic['centroid_long'],
                tj_dic['centroid_lat'],
                tj_dic['geo_coord_long'],
                tj_dic['geo_coord_lat'],
                tj_dic['bb_a_long'] ,
                tj_dic['bb_a_lat'] ,
                tj_dic['bb_b_long'],
                tj_dic['bb_b_lat'] ,
                tj_dic['bb_c_long'],
                tj_dic['bb_c_lat'] ,
                tj_dic['bb_d_long'],
                tj_dic['bb_d_lat'],
                tj_dic['bb_type']
        )
        cursor.execute(insert_place, values)

        query = \
            " insert ignore into place_tweet(\
                tweet_id,\
                place_id)\
                values( %s, %s)"
        values = (
            tj_dic['tweet_id'],
            tj_dic['id']
        )
        cursor.execute(query, values)


def insert_place_v2(includes):

    if includes.get('places') is None:
        return

    for place in includes['places']:
        insert_place =\
            "insert into place_new(\
                id,\
                place_type,\
                city_name,\
                full_name,\
                country_code,\
                country,\
                bb_a_long ,\
                bb_a_lat ,\
                bb_c_long,\
                bb_c_lat ,\
                bb_type)\
            values(%s, %s, %s, %s, %s,\
                    %s, %s, %s, %s, %s, %s\
                    )\
                on conflict(id) do nothing"
        values = (
            place['id'],
            place['place_type'],
            place['name'],
            place['full_name'],
            place['country_code'],
            place['country'],
            place['geo']['bbox'][0],
            place['geo']['bbox'][1],
            place['geo']['bbox'][2],
            place['geo']['bbox'][2],
            place['geo']['type']
        )
        cursor.execute(insert_place, values)


def insert_place_v2_tid(tweets):

    for tweet in tweets:
        if tweet.get('geo') is None:
            break

        # had to add exact location from tweets as include places didnt have it
        insert_place =\
            "insert into place_new(\
            id)\
            values(%s)\
            on conflict(id) do update set \
            geo_coord_long = %s,\
            geo_coord_lat = %s"
        values = (
            tweet['geo']['place_id'],

            tweet['geo']['coordinates']['coordinates'][0] if tweet.get('geo').get('coordinates') else None,
            tweet['geo']['coordinates']['coordinates'][1] if tweet.get('geo').get('coordinates') else None
        )
        cursor.execute(insert_place, values)


def insert_list_tags_users(users_dic, list_name):
    for user in users_dic:
        qt_tweet_s=\
            "insert into  list_tags_users(\
            user_id ,\
            tag_name \
            )\
            values(%s, %s)\
            on conflict(tag_name, user_id) do nothing"
        values = (
            user.id ,
            list_name
            )
        cursor.execute(qt_tweet_s, values)
    conn.commit()


def insert_list_tags(tags):
    for tag in tags:
        insert_query =\
            "insert  into list_tags(\
                tag_name\
                )\
                values(%s)\
                on conflict do nothing"
        values = (
            tag['name'],
        )
        cursor.execute(insert_query, values)


def insert_context_annotations(tweets):
    for tweet in tweets: 
        if(tweet.get('context_annotations') is None):
            continue
        cas = tweet['context_annotations']
        for anno in cas:
            query =\
                "insert into context_annotations(\
                id,\
                name,\
                description\
                )\
                values(%s, %s, %s)\
                on conflict(id) do nothing"
            values =\
                    (int(anno['domain']['id']),
                anno['domain']['name'],
                anno['domain'].get('description'))
            
            cursor.execute( query, values)

            query =\
                "insert into context_entities(\
                id,\
                name)\
                values (%s, %s)\
                on conflict(id) do nothing"
            values = (
                int(anno['entity']['id']),
                anno['entity']['name']
            )
            cursor.execute(query, values)

            query =\
                "insert  into context_tweets(\
                    tweet_id,\
                    annotation_id,\
                    entity_id)\
                    values( %s, %s, %s)\
                    on conflict(tweet_id, annotation_id, entity_id) do nothing"
            values = (
                tweet['id'],
                int(anno['domain']['id']),
                int(anno['entity']['id'])

            )
            cursor.execute(query, values)


def load_json(j_array):

    insert_tweets(j_array)
    conn.commit()
    insert_users(j_array)
    conn.commit()
    insert_urls_new(j_array)
    conn.commit()
    insert_media(j_array)
    conn.commit()
    insert_hashtags(j_array)
    conn.commit()
    insert_place(j_array)
    conn.commit()
    # insert_rt_qt_tweets(j_array)
    # cnx.commit()



def load_user_timeline_v2(tweepy_reponse):
    if(tweepy_reponse['data'] is None):
        return


    # cursor.execute(f"select * from tweets where id = {int(tweepy_reponse['data'][0]['id_str'])}")
    # result = cursor.fetchall()
    # if(len(result) != 0):
    #     return
    if tweepy_reponse['includes'].get('tweets'):
        insert_tweets_v2(tweepy_reponse['includes']['tweets'])
        conn.commit()
    insert_tweets_v2(tweepy_reponse['data'])
    conn.commit()
    insert_users_v2(tweepy_reponse['includes'])
    conn.commit()
    insert_urls_new(tweepy_reponse['data'])
    conn.commit()
    insert_mediav2_includes(tweepy_reponse['includes'])
    conn.commit()
    insert_mediav2_tid(tweepy_reponse['data'])
    conn.commit()
    insert_hashtags(tweepy_reponse['data'])
    conn.commit()
    insert_place_v2(tweepy_reponse['includes'])
    conn.commit()
    insert_place_v2_tid(tweepy_reponse['data'])
    conn.commit()
    insert_context_annotations(tweepy_reponse['data'])
    conn.commit()



def disconnect():
    conn.disconnect()


def connect_db(db_name = None):
    global conn
    try:
        conn = psycopg2.connect( database= db_name, **s.db_args)
    except psycopg2.OperationalError as e:
        print("psycopg2 OperationalError: " + str(e))
    except psycopg2.ProgrammingError as e:
        print("psycopg2 ProgrammingError: " + str(e))

def initialize_db():

    # connect args -> user=user, password=password, host=host, database=database_name, charset='utf8'
    global cursor
    connect_db()
    # conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    # cursor = conn.cursor(buffered=True)
    cursor = conn.cursor()
    conn.autocommit = True
    cursor.execute(f"select 1 from pg_catalog.pg_database where datname = '{s.db_name}'")
    db_result = cursor.fetchone()
    if not db_result:
        print(f"Creating database {s.db_name}")
        cursor.execute(f"create database {s.db_name}")
        conn.close()
        connect_db(s.db_name)
    else: 
        conn.close()
        connect_db(s.db_name)
    cursor = conn.cursor()
    conn.autocommit = False

    create_tweets()
    create_users()
    create_media()
    create_med_size()
    create_enhanced_urls()
    create_hashtags()
    create_qt_tweet()
    create_rt_tweet()
    create_urls_new()
    create_users_mentions()
    create_place_new()
    create_place_tweet()
    create_list_tags()
    create_list_tags_users()
    create_context_annotations()
    create_context_entities()
    create_context_tweets()

    conn.commit()
