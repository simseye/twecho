import random
import datetime
from os import listdir, walk, remove
from os.path import isfile, join, abspath
from random import randint
import zipfile
import datetime

from mysql.connector import cursor
from . import settings as s
from . import fetch_mysql as fm

def clone_lists():
    ejl = s.api.get_lists(screen_name='' )
    ej_lists_ids = [ejlist.slug for ejlist in ejl]
    for linu, ej_list in enumerate(ejl):
        list_members = s.api.get_list_members(owner_id=ej_list.user.id,
                                            slug=ej_list.slug, count=5000)
        new_name = ej_list.name # enter name of new list
        new_list = s.api.create_list(new_name, mode="private")
        user_ids = []
        for user in list_members:
            user_ids.append(user.id)
        list_len = len(user_ids)
        chunks_n = int(list_len/100)
        chunks = []
        for i in range(chunks_n):
            chunks.append(user_ids[i * 100: (i + 1) * 100])
        chunks.append(user_ids[chunks_n * 100 :])

        for chunk in chunks:
            try:
                s.api.add_list_members(list_id=new_list.id, user_id=chunk,
                            owner_screen_name=None)
            except Exception as e:
                print(e)





def load_db_file(db_password, destination_p, destination_pa):
    s.db.initializedb(db_password)


    for root, dirs, files in walk(destination_p):
        for file in files:
            s.db.load_file(join(root, file))



def zip_jsons():
    abs_src = abspath(destination_p)
    dt = datetime.datetime.now()
    dt_timestamp = dt.strftime("%Y-%b-%d_%H%M%S")
    zipf = zipfile.ZipFile(destination_pa + dt_timestamp + '.zip', 'w', zipfile.ZIP_DEFLATED)
    for root, dirs, files in walk(destination_p):
        for file in files:
            absname = abspath(join(root, file))
            arcname = absname[len(abs_src) + 1:]
            zipf.write(join(root, file), arcname)
    zipf.close()
    for root, dirs, files in walk(destination_p):
        for file in files:
            remove(join(root, file))


def deduplicate_urls():
    for urls_query in fm.fetch_urls():
        join_q = f"select * from urls_tid join ({urls_query}) as uq where\
                    uq.id = urls_tid.url_id"
        s.db.cursor.execute(join_q)
        url_tweets = s.db.cursor.fetchall()
        url = {}
        s.db.create_urls_new()
        for url_tweet in url_tweets:
            url['url'] = url_tweet['url_short']
            url['expanded_url'] = url_tweet['expanded_url']
            url['display_url'] = url_tweet['display_url']
            url['indices'] = []
            url['indices'].append(url_tweet['index_begin'])
            url['indices'].append(url_tweet['index_end'])
            tweet = {'entities':{'urls': [url]}, 'id_str': url_tweet['tweet_id'] }
            s.db.insert_urls_new([tweet])
        s.db.cnx.commit()

def deduplicate_place():
    places_q = "select * from place"
    s.db.cursor.execute(places_q)
    places = s.db.cursor.fetchall()
    s.db.insert_place_temp(places)        
    s.db.cnx.commit()

def check_after_url_dup():
    errors = []
    for i in range(10000):
        ri = random.randint(1, 9000000)
        s.db.cursor.execute(f"select * from urls where id = {ri}")
        url_old = s.db.cursor.fetchall()
        new_id = url_old[0]['url_short'].split('/')[3]
        s.db.cursor.execute(f"select * from urls_new where url_name = '{new_id}'")
        url_new = s.db.cursor.fetchall()
        if len(url_new) > 0:
            # print(f"{i}. new urls len: {len(url_new)} and id(1st) = {url_new[0]['url_name']}")
            pass
        else:
            errors.append(f"{i}.  missing id = {new_id} at old index {ri} \n")
    print(errors)