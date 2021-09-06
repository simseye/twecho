import datetime
from . import twitter_db
from os import listdir, walk, remove
from os.path import isfile, join, abspath
import zipfile
import datetime

def clone_lists(api):
    ejl = api.GetLists( )
    ej_lists_ids = [ejlist.slug for ejlist in ejl]
    # for linu, ej_list in enumerate(ejl):
    slug = "" #enter list name thats to be cloned
    o_name = ""  #enter name of the owner of the list to be cloned
    list_members = api.GetListMembersPaged(owner_screen_name=None,
                                        slug=slug, count=5000)
    list_members = list_members[2]
    new_name = "" # enter name of new list
    new_list = api.CreateList(new_name, mode="private")
    ul = api.GetLists( screen_name=None)
    list_ind = 0
    list_id = ul[list_ind]
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
            api.CreateListsMember(list_id=new_list.id, user_id=chunk,
                        owner_screen_name=None)
        except Exception as e:
            print(e)





def load_db_file(db_password, destination_p, destination_pa):
    twitter_db.initializedb(db_password)


    for root, dirs, files in walk(destination_p):
        for file in files:
            twitter_db.load_file(join(root, file))



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