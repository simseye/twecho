from pydb import twitter_api
import os

#json format: '[{"lists": [],"auth": {"CONSUMER_KEY" : "", "CONSUMER_SECRET":  "","ACCESS_TOKEN": "","ACCESS_TOKEN_SECRET": ""}, "owner_screen_name": ""}]'
def main():

    # twitter_api.load_config("enter path to json config file", "enter path to save response jsons")
    
    twitter_api.create_api_instance()
    twitter_api.get_list_tls()

if __name__ == '__main__':
    main()