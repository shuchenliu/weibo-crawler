import requests
import os
import pymongo
import json
import utils
from datetime import datetime
import cloudinary
import cloudinary.uploader
from csv_generator import csv_generator

# constants
feed_url = 'https://m.weibo.cn/api/container/getIndex?jumpfrom=weibocom&containerid=1008084882401a015244a2ab18ee43f7772d6f_-_feed'


def make_weibo_request(s, url):
    headers = {
        'user-agent': 'Mozilla/5.0',
        'Referer': 'https://m.weibo.cn/p/1008087a8941058aaf4df5147042ce104568da/super_index?jumpfrom=weibocom'
    }

    r = s.get(url=url, headers=headers)
    r.raise_for_status()

    return r


def update_db(post_collection, mblog, sina_text, pic_links):
    r_since_id = mblog['id']
    user = mblog['user']
    user_name = user['screen_name']
    user_id = user['id']
    user_rank = user['urank']

    # extract origin user info
    if "retweeted_status" in mblog and mblog["retweeted_status"]["user"] is not None:
        r_since_id = mblog["retweeted_status"]["id"]
        user_name = mblog["retweeted_status"]["user"]["screen_name"]
        user_id = mblog["retweeted_status"]["user"]["id"]
        user_rank = mblog["retweeted_status"]["user"]['urank']

    now = datetime.now()
    timestamp = datetime.timestamp(now)

    # 把信息放入列表
    post_dict = {
        "wb-id": r_since_id,
        "user-id": user_id,
        "user-name": user_name,
        "user-rank": user_rank,
        "clean_content": sina_text[1],
        "full_content": sina_text[0],
        "forward": mblog['reposts_count'],
        "comment": mblog['comments_count'],
        "like": mblog['attitudes_count'],
        "created at": timestamp,
        "relative time": mblog['created_at'],
        "pics": pic_links,
    }

    post_collection.update_one({'wb-id': r_since_id}, {'$set': post_dict}, upsert=True)
    print(f"{r_since_id} created by {user_name} is finished.")


def get_history_id(db):
    return list(db.find().sort('wb-id',-1).limit(1))[0]['wb-id']


def spider_full_content(s, _id) -> list:
    """
    GET FULL CONTENT OF THE WEIBO
    """
    weibo_detail_url = f'https://m.weibo.cn/statuses/extend?id={_id}'
    kv = {'user-agent': 'Mozilla/5.0'}
    try:
        r = s.get(url=weibo_detail_url, headers=kv)
        r.raise_for_status()
        r_json = json.loads(r.text)
    except Exception as e:
        print('爬取信息失败', e)
        return False

    weibo_full_content = r_json['data']['longTextContent']
    clean_content = utils.get_clean_text(weibo_full_content)

    return [weibo_full_content, clean_content]


def set_up_database():
    client = pymongo.MongoClient(f"mongodb+srv://{os.environ['DB_USER']}:{os.environ['DB_PW']}@{os.environ['DB_HOST']}")
    db = client.weibo_topic

    return db


def login_sina(s):
    login_url = 'https://passport.weibo.cn/sso/login'
    headers = {
        'user-agent': 'Mozilla/5.0',
        'Referer': 'https://passport.weibo.cn/signin/login?entry=mweibo&res=wel&wm=3349&r=https%3A%2F%2Fm.weibo.cn%2F'
    }

    data = {
        'username': os.environ["USER"],
        'password': os.environ["PW"],
        'savestate': 1,
        'entry': 'mweibo',
        'mainpageflag': 1
    }
    try:
        r = s.post(login_url, headers=headers, data=data)
        r.raise_for_status()
    except Exception as e:
        print('登录请求失败', e)
        return 0

    # 打印请求结果
    print(json.loads(r.text)['msg'])
    return 1


def save_csv(post, title="肺炎患者求助超话"):
    documents = post.find({}, projection={'_id': False}).sort('wb-id', pymongo.DESCENDING)
    csv_generator(documents, title)


# init cloudinary
cloudinary.config(
    cloud_name=os.environ['cld_name'],
    api_key=os.environ['cld_api_key'],
    api_secret=os.environ['cld_api_secret']
)


def upload_to_cloudinary(url):
    try:
        r = cloudinary.uploader.upload(url)
        return r['secure_url']
    except Exception as e:
        print('Upload image error')
        print(e)


def extract_and_upload_pics(pics):
    ori_links = [pic['large']['url'] for pic in pics]
    links = [upload_to_cloudinary(link) for link in ori_links]

    return links


def upload_pics_for_given_weibo(mblog):
    pic_links = []
    if 'pics' in mblog:
        tar_len = len(mblog['pics'])
        print(f"uploading {tar_len} pictures to Cloudiary")
        pic_links = extract_and_upload_pics(mblog['pics'])

        if len(mblog['pics']) != len(pic_links):
            print(f"Tried {tar_len}, {len(pic_links)} succeeded")
        else:
            print('done')

    return pic_links
