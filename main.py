import os
import requests
import json
import pymongo

from super_topic import patch_super_topic
from topic import patch_topic
from constant import ncov_topic_url, not_ncov_topic_url
from csv_generator import csv_generator


# 生成Session对象，用于保存Cookie
s = requests.Session()

def login_sina():
    login_url = 'https://passport.weibo.cn/sso/login'
    headers = {'user-agent': 'Mozilla/5.0',
               'Referer': 'https://passport.weibo.cn/signin/login?entry=mweibo&res=wel&wm=3349&r=https%3A%2F%2Fm.weibo.cn%2F'}

    data = {'username': os.environ["USER"],
            'password': os.environ["PW"],
            'savestate': 1,
            'entry': 'mweibo',
            'mainpageflag': 1}
    try:
        r = s.post(login_url, headers=headers, data=data)
        r.raise_for_status()
    except:
        print('登录请求失败')
        return 0
    # 打印请求结果
    print(json.loads(r.text)['msg'])
    return 1


def main():
    # Sign in
    if not login_sina():
      return

    # setup database
    client = pymongo.MongoClient(f"mongodb+srv://{os.environ['DB_USER']}:{os.environ['DB_PW']}@{os.environ['DB_HOST']}")
    db = client.weibo_topic

    # # get all input
    # super_topic_latest_uid = input("Latest uid:")

    # # patch super topic for ncov patient
    # raw_post = db.raw_post
    # patch_super_topic(raw_post, super_topic_latest_uid)

    # # patch topic for ncov patient
    # patch_topic(raw_post, ncov_topic_url)

    # # get data from server and parse
    # documents = raw_post.find({}, projection={'_id': False}).sort('wb-id', pymongo.DESCENDING)
    # csv_generator(documents, "肺炎患者求助超话")

    not_conv_raw_post = db.not_conv_raw_post
    for url in not_ncov_topic_url:
        patch_topic(not_conv_raw_post, url)

    documents = not_conv_raw_post.find({}, projection={'_id': False}).sort('wb-id', pymongo.DESCENDING)
    csv_generator(documents, "非肺炎其他患者求助话题")

if __name__ == '__main__':
    main()
