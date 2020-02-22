import os
import requests
import json
import pymongo
import argparse

from super_topic import patch_super_topic
from topic import patch_topic
from constant import ncov_topic_url, not_ncov_topic_url
from csv_generator import csv_generator

# constants
PNEU_MODE = 'pneumonia'

# enable flag argument
parser = argparse.ArgumentParser()
parser.add_argument('-m', '--mode', default=PNEU_MODE)
parser.add_argument('-a', '--all', action='store_const', const=True, default=False)


# 生成Session对象，用于保存Cookie
s = requests.Session()


def login_sina():
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


def crawl_pneumonia_topic(db):
    # get all input
    super_topic_latest_uid = input("Latest uid:")

    # patch super topic for nCov patient
    raw_post = db.raw_post
    patch_super_topic(raw_post, super_topic_latest_uid)

    # patch topic for nCov patient
    patch_topic(raw_post, ncov_topic_url)

    save_csv(raw_post)

    return 1


def crawl_non_pneumonia(db):
    not_conv_raw_post = db.not_conv_raw_post
    for url in not_ncov_topic_url:
        patch_topic(not_conv_raw_post, url)

    save_csv(not_conv_raw_post, "非肺炎其他患者求助话题")

    return 1


def main():
    # parse flag
    args = parser.parse_args()
    _all = getattr(args, 'all')
    _mode = getattr(args, 'mode')

    # Sign in
    if not login_sina():
        return

    # setup database
    client = pymongo.MongoClient(f"mongodb+srv://{os.environ['DB_USER']}:{os.environ['DB_PW']}@{os.environ['DB_HOST']}")
    db = client.weibo_topic

    # assemble pipeline according to args
    if _all:
        pipelines = [
            crawl_pneumonia_topic,
            crawl_non_pneumonia,
        ]
    elif _mode == PNEU_MODE:
        pipelines = [
            crawl_pneumonia_topic,
        ]
    else:
        pipelines = [
            crawl_non_pneumonia,
        ]

    # execution
    res = [step(db) for step in pipelines]


if __name__ == '__main__':
    main()
