import requests
import json
import argparse

from super_topic import patch_super_topic
from topic import patch_topic
from constant import ncov_topic_url, not_ncov_topic_url

from io_utils import (
    login_sina,
    save_csv,
    set_up_database
)

# constants
PNEU_MODE = 'pneumonia'

# enable flag argument
parser = argparse.ArgumentParser()
parser.add_argument('-m', '--mode', default=PNEU_MODE)
parser.add_argument('-a', '--all', action='store_const', const=True, default=False)
parser.add_argument('-s', '--since')

# 生成Session对象，用于保存Cookie
s = requests.Session()


def crawl_pneumonia_topic(db, _id):
    # get all input
    super_topic_latest_uid = _id

    # patch super topic for nCov patient
    raw_post = db.raw_post

    # use preset static end for test use
    patch_super_topic(raw_post, super_topic_latest_uid)

    # patch topic for nCov patient
    patch_topic(raw_post, ncov_topic_url)

    # save results to csv
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
    _id = getattr(args, 'since')

    # Sign in
    if not login_sina(s):
        return

    # setup database
    db = set_up_database()

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
    res = [step(db, _id) for step in pipelines]


if __name__ == '__main__':
    main()
