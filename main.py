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
SUPER_TOPIC = 'super'
NORMAL_TOPIC = 'normal'
NON_PNEU = 'other'

# enable flag argument
parser = argparse.ArgumentParser()
parser.add_argument('-m', '--mode', default=PNEU_MODE)
parser.add_argument('-a', '--all', action='store_const', const=True, default=False)
parser.add_argument('-s', '--save', action='store_const', const=True, default=False)    # scrape and save
parser.add_argument('-o', '--output', action='store_const', const=True, default=False)  # save file only, no scraping
parser.add_argument('-f', '--from')
parser.add_argument('-t', '--to')

# 生成Session对象，用于保存Cookie
s = requests.Session()


def crawl_pneumonia_super_topic(db, save, _from, _to):
    # patch super topic for nCov patient
    super_post = db.super_post

    # use preset static end for test use
    patch_super_topic(super_post, _from, _to)

    if save:
        save_csv(super_post, '肺炎患者求助-超级话题')

    return 1


def crawl_pneumonia_topic(db, save):
    normal_post = db.normal_post

    # patch topic for nCov patient
    patch_topic(normal_post, ncov_topic_url)

    # save results to csv
    if save:
        save_csv(normal_post, '肺炎患者求助-普通话题')

    return 1


def crawl_non_pneumonia(db, save):
    not_conv_raw_post = db.not_conv_raw_post
    for url in not_ncov_topic_url:
        patch_topic(not_conv_raw_post, url)

    if save:
        save_csv(not_conv_raw_post, "非肺炎其他患者求助话题")

    return 1


def high_order_save_csv(db, collection_name, filename):
    save_csv(db[collection_name], filename)
    return 1


def get_save_operations(_all, _mode) -> list:
    collection_list = [
        'super_post',
        'normal_post',
        'not_conv_raw_post',
    ]

    filename_list = [
        '肺炎患者求助-超级话题',
        '肺炎患者求助-话题',
        '非肺炎患者求助话题'
    ]

    _args = zip(collection_list, filename_list)
    funcs_with_args = [(high_order_save_csv, (col, fil)) for col, fil in _args]

    if _all:
        return funcs_with_args

    mode_indices_dict = {
        PNEU_MODE: [0, 1],
        SUPER_TOPIC: [0],
        NORMAL_TOPIC: [1],
        NON_PNEU: [2],
    }

    return [funcs_with_args[i] for i in mode_indices_dict[_mode]]


def get_pipeline(_all, _mode, _from, _save, _output, _to) -> list:

    # output only mode
    if _output:
        return get_save_operations(_all, _mode)

    regular_args = (_save,)
    super_args = regular_args + (_from, _to)

    full_pipeline = [
        crawl_pneumonia_super_topic,
        crawl_pneumonia_topic,
        crawl_non_pneumonia,
    ]

    args_list = [
        super_args,
        regular_args,
        regular_args
    ]

    pipeline_with_args = list(zip(full_pipeline, args_list))

    if _all:
        return pipeline_with_args

    mode_indices_dict = {
        PNEU_MODE: [0, 1],
        SUPER_TOPIC: [0],
        NORMAL_TOPIC: [1],
        NON_PNEU: [2],
    }

    step_indices = mode_indices_dict[_mode]

    return [pipeline_with_args[i] for i in step_indices]


def get_flags(args):
    flags = [
        'all',
        'mode',
        'from',
        'save',
        'output',
        'to',
    ]

    return [getattr(args, flag) for flag in flags]


def main():
    # parse flag
    args = parser.parse_args()
    flags = get_flags(args)

    # Sign in - possibly optional
    # if not login_sina(s):
    #     return

    # setup database
    db = set_up_database()

    # assemble pipeline according to args
    pipeline = get_pipeline(*flags)

    # execution
    res = [step(db, *_args) for step, _args in pipeline]


if __name__ == '__main__':
    main()
