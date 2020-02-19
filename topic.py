import re
import os
import csv
import json
import time
import random
from pymongo import MongoClient
from datetime import datetime
import sys

import requests
import utils

# 生成Session对象，用于保存Cookie
s = requests.Session()


def spider_topic(post_collection, page, topic_url):
    """
    爬取新浪话题
    新浪微博分页机制：根据时间分页，每一条微博都有一个since_id，时间越大的since_id越大
    所以在请求时将since_id传入，则会加载对应话题下比此since_id小的微博，然后又重新获取最小since_id
    将最小since_id传入，依次请求，这样便实现分页
    :return:
    """

    # 1、构造请求
    '''
    !!! important this should be change to the id of the newest post, check:https://juejin.im/post/5d46adfae51d456201486dcd on how to get this id
    '''
       
    topic_url = f"{topic_url}&page={page}"
    kv = {'user-agent': 'Mozilla/5.0',
          'Referer': 'https://m.weibo.cn/p/1008087a8941058aaf4df5147042ce104568da/super_index?jumpfrom=weibocom'}

    try:
        r = s.get(url=topic_url, headers=kv)
        r.raise_for_status()
    except:
        print('链接爬取失败')
        return False
    # 2、解析数据
    r_json = json.loads(r.text)
    cards = r_json['data']['cards']
    # 2.1、第一次请求cards包含微博和头部信息，以后请求返回只有微博信息
    # card_group = cards[2]['card_group'] if len(cards) > 1 else cards[0]['card_group']
    duplicate_count = 0
    for card in cards:
        if duplicate_count >= 3:
            print("duplicate exceeds")
            return False

        if 'mblog' not in card:
            continue

        mblog = card['mblog']

        find_id = mblog['id']

        # 2.3、解析用户信息
        user = mblog['user']
        # GET USER NAME, ID
        user_name = user['screen_name']
        user_id = user['id']
        user_rank = user['urank']

        if "retweeted_status" in mblog:
            if mblog["retweeted_status"]["user"] is not None:
                find_id = mblog["retweeted_status"]["id"]
                user_name = mblog["retweeted_status"]["user"]["screen_name"]
                user_id = mblog["retweeted_status"]["user"]["id"]
                user_rank = mblog["retweeted_status"]["user"]['urank']
            else:
                continue
        
        try:
            sina_text = spider_full_content(find_id)
        except:
            continue

        # 过滤html标签，留下内容

        now = datetime.now()
        timestamp = datetime.timestamp(now)

        # 把信息放入列表
        post_dict = {
          "wb-id": find_id,
          "user-id": user_id, 
          "user-name": user_name, 
          "user-rank": user_rank, 
          "clean_content": sina_text[1],
          "full_content": sina_text[0],
          "forward": mblog['reposts_count'], 
          "comment": mblog['comments_count'], 
          "like": mblog['attitudes_count'], 
          "created at": timestamp, 
          "relative time": mblog['created_at']
        }

        # 检验列表中信息是否完整
        # sina_columns数据格式：['wb-id', 'user-id', 'user-name', 'user-rank', 'clean_content', 'full_content', 'forward', 'comment', 'like', 'created at', 'relative time']
        # 3、保存数据
        post_collection.update_one({'wb-id': find_id}, {'$set': post_dict}, upsert=True)
        print(f"{find_id} created by {user_name} is finished.")

        # 5、爬取用户信息不能太频繁，所以设置一个时间间隔
        time.sleep(random.randint(1, 5))

def spider_full_content(id) -> list:
    """
    GET FULL CONTENT OF THE WEIBO
    """
    weibo_detail_url = f'https://m.weibo.cn/statuses/extend?id={id}'
    kv = {'user-agent': 'Mozilla/5.0'}
    try:
        r = s.get(url=weibo_detail_url, headers=kv)
        r.raise_for_status()
    except:
        print('爬取信息失败')
        return
    r_json = json.loads(r.text)
    weibo_full_content = r_json['data']['longTextContent']
    clean_content = utils.get_clean_text(weibo_full_content)

    return [weibo_full_content, clean_content]


def patch_topic(raw_post, topic_url):
    for i in range(100):
        print('第%d页' % (i + 1))
        if not spider_topic(raw_post, i, topic_url):
            continue
    