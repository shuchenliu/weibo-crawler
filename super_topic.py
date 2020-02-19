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

# 每次请求中最小的since_id，下次请求使用，新浪分页机制
super_min_since_id = ''
# 生成Session对象，用于保存Cookie
s = requests.Session()

def spider_topic(post_collection, latest_uid):
    """
    爬取新浪话题
    新浪微博分页机制：根据时间分页，每一条微博都有一个since_id，时间越大的since_id越大
    所以在请求时将since_id传入，则会加载对应话题下比此since_id小的微博，然后又重新获取最小since_id
    将最小since_id传入，依次请求，这样便实现分页
    :return:
    """

    # 1、构造请求
    global super_min_since_id
    topic_url = 'https://m.weibo.cn/api/container/getIndex?jumpfrom=weibocom&containerid=1008084882401a015244a2ab18ee43f7772d6f_-_feed'

    '''
    !!! important this should be change to the id of the newest post, check:https://juejin.im/post/5d46adfae51d456201486dcd on how to get this id
    '''
    if not super_min_since_id:
        super_min_since_id= str(int(latest_uid) + 1)

    history_max_id = list(post_collection.find().sort('wb-id',-1).limit(1))[0]['wb-id']
    # history_max_id = '4472676012169540'
       
    topic_url = f"{topic_url}&since_id={super_min_since_id}"
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
    card_group = cards[2]['card_group'] if len(cards) > 1 else cards[0]['card_group']
    duplicate_count = 0
    for card in card_group:
        if duplicate_count >= 3:
            print("duplicate exceeds")
            return False

        mblog = card['mblog']
        # 2.2、解析微博内容
        r_since_id = mblog['id']
        if r_since_id <= history_max_id:
            print('finish_history')
            return 'finish'
        
        if super_min_since_id == r_since_id:
            duplicate_count += 1
            continue

        if not spider_full_content(r_since_id):
            continue

        sina_text = spider_full_content(r_since_id)

        # 2.3、解析用户信息
        user = mblog['user']
        user_name = user['screen_name']
        user_id = user['id']
        user_rank = user['urank']

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
          "relative time": mblog['created_at']
        }

        post_collection.update_one({'wb-id': r_since_id}, {'$set': post_dict}, upsert=True)
        print(f"{r_since_id} created by {user_name} is finished.")

        # 4、获得最小since_id，下次请求使用
        if super_min_since_id > r_since_id:
            super_min_since_id = r_since_id 

        # 5、爬取用户信息不能太频繁，所以设置一个时间间隔
        time.sleep(random.randint(3, 6))

def spider_full_content(id) -> list:
    """
    GET FULL CONTENT OF THE WEIBO
    """
    weibo_detail_url = f'https://m.weibo.cn/statuses/extend?id={id}'
    kv = {'user-agent': 'Mozilla/5.0'}
    try:
        r = s.get(url=weibo_detail_url, headers=kv)
        r.raise_for_status()
        r_json = json.loads(r.text)
    except:
        print('爬取信息失败')
        return False
    
    weibo_full_content = r_json['data']['longTextContent']
    clean_content = utils.get_clean_text(weibo_full_content)

    return [weibo_full_content, clean_content]

def patch_super_topic(db, latest_uid):
    for i in range(100):
        print('第%d页' % (i + 1))
        if not spider_topic(db, latest_uid):
            continue

        if spider_topic(db, latest_uid):
            break