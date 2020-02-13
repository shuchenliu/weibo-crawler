import re
import os
import csv
import json
import time
import random
from pymongo import MongoClient
from datetime import datetime

import requests
import utils

# 每次请求中最小的since_id，下次请求使用，新浪分页机制
min_since_id = ''
# 生成Session对象，用于保存Cookie
s = requests.Session()
# 新浪话题数据保存文件
CSV_FILE_PATH = './sina_user.csv'


def login_sina():
    """
    登录新浪
    :return:
    """
    # 登录URL
    login_url = 'https://passport.weibo.cn/sso/login'
    # 请求头
    headers = {'user-agent': 'Mozilla/5.0',
               'Referer': 'https://passport.weibo.cn/signin/login?entry=mweibo&res=wel&wm=3349&r=https%3A%2F%2Fm.weibo.cn%2F'}
    # 传递用户名和密码
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


def spider_user():
    """
    爬取新浪话题
    新浪微博分页机制：根据时间分页，每一条微博都有一个since_id，时间越大的since_id越大
    所以在请求时将since_id传入，则会加载对应话题下比此since_id小的微博，然后又重新获取最小since_id
    将最小since_id传入，依次请求，这样便实现分页
    :return:
    """

    # 1、构造请求
    global min_since_id
    user_url = 'https://m.weibo.cn/api/container/getIndex?uid=7298502196&containerid=1076037298502196'

    '''
    !!! important this should be change to the id of the newest post, check:https://juejin.im/post/5d46adfae51d456201486dcd on how to get this id
    '''
    if not min_since_id:
        min_since_id='4456927708622934'
       
    user_url = f"{user_url}&since_id={min_since_id}"
    kv = {'user-agent': 'Mozilla/5.0'}

    try:
        r = s.get(url=user_url, headers=kv)
        r.raise_for_status()
    except:
        print('爬取失败')
        return
    # 2、解析数据
    r_json = json.loads(r.text)
    cards = r_json['data']['cards']
    # 2.1、第一次请求cards包含微博和头部信息，以后请求返回只有微博信息
    duplicate_count = 0
    for card in cards:
        if duplicate_count >= 3:
            print("duplicate exceeds")
            break

        mblog = card['mblog']
        # 2.2、解析微博内容
        r_since_id = mblog['id']
        if min_since_id == r_since_id:
            duplicate_count += 1
            continue

        # 过滤html标签，留下内容
        sina_text = spider_full_content(r_since_id)
        # 2.3、解析用户信息
        user = mblog['user']
        # GET USER NAME, ID
        user_name = user['screen_name']
        user_id = user['id']
        user_rank = user['urank']

        now = datetime.now()
        timestamp = datetime.timestamp(now)

        related_video = {}
        if "page_info" in mblog:
          if "media_info" in mblog["page_info"]:
            related_video = {
              "id": mblog["page_info"]["media_info"]["media_id"],
              "stream_url": mblog["page_info"]["media_info"]["stream_url"]
            }

        retweet = {}
        if "retweeted_status" in mblog:
          retweeted_id = mblog["retweeted_status"]["id"]
          retweetcontent = ""
          retweet_user = {}
          if ("user" in mblog["retweeted_status"]) & (mblog["retweeted_status"]["user"] is not None):
            retweet_user = {
              "id": mblog["retweeted_status"]["user"]["id"],
              "name": mblog["retweeted_status"]["user"]["screen_name"]
            }
            retweetcontent = spider_full_content(retweeted_id)[0]
          else:
            retweetcontent = mblog["retweeted_status"]["text"]
          retweet = {
            "id": retweeted_id,
            "content": retweetcontent,
            "user": retweet_user
          }

        pics = {}
        if "pics" in mblog:
          pics = mblog["pics"]

        # 把信息放入列表
        post_dict = {
          "wb-id": r_since_id,
          "user-id": user_id, 
          "user-name": user_name, 
          "clean_content": sina_text[1],
          "full_content": sina_text[0],
          "related_video": related_video,
          "related_pics": pics,
          "retweet": retweet,
          "forward": mblog['reposts_count'], 
          "comment": mblog['comments_count'], 
          "like": mblog['attitudes_count'], 
          "created at": timestamp, 
          "relative time": mblog['created_at']
        }

        # 检验列表中信息是否完整
        # sina_columns数据格式：['wb-id', 'user-id', 'user-name', 'clean_content', 'full_content', 'related_video', 'related_pics', 'retweet', 'forward', 'comment', 'like', 'created at', 'relative time']
        # 3、保存数据
        save_columns_to_csv(post_dict.values())

        print(f"{r_since_id} created by is finished.")

        # 4、获得最小since_id，下次请求使用
        if min_since_id > r_since_id:
            min_since_id = r_since_id 

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
    except:
        print('爬取信息失败')
        return
    r_json = json.loads(r.text)
    weibo_full_content = r_json['data']['longTextContent']
    clean_content = utils.get_clean_text(weibo_full_content)

    return [weibo_full_content, clean_content]


def save_columns_to_csv(columns, encoding='utf-8'):
    """
    将数据保存到csv中
    数据格式为：['wb-id', 'user-id', 'user-name', 'clean_content', 'full_content', 'related_video', 'related_pics', 'retweet', 'forward', 'comment', 'like', 'created at', 'relative time']
    :param columns: ['wb-id', 'user-id', 'user-name', 'clean_content', 'full_content', 'related_video', 'related_pics', 'retweet', 'forward', 'comment', 'like', 'created at', 'relative time']
    :param encoding:
    :return:
    """
    with open(CSV_FILE_PATH, 'a', encoding=encoding) as csvfile:
        csv_write = csv.writer(csvfile)
        csv_write.writerow(columns)


def patch_spider_user():
    # 爬取前先登录，登录失败则不爬取
    if not login_sina():
        return
    # 写入数据前先清空之前的数据
    if not os.path.exists(CSV_FILE_PATH):
        fields_names = ['wb-id', 'user-id', 'user-name', 'clean_content', 'full_content', 'related_video', 'related_pics', 'retweet', 'forward', 'comment', 'like', 'created at', 'relative time']
        with open(CSV_FILE_PATH, 'a', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fields_names)
            writer.writeheader()
    for i in range(500):
        print('第%d页' % (i + 1))
        spider_user()


if __name__ == '__main__':
    patch_spider_user()
    