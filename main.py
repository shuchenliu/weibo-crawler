import re
import os
import csv
import json
import time
import random
from pymongo import MongoClient
from datetime import datetime

import requests

# 每次请求中最小的since_id，下次请求使用，新浪分页机制
min_since_id = ''
# 生成Session对象，用于保存Cookie
s = requests.Session()
# 新浪话题数据保存文件
CSV_FILE_PATH = './sina_topic.csv'


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


def spider_topic(post_collection):
    """
    爬取新浪话题
    新浪微博分页机制：根据时间分页，每一条微博都有一个since_id，时间越大的since_id越大
    所以在请求时将since_id传入，则会加载对应话题下比此since_id小的微博，然后又重新获取最小since_id
    将最小since_id传入，依次请求，这样便实现分页
    :return:
    """

    # scheme: "https://m.weibo.cn/status/ItKnVeAKZ?mblogid=ItKnVeAKZ&luicode=10000011&lfid=1008084882401a015244a2ab18ee43f7772d6f_-_feed"
    #4470898193477889

    # 1、构造请求
    global min_since_id
    topic_url = 'https://m.weibo.cn/api/container/getIndex?jumpfrom=weibocom&containerid=1008084882401a015244a2ab18ee43f7772d6f_-_feed'
    if not min_since_id:
        min_since_id='4470931667569516'
       
    topic_url = f"{topic_url}&since_id={min_since_id}"
    kv = {'user-agent': 'Mozilla/5.0',
          'Referer': 'https://m.weibo.cn/p/1008087a8941058aaf4df5147042ce104568da/super_index?jumpfrom=weibocom'}

    try:
        r = s.get(url=topic_url, headers=kv)
        r.raise_for_status()
    except:
        print('爬取失败')
        return
    # 2、解析数据
    r_json = json.loads(r.text)
    cards = r_json['data']['cards']
    # 2.1、第一次请求cards包含微博和头部信息，以后请求返回只有微博信息
    card_group = cards[2]['card_group'] if len(cards) > 1 else cards[0]['card_group']
    for card in card_group:
        # 创建保存数据的列表，最后将它写入csv文件
        sina_columns = []
        mblog = card['mblog']
        # 2.2、解析微博内容
        r_since_id = mblog['id']
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

        # 把信息放入列表
        post_dict = {
          "wb-id": r_since_id,
          "user-id": user_id, 
          "user-name": user_name, 
          "user-rank": user_rank, 
          "content": sina_text, 
          "forward": mblog['reposts_count'], 
          "comment": mblog['comments_count'], 
          "like": mblog['attitudes_count'], 
          "created at": timestamp, 
          "relative time": mblog['created_at']
        }

        # 检验列表中信息是否完整
        # sina_columns数据格式：['wb-id', 'user-id', 'user-name', 'user-rank', 'content', 'forward', 'comment', 'like', 'created at', 'relative time']
        # 3、保存数据
        save_columns_to_csv(post_dict.values())
        post_collection.insert_one(post_dict)

        # 4、获得最小since_id，下次请求使用
        if min_since_id:
            min_since_id = r_since_id if min_since_id > r_since_id else min_since_id
        else:
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
    # clean_content = weibo_full_content
    # if weibo_full_content.startswith('<a  href=', 0):
    #   clean_content = clean_content.split('</a>')[1]
    # if len(weibo_full_content.split('<a data-url')) > 1:
    #   clean_content = clean_content.split('<a data-url')[0]
    # return clean_content
    return weibo_full_content

def spider_user_info(uid) -> list:
    """
    爬取用户信息（需要登录），并将基本信息解析成字典返回
    :return: ['用户名', '性别', '地区', '生日']
    """
    user_info_url = 'https://weibo.cn/%s/info' % uid
    kv = {'user-agent': 'Mozilla/5.0'}
    try:
        r = s.get(url=user_info_url, headers=kv)
        r.raise_for_status()
    except:
        print('爬取用户信息失败')
        return
    # 使用正则提取基本信息
    basic_info_html = re.findall('<div class="tip">基本信息</div><div class="c">(.*?)</div>', r.text)
    # 提取：用户名、性别、地区、生日 这些基本信息
    basic_infos = get_basic_info_list(basic_info_html)
    return basic_infos


def get_basic_info_list(basic_info_html) -> list:
    """
    将html解析提取需要的字段
    :param basic_info_html:
    :return: ['用户名', '性别', '地区', '生日']
    """
    basic_infos = []
    basic_info_kvs = basic_info_html[0].split('<br/>')
    print(basic_info_kvs)
    for basic_info_kv in basic_info_kvs:
        if basic_info_kv.startswith('昵称'):
            basic_infos.append(basic_info_kv.split(':')[1])
        elif basic_info_kv.startswith('性别'):
            basic_infos.append(basic_info_kv.split(':')[1])
        elif basic_info_kv.startswith('地区'):
            area = basic_info_kv.split(':')[1]
            # 如果地区是其他的话，就添加空
            if '其他' in area or '海外' in area:
                basic_infos.append('')
                continue
            # 浙江 杭州，这里只要省
            if ' ' in area:
                area = area.split(' ')[0]
            basic_infos.append(area)
        elif basic_info_kv.startswith('生日'):
            birthday = basic_info_kv.split(':')[1]
            # 19xx 年和20xx 带年份的才有效，只有月日或者星座的数据无效
            if birthday.startswith('19') or birthday.startswith('20'):
                # 只要前三位，如198、199、200分别表示80后、90后和00后，方便后面数据分析
                basic_infos.append(birthday[:3])
            else:
                basic_infos.append('')
        else:
            pass
    # 有些用户的生日是没有的，所以直接添加一个空字符
    if len(basic_infos) < 4:
        basic_infos.append('')
    return basic_infos


def save_columns_to_csv(columns, encoding='utf-8'):
    """
    将数据保存到csv中
    数据格式为：['w-id', 'latest update', 'user-id', 'user-name', 'user-rank', 'content', 'forward', 'comment', 'like', 'created at', 'relative time']
    :param columns: ['w-id', 'latest update', 'user-id', 'user-name', 'user-rank', 'content', 'forward', 'comment', 'like', 'created at', 'relative time']
    :param encoding:
    :return:
    """
    with open(CSV_FILE_PATH, 'a', encoding=encoding) as csvfile:
        csv_write = csv.writer(csvfile)
        csv_write.writerow(columns)


def patch_spider_topic():
    # setup database
    client = MongoClient(f"mongodb+srv://{os.environ['DB_USER']}:{os.environ['DB_PW']}@{os.environ['DB_HOST']}")
    db = client.weibo_topic
    post = db.post

    # 爬取前先登录，登录失败则不爬取
    if not login_sina():
        return
    # 写入数据前先清空之前的数据
    if not os.path.exists(CSV_FILE_PATH):
        fields_names = ['wb-id', 'latest update', 'user-id', 'user-name', 'user-rank', 'content', 'forward', 'comment', 'like', 'created at', 'relative time']
        with open(CSV_FILE_PATH, 'a', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fields_names)
            writer.writeheader()
    for i in range(1000):
        print('第%d页' % (i + 1))
        spider_topic(post)


if __name__ == '__main__':
    patch_spider_topic()