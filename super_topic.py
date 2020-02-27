import json
import time
import random
import sys

import requests

from io_utils import (
    make_weibo_request,
    update_db,
    spider_full_content,
    upload_pics_for_given_weibo
)

# 每次请求中最小的since_id，下次请求使用，新浪分页机制
super_min_since_id = None
# 生成Session对象，用于保存Cookie

s = requests.Session()
count = 0

temp_history_max_id = '4472676012169540'


def spider_topic(post_collection, latest_uid, history_max_id):
    """
    爬取新浪话题
    新浪微博分页机制：根据时间分页，每一条微博都有一个since_id，时间越大的since_id越大
    所以在请求时将since_id传入，则会加载对应话题下比此since_id小的微博，然后又重新获取最小since_id
    将最小since_id传入，依次请求，这样便实现分页
    :return:
    """

    global count
    # 1、构造请求
    global super_min_since_id
    # topic_url = 'https://m.weibo.cn/api/container/getIndex?jumpfrom=weibocom&containerid=1008084882401a015244a2ab18ee43f7772d6f'

    topic_url='https://m.weibo.cn/api/container/getIndex?containerid=1008084882401a015244a2ab18ee43f7772d6f_-_feed&luicode=10000011&lfid=100103type%3D1%26q%3D%E8%82%BA%E7%82%8E%E6%82%A3%E8%80%85%E6%B1%82%E5%8A%A9%E8%B6%85%E8%AF%9D&featurecode=100000'

    '''
    !!! important this should be change to the id of the newest post, check:https://juejin.im/post/5d46adfae51d456201486dcd on how to get this id
    '''

    # use given latest id if specified,
    # otherwise assuming from the newest
    if latest_uid:
        super_min_since_id = str(int(latest_uid) + 1)

    if super_min_since_id:
        topic_url = f"{topic_url}_-_feed&since_id={super_min_since_id}"

    try:
        r = make_weibo_request(s, topic_url)
    except Exception as e:
        print('链接爬取失败', e)
        return False

    # 2、解析数据
    r_json = json.loads(r.text)
    cards = r_json['data']['cards']
    cards = filter(lambda x: 'card_group' in x, cards)
    cards = list(filter(lambda x: x['itemid'] == '', cards))

    # 2.1、第一次请求cards包含微博和头部信息，以后请求返回只有微博信息
    card_group = cards[0]['card_group']
    duplicate_count = 0

    for card in card_group:
        if duplicate_count >= 3:
            print("duplicate exceeds")
            return False

        if 'mblog' not in card:
            continue

        mblog = card['mblog']
        # 2.2、解析微博内容
        r_since_id = mblog['id']

        if r_since_id <= history_max_id:
            print('finish_history')
            return 'finish'

        # update since_id if this is the first run
        if not super_min_since_id:
            super_min_since_id = r_since_id
        elif super_min_since_id == r_since_id:
            duplicate_count += 1
            continue

        content_text = spider_full_content(s, r_since_id)

        if not content_text:
            continue

        count += 1
        sys.stdout.write(f'\r scrawled {r_since_id}, total count {count}\n')
        # sys.stdout.flush()

        # upload pic to cloudinary
        pic_links = upload_pics_for_given_weibo(mblog)

        # 3 Update DB
        update_db(post_collection, mblog, content_text, pic_links)

        # 4、获得最小since_id，下次请求使用
        if super_min_since_id > r_since_id:
            super_min_since_id = r_since_id 

        # 5、爬取用户信息不能太频繁，所以设置一个时间间隔
        time.sleep(random.randint(3, 6))


def patch_super_topic(
        db,
        latest_uid=None,
        max_page=100,
        use_static_end=False
):
    """
        latest_uid: start weibo id, default none
        max_page: max # iterations that will be performed, default 100
        use_static_end: whether use preset ending id, default no
    """

    # need to check empty for new collections
    if use_static_end or db.count() == 0:
        history_max_id = temp_history_max_id
    else:
        history_max_id = list(db.find().sort('wb-id',-1).limit(1))[0]['wb-id']

    for i in range(max_page):
        print('第%d页' % (i + 1))

        if not spider_topic(db, latest_uid, history_max_id):
            continue
        if spider_topic(db, latest_uid, history_max_id):
            break
