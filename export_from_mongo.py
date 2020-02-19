import pymongo
import csv
import os

def main():
    # setup database
    client = pymongo.MongoClient(f"mongodb+srv://{os.environ['DB_USER']}:{os.environ['DB_PW']}@{os.environ['DB_HOST']}")
    db = client.weibo_topic
    # raw_post = db.raw_post
    raw_post = db.raw_post
    documents = raw_post.find({}, projection={'_id': False}).sort('wb-id', pymongo.DESCENDING)

    fields_names = ['wb-id', 'user-id', 'user-name', 'user-rank', 'clean_content', 'full_content', 'forward', 'comment', 'like', 'created at', 'relative time']
    with open('./cov.csv', 'a', encoding='utf-8') as csvfile:
        csv_writer = csv.DictWriter(csvfile, fieldnames=fields_names)
        csv_writer.writeheader()
        csv_writer.writerows(documents)

if __name__ == '__main__':
    main()