import pandas as pd

def parse_topic():
  f=pd.read_csv("武汉人民买菜情况话题随抓.csv")
  f.drop_duplicates(subset="wb-id", keep = "last", inplace = True)
  keep_col = ['user-name', 'clean_content']
  new_f = f[keep_col]
  new_f.to_csv("武汉人民是怎么购物的话题随抓.csv", index=False)

def parse_people():
    f=pd.read_csv("sina_user.csv")
    keep_col = ['user-name', 'clean_content', 'related_video', 'related_pics']
    new_f = f[keep_col]
    new_f.to_csv("妈妈自缢死因却写呼吸衰竭.csv", index=False)

if __name__ == '__main__':
    parse_topic()