import re


herf = r"<a .*?<\/a>"

people_herf = r"<a href='\/n\/.*?<\/a>"

img_r = r'<span class="url-icon"><img alt=.*?<\/span>'

def get_text_without_people_url(text):
    people_url_list = re.findall(people_herf, text)
    clean_text = text
    for url in people_url_list:
      people_name = re.findall(r'\>.*?\<', url)[0][1:-1]
      clean_text = re.sub(url, people_name, clean_text)
    return clean_text

def get_text_without_topic_url(text):
    topic_url_list = re.findall(herf, text)
    clean_text = text
    for url in topic_url_list:
      clean_text = clean_text.replace(url, "")
    return clean_text

def delete_emotion_img(text):
    img_list = re.findall(img_r, text)
    clean_text = text
    for img in img_list:
      clean_text = clean_text.replace(img, "")
    return clean_text

def get_clean_text(text):
    clean_people_url = get_text_without_people_url(text)
    clean_topic_url = get_text_without_topic_url(clean_people_url)
    clean_emotion_img = delete_emotion_img(clean_topic_url)
    clean_br = clean_emotion_img.replace("<br />", "/**/")
    return clean_br