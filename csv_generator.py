# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.4'
#       jupytext_version: 1.2.3
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

import re
import pandas as pd

regexes = {
    'name': '姓名】(.*?)【',
    'age': '年龄】(.*?)【',
    'city': '所在城市】(.*?)【',
    'address': '所在小区、社区】(.*?)【',
    'disease-course': '患病时间】(.*?)【',
    'contact': '联系方式】(.*?)【',
    'emergency-contact': '其他紧急联系人】(.*?)【',
    'description': '病情.*描述】(.*)。',
}

styles = [
    dict(selector='th', props=[('text-align', 'left'), ('max-width', '300px'), ('word-wrap', 'break-word')]),
    dict(selector='td', props=[('text-align', 'left'), ('max-width', '300px'), ('word-wrap', 'break-word')])
]

def parse_content(content, regexes):
    parsed_content = {}
    for field, regex in regexes.items():
        matched = re.search(regex, content, re.IGNORECASE)
        parsed_content[field] = matched[1] if matched else ''
        parsed_content[field] = re.sub('[/*]', '', parsed_content[field])

    # In case parser fails, no data; set it to content
    if not parsed_content['name'] and not parsed_content['description']:
        parsed_content['description'] = content
    return pd.Series(parsed_content)

# +
# We cannot show PII fields values directly, at least need some masking
def add_mask(value, length):
    if not value:
        return value
    return value[:length] + '**'


def csv_generator(documents, file_name):

    df = pd.DataFrame(list(documents))

    parsed_df = df.join(df['clean_content'].apply(lambda x: parse_content(x, regexes)))
    parsed_df['created-at'] = pd.to_datetime(parsed_df['created at'], unit='s')
    parsed_df = parsed_df.drop(columns=['clean_content', 'full_content', 'relative time', 'created at'])

    parsed_df['name'] = parsed_df['name'].apply(lambda x:add_mask(x,1))
    parsed_df['address'] = parsed_df['address'].apply(lambda x:add_mask(x,8))
    parsed_df['contact'] = parsed_df['contact'].apply(lambda x:add_mask(x,5))
    parsed_df['emergency-contact'] = parsed_df['emergency-contact'].apply(lambda x:add_mask(x,5))

    parsed_df = parsed_df.reindex(columns=[
        'name',
        'age',
        'city',
        'address',
        'disease-course',
        'contact',
        'emergency-contact',
        'description',
        'created-at',
        'wb-id',
        'user-id',
        'user-name',
        'user-rank',
        'forward',
        'comment',
        'like'
    ]).rename(columns={
        'name': 'name (姓名)',
        'age': 'age (年龄)',
        'city': 'city (所在城市)',
        'address': 'address (所在小区、社区)',
        'disease-course': 'disease_course (患病时间)',
        'contact': 'contact (联系方式)',
        'emergency-contact': 'emergency_contact (其他紧急联系人)',
        'description': 'description (病情描述)'
    })

    keep_col = ['user-name', 'name (姓名)' , 'age (年龄)', 'city (所在城市)', 'address (所在小区、社区)', 'disease_course (患病时间)', 'description (病情描述)']
    new_f = parsed_df[keep_col]
    new_f.to_csv(f"{file_name}.csv", index=False)