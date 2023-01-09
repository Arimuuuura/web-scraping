#!/usr/bin/env python
# coding: utf-8

# In[22]:


import pandas as pd
from bs4 import BeautifulSoup
import urllib.request as req


# In[3]:


url = "https://kino-code.work/python-scraping/"
response = req.urlopen(url)


# In[4]:


# HTML のパース
parser_html = BeautifulSoup(response, 'html.parser')


# In[10]:


# 全ての a タグを取得
title_lists = parser_html.find_all('a')


# In[17]:


# title, url の配列をそれぞれ作成
title_list = []
url_list = []

for i in title_lists:
    title_list.append(i.string)
    url_list.append(i.attrs['href'])


# In[23]:


# DataFrame の作成
df_title_url = pd.DataFrame({'Title': title_list, 'URL': url_list})


# In[26]:


# None を除いた配列の作成(欠損値を削除)
df_notnull = df_title_url.dropna(how='any')


# In[29]:


# 特定の文字を含む配列の作成
df_contain_python = df_notnull[df_notnull['Title'].str.contains('Python超入門コース')]


# In[32]:


# csvに書き出し
df_contain_python.to_csv('./PycharmProjects/web-scraping/output/output.csv')


# In[ ]:
