#!/usr/bin/env python
# coding: utf-8

# 特定のURL配下にあるYouTubeのタイトルとリンクを取得しcsvに書き出すスクリプト

# In[1]:


from bs4 import BeautifulSoup
import requests
import pandas as pd
import time


# In[2]:


url = 'https://kino-code.work/python-super-basic-course/'
r = requests.get(url)
time.sleep(3)


# In[3]:


soup = BeautifulSoup(r.text, 'html.parser')


# In[5]:


# soup


# ##### class を指定して要素を取得

# In[6]:


contents = soup.find(class_="entry-content")


# In[8]:


# contents


# ##### 指定した要素ないの a タグを取得

# In[9]:


get_a = contents.find_all("a")


# In[11]:


# get_a


# ##### a タグ内の url を取得し配列に格納

# In[15]:


title_links = []
for i in range(len(get_a)):
    try:
        link_ = get_a[i].get("href")
        title_links.append(link_)
    except:
        # 例外時
        pass


# In[14]:


# title_links


# #####

# In[16]:


youtube_titles = []
youtube_links = []
for i in range(len(title_links)):
    title_link = title_links[i]
    print("==========")
    print('%s / %s 実行中' % (i+1, len(title_links)))
    print(title_link)
    print("==========")
    r = requests.get(title_link)
    time.sleep(3)
    soup = BeautifulSoup(r.text, 'html.parser')
    youtube_title = soup.find(class_="entry-title").text
    
    if youtube_title == '404 NOT FOUND':
        continue
    else:
        youtube_link = soup.find('iframe')['src'].replace("embed/", "watch?v=")
        
        youtube_titles.append(youtube_title)
        youtube_links.append(youtube_link)


# In[20]:


# youtube_titles


# In[19]:


# youtube_links


# In[21]:


# 辞書型にまとめる
result = {
    'youtube_title': youtube_titles,
    'youtube_link': youtube_links
}


# In[23]:


# result


# ##### csv 出力

# In[24]:


df = pd.DataFrame(result)


# In[26]:


# df


# In[27]:


df.to_csv('./PycharmProjects/web-scraping/result/result.csv', index=False, encoding='utf-8')


# In[ ]:




