#!/usr/bin/env python
# coding: utf-8

# In[13]:


from selenium import webdriver
import time
import pandas as pd


# In[16]:


from selenium.webdriver.common.by import By


# In[17]:


USER = "test_user"
PASS = "test_pw"


# In[8]:


# Chrome 起動
browser = webdriver.Chrome()
browser.implicitly_wait(3)


# In[10]:


# ログインページにアクセス
url_login = "http://kino-code.work/membership-login/"
browser.get(url_login)
time.sleep(3)
print("ログインページにアクセス")


# In[19]:


# ユーザー名を入力
# element = browser.find_element_by_id("swpm_user_name") # 古い書き方
element = browser.find_element(By.ID, "swpm_user_name")
element.clear()
element.send_keys(USER)
# パスワードを入力
# element = browser.find_element_by_id('swpm_password') # 古い書き方
element = browser.find_element(By.ID, "swpm_password")
element.clear()
element.send_keys(PASS)
print("フォームを入力")


# In[20]:


# ログインボタン押下
browser_form = browser.find_element(By.NAME, "swpm-login")
time.sleep(3)
browser_form.click()
print("ログインボタンをクリック")


# In[21]:


# ウェブサイトへアクセス
url = "https://kino-code.work/member-only/"
time.sleep(3)
browser.get(url)
print(url, "ダウンロードページアクセス")


# In[22]:


# XPATH を指定して要素を取得
frm = browser.find_element(By.XPATH, "/html/body/div/div[3]/div/main/article/div/p[2]/button")
time.sleep(1)
frm.click()
time.sleep(5)
print("ダウンロードボタンをクリック")


# In[ ]:




