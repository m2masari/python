#!/usr/bin/env python
# coding: utf-8

# # Panda dataframe'inde normalizasyon nasıl yapılır?
# Bazı değişkenleri analiz ederken normalizasyon yapmak önemlidir. Örnek olarak fiyat ve m2. Fiyat binlerle ifade edilirken m2 yüzler ölçeğindedir. Uzaklık mesafeside yine km olarak farklı bir ölçektedir. Bu 3 değeri birbiri ile ölçerken veya birbiri arasında yakınlık ölçerken normalizasyon ile [0,1] arasına çekilmelidir.<br>
# Bu yazımımızda aşağıdaki iki  normalizasyon denklemini  kullanacağız.<br>
# 1)  (max(x)-x)/(max(x))<br>
# 2)  (x-min(x))/(max(x)-min(x))

# Gerekli paketleri öncelikle yükleyelim

# In[1]:


import pandas as pd # Panda dataframe'i icin gerekli
from pandas import DataFrame 
import warnings
warnings.filterwarnings('ignore')


# Dataframe'mimiz aşağıdadır.Group_Id ile Group_Id1 arasındaki prize,size ve distance'ı normalize edelim.

# In[11]:


data = [['1020',12000,100,'1060',15000,120,1.2],
        ['1020',12000,100,'1050',20000,75,0.9],
        ['1020',12000,100,'1030',10000,90,2.3],
        ['1030',5000,150,'1040',10000,300,3.7],
        ['1030',5000,150,'1080',2500,60,0.3]]
df = pd.DataFrame(data, columns = ['Group_Id', 'Price','Size','Group_Id1', 'Price1','Size1','distance']) 
 
# print dataframe. 
df


# Öncelikle Group_Id bazında Price1 ve Size1'in ortalamalarını kontrol edelim

# In[12]:


df_avg=df.groupby('Group_Id',as_index=False).agg({'Price1' : 'mean', 'Size1' : 'mean', 'distance': 'mean'})
df_avg.columns = ['Group_Id', 'Price_mean', 'Size_mean','distance_mean']
df_avg.head(10)


# Price'ın ve Size'ın farklarını alalım. Distance iki item arasında ki fark olduğundan farkı alınmadı

# In[13]:


df['Price_dif'] = (df['Price'] - df['Price1']).abs()
df['size_dif'] = (df['Size'] -df['Size1']).abs()
df.head(10)


# Price_dif ve size_dif  üzerine x-min(x))/(max(x)-min(x))) formulü group_id bazında uygulanmıştır.

# In[14]:


df_score=df[['Group_Id','Price_dif','size_dif','distance']]
df_score[['Price_dif_score', 'Size_dif_score','distance_score']]= df_score.groupby('Group_Id')    .transform(lambda x: (x-x.min())/(x.max()-x.min()))
df_score.head()


# Asağıda apply fonksiyonu kullanaraktan normalizasyon yapılmıştır.

# In[15]:


cols = ['Price_dif','size_dif','distance']
df[cols] = df.groupby('Group_Id')[cols].apply(lambda x: (x-x.min())/(x.max()-x.min()))
df.head()


# Aşağıda farklı bir yolla normalizasyon yapılmıştır

# In[16]:


df_score=df[['Group_Id','Price_dif','size_dif','distance']]
dist =  df_score.groupby('Group_Id').transform('max')
df_score1 = df_score.join(dist.sub(df_score.drop('Group_Id', axis=1)).div(dist).add_suffix('_score'))            .drop(['Price_dif','size_dif','distance'], axis=1)
df_score1.head()


# Aşağıda Group bazında normalizasyon yapılmıştır ve kullanılan formül (x-min(x))/(max(x)-min(x))'dir

# In[25]:


data = [['Group 1',10,100],
       ['Group 1',28,80],
       ['Group 1',15,60],
       ['Group 1',30,120],
       ['Group 2',10,120],
       ['Group 2',20,130],
       ['Group 2',30,200],
       ['Group 2',40,250],
       ['Group 2',50,300]] 
df = pd.DataFrame(data, columns = ['Group','price','size']) 


# In[24]:


df[['normalized_price', 'normalized_size']]= df.groupby('Group').transform(lambda x: (x - x.min())/ (x.max() - x.min()))
df


# In[26]:


df[['normalized_price', 'normalized_size']]= df.groupby('Group').transform(lambda x: (x.max() - x)/ x.max())
df

