#!/usr/bin/env python
# coding: utf-8

# Asagıda tanımladığımız dictionary'yi panda dataframe'ine cevirip daha sonra drop column,null değerleri drop etme, kolon adlarını değiştirme, kolon ekleme gibi işlemler yapacağız. En sonda bütün işlemleri tek bir code'da yapacağız.

# In[45]:


# Kütüphanemizi yükleyelim
import numpy as np
import pandas as pd

# Dictionary formatında datamızı girelim
Houses = {
    'Houses': ['House1', 'House2', 'House3', 'House4'],
    'Size': [100,80,50,np.nan],
    'Price': [18000, 250000, 300000, 500000],
    'RoomCount': ['3+1',np.nan,'1+1','3+1']
}


# In[46]:


Houses


# In[47]:


#Dictionary formatını panda dataframe'e çevirelim
df = pd.DataFrame.from_dict(Houses)
df


# In[48]:


# Price Kolonunu  silelim
del df['Price']
df


# In[49]:


# Null değerleri olan satırları drop edelim. Öncelikle boş hücreyi nan ile değiştirmemiz gerekir.
df = df.dropna(subset=['Size', 'RoomCount'])
df


# In[50]:


#Size kolonunu m2 ile RoomCount kolonunu oda sayısı ile değiştirelim
df = df.rename(
    {
        'Size': 'm2',
        'RoomCount': 'OdaSayısı',
    },
    axis=1,
)
df


# In[51]:


# Baska bir kolon ekleyelim
df['BulunduguKat'] = [1,5]
df


# Asagıda tek bir code'da bütün işlemleri birleştireceğiz.

# In[52]:


df = (
    pd.DataFrame(Houses)
    .drop(columns="Price")
    .dropna(subset=['Size', 'RoomCount'])
    .rename(columns={"Size": "m2", "RoomCount": "OdaSayısı"})
    .assign(BulunduguKat=[1,5])
    )
df

