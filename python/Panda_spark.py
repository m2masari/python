#!/usr/bin/env python
# coding: utf-8

# 20 milyon satırlı 5 kolonlu bir exceli jupyter ortamında okumam gerekti. Spark dataframe olarak okudum ve postgre'den  gelen tablolarıda spark dataframe cevirdim. Bunları joinledim. Bekledim ama gelmedi:). Spark dataframe'de istediğim performansı alamadım. Bunun üzerine okuduğum exceli ve postgreSQL'den gelen dataları panda dataframe cevirdim ve performans boom:). Panda dataframe veri bilimcilerin analiz edeceği datalar icin çok iyi performansa sahip ve yeterli. Tabi ki büyük veri icin spark dataframe önemli ama veriniz yeterince büyük mü? Verinizin kaynağı ne? Bu sorular cevaplanıp tercih yapılmalı.
# Kodlar asagıdadır.

# In[ ]:


import pandas as pd # Panda dataframe 
import psycopg2  # PostgreSQL sorgusu için
from pandas import DataFrame # PostgreSQL sorgusunu panda dataframe cevirmek icin gerekli


# In[ ]:


# Excelin panda dataframe cekilmesi
colnames=['id', 'price'] 
dfexcel_panda = pd.read_csv("/Documents/df.csv",names=colnames, header=None) 


# In[ ]:


# Excelin spark dataframe cekilmesi
dfexcel_spark= sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true')               .load('/Documents/df.csv')


# In[ ]:


# PostgreSQL sorgusu icin
# Bağlanacagınız makinenin bilgileri
connection = psycopg2.connect(user = "xxx",
                                  password = "xxx",
                                  host = "xxx",
                                  port = "xxx",
                                  database = "xxx");


# In[ ]:


# PostgreSQL sorgusunun çekilmesi
cursor = connection.cursor()
cursor.execute("Postgresql Sorgusu") 


# In[ ]:


# Panda dataframe performans daha iyi
df_panda= DataFrame(cursor.fetchall())


# In[ ]:


# Spark dataframe cevirme kısmı::performans düsüklügü
spark = SparkSession(sc)
df_spark = spark.createDataFrame(cursor.fetchall())  


# In[ ]:


# İki pandanın joinlenmesi
df_final_panda=pd.merge(dfexcel_panda, df_panda, left_on='id', right_on='id')


# In[ ]:


# İki sparkın joinlenmesi::performans düsüklügü
df_final_spark = dfexcel_spark.join(df_spark, dfexcel_spark.id== df_spark.id).drop(df_spark.id)


# In[ ]:


# Pandayı csv olarak  export et
df_final_panda.to_csv('/Documents/df.csv')


# In[ ]:


# Sparkı csv olarak  export et
df_final_spark.coalesce(1).write.format('com.databricks.spark.csv').save('/Documents/df.csv',header = 'true')

