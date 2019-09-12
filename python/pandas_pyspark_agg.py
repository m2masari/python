#!/usr/bin/env python
# coding: utf-8

# In[18]:


import pandas as pd


# Group'a göre max,mean,min alalım. Aggregate fonksiyonu ile group by yapıp kolonları rename yapacağız.
# Oncelikle panda dataframinde nasıl yapılacağını gösterelim.

# In[19]:


df=  [['A',100,10000]
     ,['A',120,15000]
     ,['A',300,50000]
     ,['B',100,180000]
     ,['B',80,200000]]

# panda dataframe'i oluşturalım
df = pd.DataFrame(df, columns = ['group','size','amount']) 


# In[20]:


df


# In[21]:


df1=df.groupby(['group'])       .agg({'amount':{'amount_max': 'max','amount_mean': 'mean','amount_min': 'min'}       ,'size':{'size_max': 'max','size_mean': 'mean', 'size_min': 'min'}})        .reset_index() 
df1.columns = df1.columns.droplevel(0)
df1


# Aşağıdada Spark data frameinde nasıl yapılacağına bakalım. 

# In[34]:


import numpy as np
import os
import findspark
import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import count,min, max,mean
import pyspark.sql.functions as func


# In[9]:


os.environ["PYSPARK_PYTHON"]=""
os.environ['PYSPARK_SUBMIT_ARGS'] = ''
sc = SparkContext(appName='sparkframe')


# In[10]:


spark = SparkSession(sc)


# In[63]:


df = spark.createDataFrame([('A',100,10000),
                            ('A',120,15000),
                            ('A',300,50000),
                            ('B',100,180000),
                            ('B',80,200000)]
                           , ['group', 'size', 'amount'])


# In[64]:


df.show()


# In[65]:


df=df.groupby('group').agg(max("amount"),mean("amount"),min("amount")                             ,max("size"),mean("size"),min("size"))         .withColumnRenamed("max(amount)", "amount_max")         .withColumnRenamed("mean(amount)", "amount_mean")         .withColumnRenamed("min(amount)", "amount_min")         .withColumnRenamed("max(size)", "size_max")         .withColumnRenamed("mean(size)", "size_mean")         .withColumnRenamed("min(size)", "size_min") 


# In[66]:


df.show()

