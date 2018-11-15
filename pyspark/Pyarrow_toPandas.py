#!/usr/bin/env python
# coding: utf-8

# # Pypsark Dataframe'ini Panda Dataframe'ine çevirirken yaşanan performans sıkıntısına bir çözüm: Pyarrow Modulü 
#  <br>
# <font size=4,font style=arial>
#  Bu yazımızda oluşturduğumuz pyspark dataframe'ini panda daraframe'ine çevirirken yaşanan bekleme süresini %80 <br> 
#  iyileştiren bir modül olan Pyarrow modülünü test edeceğiz. Öncelikle Windows Command penceresinde "pip install <br> 
#  pyarrow" yazıp bu modülü kurmanız gerekmektedir. <br> 
# Not:Kurmadan önce bilgisayarınız 64 bit ve sizde python 32 bit kurulmuşsa bu problem. Python 64 bit için olanı <br>
#   kurmanız gerekiyor. Bu düzeltme yapılmazsa Pyarrow paketi kurulmuyor.<br>   
# </font>   

# <font size=4,font style=arial>
# Sparksession ve sparkcontext'i oluşturalım. Round,pandas,randn fonskiyonlarını yükleyelim
# </font> 

# In[2]:


#for the round function
import pyspark.sql.functions as func
#for pandas
import pandas as pd
#for randn 
from pyspark.sql.functions import randn
##create spark session
from pyspark import SparkContext
from pyspark.sql import SQLContext
sc =SparkContext()
sqlContext = SQLContext(sc)
from pyspark.sql.session import SparkSession
spark = SparkSession(sc)


# <font size=4,font style=arial>
# 5 tane normal dağılımlı(mean=0,std=1) kolona sahip olan veri setimizi oluşturalım.
# </font>  

# In[3]:


df1 = sqlContext.range(0, 4000000)     .withColumn('normal1',func.abs(func.round(100*randn(seed=1),2)))     .withColumn('normal2',func.abs(func.round(100*randn(seed=2),2)))     .withColumn('normal3',func.abs(func.round(100*randn(seed=3),2)))     .withColumn('normal4',func.abs(func.round(100*randn(seed=4),2)))     .withColumn('normal5',func.abs(func.round(100*randn(seed=5),2)))


# <font size=4,font style=arial>
# Sparkcontext'in içeriği aşağıda ki şekilde görülebilir. Spark UI tıklanırsa çalışılan jop'lar görüleiblir. Master makinem kendi lokalim.
# </font>  

# In[4]:


sc


# <font size=4,font style=arial>
# df1 dataframe'ini panda dataframe'i yapalım ve süreyi görelim.
# </font> 

# In[5]:


get_ipython().run_line_magic('time', 'pdf = df1.toPandas()')


# <font size=4,font style=arial>
# Şimdi pyarrow paketini enable yapıp tekrar panda dataframe'ine çevirip süreyi görelim
# </font> 

# In[6]:


spark.conf.set("spark.sql.execution.arrow.enabled", "true")


# In[7]:


get_ipython().run_line_magic('time', 'pdf = df1.toPandas()')


# <font size=4,font style=arial>
# Sürede %80'lik bir iyileştirme var ki bu süper bir rakam.
# </font> 

# <font size=4,font style=arial>
# pdf panda dataframe'i df1 ise spark dataframe'idir. Panda'da head, sparkda show ile dataframe görülebilir.
# </font> 

# In[8]:


pdf.head(5)


# In[9]:


df1.show(5)


# <font size=4,font style=arial>
# pdf panda dataframe'i olduktan sonra panda paketinin esnekliğinden faydalanarak her türlü veri manipulasyonu yapılabilir.
# </font> 

# In[10]:


pd.set_option('float_format', '{:f}'.format)


# <font size=4,font style=arial>
# Mesela describe
# </font> 

# In[11]:


pdf.describe()

