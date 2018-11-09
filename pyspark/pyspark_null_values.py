
# coding: utf-8

# # PYSPARK'DA NULL DEĞERLERİN YÖNETİLMESİ
# <font size=4,font style=arial>
# <br><br>
# Bu yazımızda Pyspark'da veri setlerinde karşımıza çıkabilen null değerler nasıl yönetilir ile aşağıda ki sorulara cevap arayacağız ;<br><br>
# 
# -Satır satır veriyi inceleyip hiç null değeri olmayan satırlar nasıl gösterilir?<br><br>
# -Satır satır veriyi inceleyip n ve n'den fazla null değeri olmayan satırlar nasıl gösterilir?<br><br>
# -İstenilen bir kolon için null değerler/null olmayan değerler nasıl filtrelenir?<br><br>
# -Null değerleri herhangi bir numerik değerle (eğer kolon numerik ise) veya string bir değer ile (eğer ki kolon string ise)  değer ile nasıl değiştirilir?<br><br>
# -Null değerler ortalama ve medyan değerleri ile nasıl değiştirilir?<br><br>
# -Null değerler, avg ve count hesaplamalarını nasıl etkiler? Averaj hesabında null değerleri hesaba katılır mı?
# </font>

# In[1]:


##Spark session oluşturulur
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)


# <font size=4,font style=arial>
# <br>
# Data setimizi manual oluşturalım. İki şekilde oluşturabiliriz. Birincisi StructType kullanarak ikincisi klasik şekilde.
# </font>

# In[2]:


##true for nullable, and false for not nullable
from pyspark.sql.types import *
schema = StructType([StructField('Id',IntegerType(), True),
                     StructField('Name',StringType(), True),
                     StructField('Height', DoubleType(),True),
                     StructField('Age', IntegerType(),True),
                     StructField('Gender', StringType(),True),
                     StructField('Profession', StringType(),True)])
 


# In[3]:


df = spark.createDataFrame([(1, 'John', 1.79, 28,'M', 'Doctor'),
                            (2, 'Steve', 1.78, 45,'M', None),
                            (3, 'Emma', 1.75, None, None, None),
                            (4, 'Ashley',1.6, 33,'F', 'Analyst'),
                            (5, 'Olivia', 1.8, 54,'F', 'Teacher'),
                            (6, 'Hannah', 1.82, None, 'F', None),
                            (7, 'William', 1.7, 42,'M', 'Engineer'),
                            (None,None,None,None,None,None),
                            (8,'Ethan',1.55,38,'M','Doctor'),
                            (9,'Hannah',1.65,None,'F','Doctor'),
                            (10,'Xavier',1.64,43,None,'Doctor'),
                            (11,None,None,None,None,None)],schema)


# <font size=4,font style=arial>
# <br>
# Aşağıda klasik şekilde oluşturulan yol mevcut.
# </font>

# In[34]:


##
df = spark.createDataFrame([(1, 'John', 1.79, 28,'M', 'Doctor'),
                            (2, 'Steve', 1.78, 45,'M', None),
                            (3, 'Emma', 1.75, None, None, None),
                            (4, 'Ashley',1.6, 33,'F', 'Analyst'),
                            (5, 'Olivia', 1.8, 54,'F', 'Teacher'),
                            (6, 'Hannah', 1.82, None, 'F', None),
                            (7, 'William', 1.7, 42,'M', 'Engineer'),
                            (None,None,None,None,None,None),
                            (8,'Ethan',1.55,38,'M','Doctor'),
                            (9,'Hannah',1.65,None,'F','Doctor'),
                            (10,'Xavier',1.64,43,None,'Doctor'),
                            (11,None,None,None,None,None)]
                           , ['Id', 'Name', 'Height', 'Age', 'Gender', 'Profession'])


# <font size=4,font style=arial>
# <br>
# data type'ı ve kolonları görelim
# </font>

# In[5]:


print(df)


# <font size=4,font style=arial>
# <br>
# Kolonları ve tiplerini görelim
# </font>

# In[6]:


df.printSchema()


# <font size=4,font style=arial>
# <br>
# dataframe'i görelim
# </font>

# In[7]:


df.show()


# 
# <font size=4,font style=arial>
# <br>
# Bütün kolonlarda dolu olan(hiç null değeri olmayan) satır sayısı kaç?
# </font>

# In[30]:


df.dropna().count()


# <font size=4,font style=arial>
# <br>
# Bu satırları göster
# </font>

# In[35]:


df.dropna(how = "any").show()


# <font size=4,font style=arial>
# <br>
# Eğer satırda bütün değerler null ise o satırı getirme demektir.
# </font>

# In[36]:


df.show()
df.dropna(how = "all").show()


# <font size=4,font style=arial>
# <br>
# Null değerleri 0 la doldur ama bunu sadece numerik kolonlar için yapar string kolonlar doldurulmaz.
# </font>

# In[11]:


df.fillna(0).show()


# <font size=4,font style=arial>
# <br>
# string kolonlardaki null değerleri Profession ile doldurur
# </font>

# In[12]:


df.fillna('Profession').show()


# <font size=4,font style=arial>
# <br>
# Burada sadece Profession kolonunda ki Null değerleri Free ile doldurur
# </font>

# In[13]:


df.fillna('Free', subset=['Profession']).show()


# <font size=4,font style=arial>
# <br>
# Bütün satırlarda 2 ve 2'den fazla null olmayan değer varsa o satırı göster 
# </font>

# In[16]:


ff1=df.dropna(thresh=2)
ff1.show()


# <font size=4,font style=arial>
# <br>
# Height, Age, Gender, Profession kolonlarında toplamda 2 ve 2'den az Null değeri olan satırları al
# </font>

# In[17]:


ff2=df.dropna(thresh=2,subset=('Height','Age','Gender','Profession'))
ff2.show()


# <font size=4,font style=arial>
# <br>
# Profession'da null olmayan değerleri Occupied null ise Unknown yaz
# </font>

# In[18]:


from pyspark.sql.functions import when   
yeni=df.withColumn('Profession', when(df.Profession.isNotNull(), 'Occupied').otherwise('Unknown'))
yeni.show()


# <font size=4,font style=arial>
# <br>
# "Gender", "Profession", "Id", "Name" kolonlarını hariç tutarak , numerik kolonlar için Null değerleri ortalama ile değiştir 
# </font>

# In[19]:


from pyspark.sql.functions import *
def fill_with_mean(this_df, exclude=set()):

    stats = this_df.agg(*(avg(c).alias(c) for c in this_df.columns if c not in exclude))
    return this_df.na.fill(stats.first().asDict())

fill_with_mean(df, ["Gender", "Profession", "Id", "Name"]).show()


#  <font size=4,font style=arial>
# <br>
# Kolon tipi bigint,double,int olan kolonlardaki null değerleri medyan ile değiştir 
# </font>
# </font>

# In[20]:


num_cols = [col_type[0] for col_type in filter(lambda dtype: dtype[1] in {"bigint", "double", "int"}, df.dtypes)]
### Compute a dict with <col_name, median_value>
median_dict = dict()
for c in num_cols:
   median_dict[c] = df.stat.approxQuantile(c, [0.5], 0.001)[0]
df_imputed=df.na.fill(median_dict)
df_imputed.show()


# <font size=4,font style=arial>
# <br>
# Null değerler, ortalama hesabında hiçbir şekilde hesaba katılmaz
# </font>

# In[21]:


df.agg(mean("Height")).show()


# <font size=4,font style=arial>
# <br>
# Height'ı null gelen satırlar hangileridir. Bunun için sql functions'ı yüklememiz gerekiyo
# </font>

# In[22]:


from pyspark.sql import functions as F
df.filter(F.isnull("Height")).show()


# <font size=4,font style=arial>
# <br>
# Height'ı null olmayan veya boş olmayan satırlar hangileridir(bizim dataframe'inde boş değilde null değerleri var. Bazı durumlarda boş değerler ilede karşılaşılabilir. 
# </font>

# In[23]:


df.filter((df.Height.isNotNull()) | (df.Height != "")).show()


# <font size=4,font style=arial>
# <br>
# boş olmayan değerlerin ortalaması default oralama ile aynı olması gerekir
# </font>

# In[24]:


df.agg(mean("Height")).show()


# In[25]:


df.filter((df.Height.isNotNull()) | (df.Height != "")).agg(mean("Height")).show()


# <font size=4,font style=arial>
# <br>
# Age 30'den büyük kolonların ortalamasını al
# </font>

# In[26]:


df.filter(df['Age'] > 30).agg({"Age": "avg"}).show()


# <font size=4,font style=arial>
# <br>
# Profession'a göre yaş ortalamasını ve sayısını,toplamını al(null değerler ayrı değerlenidirilir
# </font>

# In[27]:


import pyspark.sql.functions as f
df.groupBy("Profession").agg(f.mean("Age").alias("Mean"),f.count("Age").alias("Count")
                            ,f.sum("Age").alias("Sum")).show()


# <font size=4,font style=arial>
# <br>
# Gendera göre age ortalamasını ve null olmayan professionı say
# </font>

# In[28]:


df.show()
df.groupBy("Gender").agg({'Age':'avg', 'Profession':'count'}).show()


# <font size=4,font style=arial>
# <br>
# 40'dan büyük kolonları 1 ile değiştir yoksa 0 ile değiştirelim
# </font>

# In[29]:


from pyspark.sql.functions import when   
cols = df.columns # list of all columns
for col in cols:df= df.withColumn(col, when(df[col]>40,1).otherwise(0))
df.show()

