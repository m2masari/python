
# coding: utf-8

# # PYSPARK VERİ MANİPULASYONU VE MAKİNA ÖĞRENMESİ METHODU 
# <font size=4,font style=arial>
# <br><br>
# Merhaba,<br>
# Pyspark kodları ile önce veri manipulasyonu yapacağız. İki tane veri setini rastgele sayılarla oluşturup join yapıp ,aşağıdaki fonksiyonları ve metodları uygulayacağız:  <br>
# 1-Case when <br>
# 2-Distinct <br>
# 3-Kolon adları değiştirme <br>
# 4-Kolonların min,max,avg,sum,count,standard deviation ı bulma<br>
# 5-Group by sum,count,mean <br>
# 6-Filter <br>
# 7-Rank <br>
# 8-Makine Öğrenmesi methodu olan Lojistik Regresyon Analizi <br>
# </font>

# <font size=4,font style=arial>
# Öncelikle gerekli fonksiyonları yükleyelim ve spark contexti oluşuralım
# </font>

# In[1]:


#round fonksiyonu  için
import pyspark.sql.functions as func
##Spark session oluşturulur
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import rand, randn,when,mean, min, max,sum,stddev,count,desc,col
from pyspark.sql.window import Window
sc =SparkContext()
sqlContext = SQLContext(sc)


# <font size=4,font style=arial>
# <br>
# İlk veri setimizi(data frame) oluşturalım ve adı df1 olsun. 1 milyon satırlı 5 kolonlu oluşturalım. Kolonlar;2 'si uniform dağılım, 2'si normal dağılım ve bir kolonda 0,1 sayıları içerecek şekilde olsun.<br>
# id:id kolonu<br>
# uniform: uniform dağılımlı kolon<br>
# uniform1: uniform dağılımlı kolon<br>
# normal: normal dağılımlı kolon<br>
# normal1: normal dağılımlı kolon<br>
# Y:0,1 içeren kolon<br>
# NOT:func.round oluşturduğumzu rastgele sayıları yuvarlamak içindir <br>
# NOT: Sparksjobs ları http://localhost:4040/jobs/ 'den takip edebilirsiniz    
# </font>

# In[33]:


df1 = sqlContext.range(0, 1000000)     .withColumn('uniform', func.round(rand(seed=10),2))     .withColumn('uniform1',func.round(rand(seed=9),2))     .withColumn('normal',func.round(randn(seed=22),2))     .withColumn('normal1',func.round(randn(seed=23),2))     .withColumn('Y', when(rand() > 0.5, 1).otherwise(0))


# <font size=4,font style=arial>
# df1 satır sayısı
#     </font>

# In[34]:


#df1 satır sayısı
df1.count()


# <font size=4,font style=arial>
# İkinci veri setimiz df2 olsun.
# <br>    
# id=id kolunu
#     <br>
# uniform2: uniform dağılımlı kolon
# <br>
# normal2: normal dağılımlı kolon
# <br>
# normal3: normal dağılımlı kolon
# </font>

# In[35]:


df2 = sqlContext.range(0, 1000000)     .withColumn('uniform2', func.round(rand(seed=9),2))     .withColumn('normal2', func.round(randn(seed=3),2))     .withColumn('normal3', func.round(randn(seed=5),2))


# <font size=4,font style=arial>
# df2 satır sayısı
# </font>

# In[36]:


df2.count()


# <font size=4,font style=arial>
# df1 verisi için en tepedeki 5 satırı görelim
# </font>

# In[37]:


df1.show(5)


# <font size=4,font style=arial>
# df2'den de bir örnek alalım. 5'tanesini görelim. (Set seed sürekli aynı örneklemin gelmesini sağlar.)
# </font>

# In[38]:


df2.sample(False, 0.1, seed=0).limit(5).show()


# # CASE WHEN 
# <br>
# <br>
# <font size=4,font style=arial>
# df1'e C1 adında bir kolon ekleyelim.Case when 'li kurallarımız;
# <br>
# df1.normal <= 0.1 ise A <br>
# df1.normal > 0.1 ve df1.normal =< 0.7 ise 'B' <br> 
# df1.normal > 0.7 ve df1.normal =< 1  ise 'C'  
# df1.normal > 1 ve df1.normal<1.3 ise 'D'  <br> 
# df1.normal >= 1.3 ise 'E'   
# </font>

# In[39]:


#parantezler önemli yoksa error mesajı alınır
df1=df1.withColumn('C1', when(df1.normal<=0.1, 'A')                    .when((df1.normal>0.1) & (df1.normal<=0.7), 'B')                    .when((df1.normal>0.7) & (df1.normal<=1), 'C')                    .when((df1.normal>1) & (df1.normal<1.3), 'D')                    .otherwise('E')) 


# # DISTINCT
# <br> 
# <br> 
# <font size=4,font style=arial>
# Oluşturdumuz C1 kolonuna distinct ile bir göz atalım   
# </font>

# In[40]:


df1.select('C1').distinct().show()


# <font size=4,font style=arial>
# df1 veri setimize bir göz atalım
# </font>    

# In[41]:


df1.show(10)


# <font size=4,font style=arial>
# Benzer bir çalışmayı df2 içinde yapalım.Kolon adı C2 olsun. 
# <br>
# df2.normal2 <= 0.3 ise 'W'
# <br>
# df2.normal2 > 0.3 ise 'Z' 
# </font>

# In[42]:


df2=df2.withColumn('C2', when(df2.normal2<=0.3, 'W')                     .otherwise('Z')) 


# <font size=4,font style=arial>
# df2'e göz atalım
# </font>

# In[43]:


df2.show(10)


# # JOIN
# <br>
# <br>
# <font size=4,font style=arial>
# df1 ile df2 join yapalım ekstradan gelen id kolonunu drop edelim
# </font>

# In[44]:


df = df1.join(df2, df1.id == df2.id).drop(df2.id)


# <font size=4,font style=arial>
# df kontrol edelim
# </font>

# In[45]:


df.printSchema()


# <br>
# <br>
# <font size=4,font style=arial>
# df'in top 10 kontrolü
# </font>

# In[46]:


df.show(10)


# # YENI KOLON EKLEME
# <br>
# <br>
# <font size=4,font style=arial>
# df'de bulunan uniform1 ve normal kolonlarını kullanarak yeni bir kolon oluşuralım şu an bu kolunu dataframe'e yazmayacağız
# </font>

# In[47]:


df.withColumn('yeni_kolon',3*df.uniform1-3*df.normal ).show(5)


# # KOLON ADLARINI DEĞİŞTİRME
# <br>
# <br>
# <font size=4,font style=arial>
# Yeni kolon adlarımız U1,U2,U3,N1,N2,N3,N4'e çevirelim.  
# </font>

# In[48]:


df = df.withColumnRenamed("uniform", "U1")       .withColumnRenamed("uniform1", "U2")       .withColumnRenamed("uniform2", "U3")       .withColumnRenamed("normal", "N1")       .withColumnRenamed("normal1", "N2")       .withColumnRenamed("normal2", "N3")       .withColumnRenamed("normal3", "N4")
df.printSchema()


# # KOLONUN MİN,MAX,AVG,SUM,COUNT,STANDARD DEVIATION'NI BULMA
# <br>
# <font size=4,font style=arial>
# N1 kolonu için bulalım.Method 1 aggregate fonksiyonunu, Method 2 select'i kullanıyor. İsterseniz de withColumnRenamed fonksiyonu kullanarak kolona yeni ad verilebilir. func.round ilede round yapabilirsiniz.
# </font>

# In[49]:


#1.method
df.agg(min("N1"),max("N1"),mean("N1"),func.round(sum("N1"),2),stddev("N1"),count("N1")).withColumnRenamed("min(N1)", "min").show()


# In[50]:


#2.method
df.select([min('N1'), max('N1'),mean('N1'),sum('N1'),stddev('N1'),count('N1')]).show()


# # GROUP BY 
# <br>
# <font size=4,font style=arial>
# Yukarıda buldugumuz değerleri C1 ve C2 için group by şeklinde bulalım
# </font>

# In[51]:


df.groupby('C1','C2').agg(min("N1"),max("N1"),mean("N1"),func.round(sum("N1"),2),stddev("N1"),count("N1")).withColumnRenamed("min(N1)", "min").show() 


# # FILTER
# <br>
# <br>
# <font size=4,font style=arial>
# FILTER fonksiyonu aşağıda ki şekilde hesaplanır. 
# (C1=A) ve (U1 0.9'a eşit olmasın) ve (Y 0'a eşit olsun) ve (N1 0'dan büyük olsun) 
# </font>

# In[52]:


df.filter('C1="A" and U1 != 0.9 and Y=0 and N1>0').show(10)


# <font size=4,font style=arial>
# FİLTER 'dan sonra min,max,mean,sum,stddev'ını bulalım
# </font>

# In[53]:


df.filter('C1="A" and U1 != 0.9 and Y=0 and N1>0')         .agg(min("N1"),max("N1"),mean("N1"),func.round(sum("N1"),2),stddev("N1"),count("N1")).show()


# <font size=4,font style=arial>
# Çapraz tablolama
# </font>

# In[54]:


df.crosstab('C1', 'C2').show()


# <font size=4,font style=arial>
# distinct değerleri görme
# </font>

# In[55]:


df.select('C1','C2').dropDuplicates().show()


# # ORDER BY-SORT DESCENDING
# <br>
# <br>
# <font size=4,font style=arial>
# C1 ve C2 değişkenine göre count alıp büyükten küçüğe doğru sıralayım
# </font>

# In[56]:


df.groupBy('C1','C2').count().orderBy('count',ascending=False).show()


# # RANK
# <br>
# <br>
# <font size=4,font style=arial>
# C1 ve C2 değişkenine göre N1 büyükten küçüğe sıralanacak şekilde rank numarası ata. Herbir grupdan top 2'yi seç
# </font>

# In[57]:


df.withColumn("row_number", func.row_number().over(Window.partitionBy("C1","C2").orderBy(desc("N1"))))  .filter(col('row_number') <= 2).show()      


# # MACHINE LEARNING
# <br>
# <br>
# <font size=4,font style=arial>
# Lojistik regresyon uygulamadan önce gerekli paketleri yükleyelim. 
# </font>

# In[58]:


import numpy
from pyspark.ml.feature import RFormula
from pyspark.ml.classification import BinaryLogisticRegressionSummary,LogisticRegression
from pyspark.ml.evaluation import (BinaryClassificationEvaluator,MulticlassClassificationEvaluator) 


# <br>
# <font size=4,font style=arial>
# Bağımlı değişkenimiz Y ve bağımsız değişkenlerimiz U1,U2,U3,N1,N2,N3,N4,C1,C2 olmak üzere Lojistik regresyon analizi yapalım. Spark model için bir features (bağımsız değişkenlerin oluşturduğu sparse matris) ve label(bağımlı değişken) vektörlerinin oluşturulması gerekiyor. R formula bunu oluşturmaktadır.
# </font>

# In[59]:


formula = RFormula(formula="Y ~ U1+U2+U3+N1+N2+N3+N4+C1+C2")
output = formula.fit(df).transform(df)


# <font size=4,font style=arial>
# Modelimiz için gerekli olan features(yer kaplamaması ve işlem kolaylığı açısından oluşturulan sparse matris)  ve label kolonu oluştu. Aşağıda da çıktısı var.
# </font>    

# In[60]:


output.show(5,truncate=False) 


# <font size=4,font style=arial>
# Model için sadece features ve label kolonunu alacağız.
# </font>

# In[61]:


df_log=output.select([c for c in output.columns if c not in {'id','U1','U2','N1','N2','Y','C1','U3','N3','N4','C2'}])


# In[62]:


df_log


# In[63]:


df_log.show(5)


# <font size=4,font style=arial>
# Veri setimizi %70 ve %30 şeklinde ikiye bölüp %70'de modeli kurup %30'da test edelim.
# </font>

# In[64]:


lr_train,lr_test=df_log.randomSplit([0.7,0.3])


# <font size=4,font style=arial>
# Modelimizi uygulayalım
# </font>

# In[65]:


final_model=LogisticRegression()
fit_final_model=final_model.fit(lr_train)


# <font size=4,font style=arial>
# train setine uyguladığımız modeli test edelim
# </font>

# In[66]:


predictions_and_labels=fit_final_model.evaluate(lr_test)


# <font size=4,font style=arial>
# label gerçekleşen ve prediction da tahmin olmak üzere aşağıda ki şekildedir
# </font>

# In[67]:


predictions_and_labels.predictions.show(100,truncate=False)


# <font size=4,font style=arial>
# Roc eğrisinin altında ki alanı rakam olarak görelim. 1'e yakın bir değer iyi bir değerdir. Veri seti manual oluşturulduğundan aşağıda ki şekilde bir değer çıkmıştır.
# </font>

# In[68]:


my_eval=BinaryClassificationEvaluator()
my_final_roc=my_eval.evaluate(predictions_and_labels.predictions)
my_final_roc

