
# coding: utf-8

# # Pyspark Word Count
# <br>
# <font size=4,font style=arial>
# Bu yazıda ki amacımız;<br> 
#     -Metnin RDD olarak okunması<br> 
#     -Noktalama işaretlerinin çıkarılması<br>
#     -Büyük harflerin küçük harf yapılması (Aksi takdirde Spark iki farklı kelime gibi algılıyor. Örnek Computer ve computer,iki farklı kelime group by yapıp toplandığında tek kelime altında toplanmıyor)<br>
#     -Kelimelere ayırıp, aynı kelimeleri group by ile toplayıp, büyükten küçüğe sort yapıp, en fazla geçen kelimenin bulunması<br>
# <br>
# <br>
#     Metnimizi RDD olarak okuyalım ve text adında kaydedelim. (RDD 'ler unstructure bir yapıya sahip olduklarından, word count'lar için idealdir.)
# </font> 

# In[2]:


text=sc.parallelize(["#$Apache\Hadoop* -and- APache -./:;<=>? Spark: are +well-known@examples:;<=> of Big $$$data%%% processing{|} systems.\
   Hadoop$%&$%&$%& and \\SpArk\\]^ are<=> designed'' for^^^^ distributed *+,-./:;<=>?@[\processing of large#$%&\'()*+,-./:;<\
   =>?@[\data$%& sets@@ across( {|}~clusters of +,-.ComputERS\'()*+,-./:;<.apache++ SpArk is%&  open +,-.source and one^^ of the Most\
   FAMOUS;<= Big DATA$% PROCESSING systems"])


# <font size=3,font style=arial>
# <br>
# SparkContext'imizi ve SparkSession'ımızı oluşturalım
# </font> 

# In[1]:


from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)


# <font size=3,font style=arial>
# <br>
# İhtiyacımız olan fonksiyonları import edelim
# </font> 

# In[3]:


from pyspark.sql.functions import lower, col
import re, string


# <font size=3,font style=arial>
# <br>
# Aşağıda ki fonksiyon yardımıyla; büyük harfleri küçültelim ve noktalama işaretlerini ve gereksiz karakterleri metinden çıkaralım<br>
# Not:Lower harfleri küçültüyor<br>
# Not:Replace noktalama işaretlerini çıkarıyor<br>
# </font> 

# In[5]:


def noktalama_kucuk_harf(x):
    punc='!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~'
    lowercased_str = x.lower()
    for ch in punc:
        lowercased_str = lowercased_str.replace(ch, ' ')
    return lowercased_str


# <font size=3,font style=arial>
# <br>
# Textimizin yeni hali aşağıdadır. Bütün noktalama işaretlerinden arındırılmış ve harfler küçülmüş durumda.
# </font> 

# In[6]:


text_rdd = text.map(noktalama_kucuk_harf)
text_rdd.collect()


# <font size=3,font style=arial>
# <br>
# Aşağıda Flatmap transformation'ı ile texti kelimelere böldük. Herbir transformations'dan sonra RDD'ler yeni bir RDD'ye atanır. Mevcut RDD değiştirilemez(Immutable).  
# </font> 

# In[25]:


text_word_flatmap=text_rdd.flatMap(lambda s:s.split (" "))


# <font size=3,font style=arial>
# <br>
# Take bir action'dır ve RDD'nin içeriğini görmemizi sağlar.(5 tanesini listeleyelim. Eğer ki istenirse text_word_flatmap.collect() kullanılarak RDD'nin tamamı listeleneblir fakat collect action'ı büyük veri için memory sıkıntısı 
#  yaratabilir.)
# </font> 

# In[26]:


text_word_flatmap.take(5)


# <font size=4,font style=arial>
# Textimizdeki boşluk sayısı çok (arındırdığımz noktalama işaretlerini boşluklar ile değiştirdik). Bu boşlukları filitreleyelim
#     </font> 

# In[9]:


text_word_flatmap1 = text_word_flatmap.filter(lambda x: len(x)>0 )


# <font size=4,font style=arial>
# <br>
# Boşluklar filitrelendikten sonra metinde geçen kelimeleri listeleyelim
# </font> 

# In[10]:


text_word_flatmap1.collect()


# <font size=4,font style=arial>
# <br>
# İhtiyacımız olmayan kelimeleri metinden çıkaralım. Çıkaracağımız kelimeler={'is','and','are','of','and','for','the'}
# </font> 

# In[13]:


cikacak_kelimeler={'is','and','are','of','and','for','the'}
text_word_flatmap2 = text_word_flatmap1.filter(lambda x: x not in cikacak_kelimeler)


# <font size=4,font style=arial>
# <br>
# Map transformation'ı ile kelimelerin yanına 1 getirelim(daha sonra bu kelimeleri toplayacağız)
# </font> 

# In[14]:


text_map=text_word_flatmap2.map(lambda s:(s,1))


# In[15]:


text_map.collect()


# <font size=4,font style=arial>
# <br>
# Şimdi kelimeleri group by yapıp toplayalım
# </font> 

# In[17]:


text_reduce=text_map.reduceByKey(lambda x,y:x+y)


# In[19]:


text_reduce.collect()


# <font size=4,font style=arial>
# <br>
# Büyükten küçüğe sort yapıp en fazla geçen 5 kelimeyi bulalım.
# </font> 

# In[20]:


text_reduce.sortBy(lambda a: a[1],False).take(5)


# <font size=3,font style=arial>
# <br>
# Görüldüğü üzere metinde en fazla apache,spark,data ve processing kelimeleri 3'er defa geçiyor. Metinin büyük veriden bahsettiği kesin:)
# </font> 

# <font size=3,font style=arial>
# <br>
# Eğer ki istenirse baş harfleri aynı olan kelimelerde group by yapılabilir. Baş harfi aynı olan kelimeler hangileridir bir göz atalım
# </font> 

# In[23]:


basharf = text_word_flatmap2.groupBy(lambda x: x[0])


# In[24]:


for t in basharf.collect():
    print((t[0],[i for i in t[1]]))

