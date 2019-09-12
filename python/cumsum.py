#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd


# In[3]:



data = [['Jon','car',10000]
        ,['Jon','car',150]
        ,['Jon','house',150000]
        ,['Alex','house',12000]
        ,['Alex','house',50000]
        ,['Steve','house',180000]
        ,['Jon','car',20000]
        ,['Jon','piano',2000]
       ]
  
# Create the pandas DataFrame 
df = pd.DataFrame(data, columns = ['Name','Item','price']) 
df.head(10) 


# In[10]:


df['count_by_person']=df.groupby('Name').cumcount()+1
df['count_by_item']=df.groupby('Item').cumcount()+1
df['count_by_both']=df.groupby(['Name','Item']).cumcount()+1


# In[11]:


df


# In[ ]:




