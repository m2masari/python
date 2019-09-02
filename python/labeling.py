#!/usr/bin/env python
# coding: utf-8

# Labeling categorizing in Pandas datafareme in python with an example

# In[18]:


import pandas as pd


# In[19]:


df=pd.DataFrame({'A':['x1','x2','x3'],'B':['female','male','female'],'C':['male','male','female']})


# In[20]:


df


# In[21]:


mapping={'male':0,'female':1}


# In[22]:


cols=['B','C']


# In[23]:


df[cols]=df[cols].applymap(mapping.get)


# In[24]:


df

