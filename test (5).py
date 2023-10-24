#!/usr/bin/env python
# coding: utf-8

# In[1]:


get_ipython().run_line_magic('reload_ext', 'sql')


# In[2]:


get_ipython().run_line_magic('sql', 'postgresql://student:student@127.0.0.1/sparkifydb')


# In[3]:


get_ipython().run_line_magic('sql', 'SELECT * FROM songplays LIMIT 5;')


# In[4]:


get_ipython().run_line_magic('sql', 'SELECT count(*) FROM songplays')


# In[5]:


get_ipython().run_line_magic('sql', 'SELECT * FROM users LIMIT 5;')


# In[6]:


get_ipython().run_line_magic('sql', 'SELECT count(*) FROM users')


# In[7]:


get_ipython().run_line_magic('sql', 'SELECT * FROM songs LIMIT 5;')


# In[8]:


get_ipython().run_line_magic('sql', 'SELECT count(*) FROM songs')


# In[9]:


get_ipython().run_line_magic('sql', 'SELECT * FROM artists LIMIT 5;')


# In[10]:


get_ipython().run_line_magic('sql', 'SELECT count(*) FROM artists')


# In[11]:


get_ipython().run_line_magic('sql', 'SELECT * FROM time LIMIT 5;')


# In[12]:


get_ipython().run_line_magic('sql', 'SELECT count(*) FROM time')


# ## REMEMBER: Restart this notebook to close connection to `sparkifydb`
# Each time you run the cells above, remember to restart this notebook to close the connection to your database. Otherwise, you won't be able to run your code in `create_tables.py`, `etl.py`, or `etl.ipynb` files since you can't make multiple connections to the same database (in this case, sparkifydb).

# # Sanity Tests 
# 
# Execute the cells below once you are ready to submit the project. Some basic sanity testing will be performed to esnure that your work does NOT contain any commonly found issues. 
# 
# Run each cell and if a cell produces an warning message is orange, you should make appropriate changes to your code before submitting. If all test in a cell pass,no warnings will be printed. 
# 
# The test cases assume that you are using certain column names in your tables. If you get a `IndexError: single positional indexer is out-of-bounds` you may need to change the column names being used by the test cases. Instructions for doing this appear right boefore cell that may require these changes.
# 
# The tests below are only meant to help you make your work foolproof. The submission will still be graded by a human grader against the project rubric.
# 
# ---

# ## Grab Table Names for Testing

# In[13]:


import sql_queries as sqlq


# In[14]:


get_ipython().run_cell_magic('sql', '_tablenames <<', "SELECT tablename\nFROM pg_catalog.pg_tables\nWHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema' AND tableowner = 'student';")


# In[15]:


tablenames = _tablenames.DataFrame()


# In[16]:


user_table = [name for name in list(tablenames.tablename) if name in sqlq.user_table_create]
if user_table:
    user_table = user_table[0]
else:
    #Handle the case when the list is empty
    pass


# In[17]:


song_table = [name for name in list(tablenames.tablename) if name in sqlq.song_table_create]
if song_table:
    song_table = song_table[0]
else:
    #Handle the case when the list is empty
    pass


# In[18]:


artists_table = [name for name in list(tablenames.tablename) if name in sqlq.artist_table_create]
if artists_table:
    artists_table = artists_table[0]
else:
    #Handle the case when the list is empty
    pass


# In[19]:


songplay_table = [name for name in list(tablenames.tablename) if name in sqlq.songplay_table_create]
if songplay_table:
    songplay_table = songplay_table[0]
else:
    #Handle the case when the list is empty
    pass


# In[ ]:





# ## Run Primary Key Tests

# In[20]:


# reasign table name

user_table = "users"
song_table = "songs"
artists_table = "artists"
songplay_table = "songplays"


# In[21]:



get_ipython().run_line_magic('sql', "_output << SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS data_type, a.attnotnull, i.indisprimary FROM   pg_index i JOIN   pg_attribute a ON a.attrelid = i.indrelid                      AND a.attnum = ANY(i.indkey) WHERE  i.indrelid = '{user_table}'::regclass")


# In[22]:


if not _output:
    print('\033[93m'+'[WARNING] '+ f"The {user_table} table does not have a primary key!")


# In[23]:


get_ipython().run_line_magic('sql', "_output << SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS data_type, a.attnotnull, i.indisprimary FROM   pg_index i JOIN   pg_attribute a ON a.attrelid = i.indrelid                      AND a.attnum = ANY(i.indkey) WHERE  i.indrelid = '{artists_table}'::regclass")


# In[24]:


if not _output: 
    print('\033[93m'+'[WARNING] '+ f"The {artists_table} table does not have a primary key!")


# In[25]:


get_ipython().run_line_magic('sql', "_output << SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS data_type, a.attnotnull, i.indisprimary FROM   pg_index i JOIN   pg_attribute a ON a.attrelid = i.indrelid                      AND a.attnum = ANY(i.indkey) WHERE  i.indrelid = '{songplay_table}'::regclass")


# In[26]:


if not _output:
    print('\033[93m'+'[WARNING] '+ f"The {songplay_table} table does not have a primary key!")


# In[27]:


get_ipython().run_line_magic('sql', "_output << SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS data_type, a.attnotnull, i.indisprimary FROM   pg_index i JOIN   pg_attribute a ON a.attrelid = i.indrelid                      AND a.attnum = ANY(i.indkey) WHERE  i.indrelid = '{song_table}'::regclass")


# In[ ]:


if not _output:
    print('\033[93m'+'[WARNING] '+ f"The {song_table} table does not have a primary key!")


# ## Run Data Type and Constraints Check

# In[ ]:


# reasign table name

user_table = "users"
song_table = "songs"
artists_table = "artists"
songplay_table = "songplays"


# In[ ]:


get_ipython().run_line_magic('sql', "_output << SELECT * FROM information_schema.columns where table_name='users'")


# **Check the column `user_id` for correct data type.**
# If you get a `IndexError: single positional indexer is out-of-bounds` error, you may be using a different column name. Change the column name below and run the cell again. 

# In[ ]:



output = _output.DataFrame()
if 'userid' in output.column_name.values:
    _dtype = output[output.column_name == 'userid'].data_type.iloc[0]
    if _dtype != 'string':
        print('\033[93m'+'[WARNING] '+ f"Type {_dtype} may not be an appropriate data type for column 'userid' in the '{user_table}' table.")
else:
    print('\033[93m'+'[WARNING] '+ "No column named 'userid' found in the output DataFrame.")
    


# In[ ]:


get_ipython().run_line_magic('sql', "_output << SELECT * FROM information_schema.columns where table_name='{song_table}'")


# **Check the column `year` for correct data type.
# Check columns `title` and `duration` for not-NULL constraints.**
# 
# If you get a `IndexError: single positional indexer is out-of-bounds` error, you may be using different column names. Change the column name(s) below and run the cell again. 

# In[ ]:


output = _output.DataFrame()

_dtype = output[output.column_name == 'year'].data_type.iloc[0]
if _dtype not in ['integer']:
    print('\033[93m'+'[WARNING] '+ f"Type '{_dtype}' may not be an appropriate data type for column 'year' in the '{song_table}' table.")

_nullable_title = output[output.column_name == 'title'].is_nullable.iloc[0]
_nullable_duration = output[output.column_name == 'duration'].is_nullable.iloc[0]
if (_nullable_duration != 'NO') or (_nullable_title != 'NO'): 
    print('\033[93m'+'[WARNING] '+ f"You may want to add appropriate NOT NULL constraints to the '{song_table}' table.")


# In[ ]:


get_ipython().run_line_magic('sql', "_output << SELECT * FROM information_schema.columns where table_name='{artists_table}'")


# **Check the columns `latitude` and `longitude` for correct data type.
# Check column `name` for not-NULL constraint.**
# 
# If you get a `IndexError: single positional indexer is out-of-bounds` error, you may be using different column names. Change the column name(s) below and run the cell again. 

# In[ ]:


output = _output.DataFrame()
if 'artist_latitude' in output.column_name.values:
    _dtype_latitude = output.loc[output.column_name == 'artist_latitude', 'data_type'].iloc[0]
    if _dtype_latitude not in ['double precision']:
        print('\033[93m'+'[WARNING] '+ f"Type '{_dtype_latitude}' may not be an appropriate data type for column 'latitude' in the '{artists_table}' table")
else:
    print('\033[93m'+'[WARNING] '+ "No column named 'artist_latitude' found in the DataFrame")
print(output)


# In[ ]:


get_ipython().run_line_magic('sql', "_output << SELECT * FROM information_schema.columns where table_name='songplays'")


# **Check the columns `start_time` and `user_id` for correct data type.
# Check columns `start_time` and `user_id` for not-NULL constraint.**
# 
# If you get a `IndexError: single positional indexer is out-of-bounds` error, you may be using different column names. Change the column name(s) below and run the cell again. 

# In[ ]:


output = _output.DataFrame()
user_id_rows = output[output.column_name == 'userid']
if not user_id_rows.empty:
    _dtype_user_id = user_id_rows.data_type.iloc[0]
    if _dtype_user_id not in ['integer', 'bigint']:
        print('\033[93m'+'[WARNING] '+ f"Type '{_dtype_user_id}' may not be an appropriate data type for column 'userid' in the '{songplay_table}' table.")
else:
    print('\033[93m'+'[WARNING] '+ "No rows found for column 'userid'.")
print(output)


# ## Run Tests for Upsertion Check

# In[ ]:


import re


# In[ ]:


if not re.search('ON\s+CONFLICT',sqlq.songplay_table_insert,re.IGNORECASE) or    not re.search('ON\s+CONFLICT',sqlq.user_table_insert,re.IGNORECASE) or    not re.search('ON\s+CONFLICT',sqlq.song_table_insert,re.IGNORECASE) or    not re.search('ON\s+CONFLICT',sqlq.artist_table_insert,re.IGNORECASE):
    print('\033[93m'+'[WARNING]Some of your insert queries may need an "ON CONFLICT" clause.')
    print('\033[93m'+'         You can either skip conflicting insertions with with "ON CONFLICT DO NOTHING"')
    print('\033[93m'+'         OR use "ON CONFLICT DO UPDATE SET"')
    print('\033[93m'+'         Check this link for more details: https://www.postgresqltutorial.com/postgresql-upsert/')


# In[ ]:




