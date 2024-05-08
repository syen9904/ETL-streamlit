import streamlit as st
import os
import pandas as pd
import sqlite3
from typing import List
import sqlite3
import logging
from sqlite3 import Error

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s') 

def get_db_columns(db_path, table_name):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name});")
    columns = [row[1] for row in cursor.fetchall()] 
    conn.close()
    return columns
    
def create_fake_df():
    data = {
        'Name': ['John Doe', 'Jane Doe', 'Jim Brown', 'Jake Blues'],
        'Age': [28, 34, 23, 45],
        'City': ['New York', 'Los Angeles', 'Chicago', 'New Orleans'],
        'Occupation': ['Developer', 'Scientist', 'Manager', 'Musician'],
        'Name1': ['John Doe', 'Jane Doe', 'Jim Brown', 'Jake Blues'],
        'Age1': [28, 34, 23, 45],
        'City1': ['New York', 'Los Angeles', 'Chicago', 'New Orleans'],
        'Occupation1': ['Developer', 'Scientist', 'Manager', 'Musician'],
        'Name2': ['John Doe', 'Jane Doe', 'Jim Brown', 'Jake Blues'],
        'Age2': [28, 34, 23, 45],
        'City2': ['New York', 'Los Angeles', 'Chicago', 'New Orleans'],
        'Occupation2': ['Developer', 'Scientist', 'Manager', 'Musician']
    }
    df = pd.DataFrame(data)
    for i in range(3):
        df = pd.concat([df, df])
    return df

def create_db(csv_path, db_path, table_name) -> List:
    if os.path.exists(db_path): 
        logging.info(f'db already exist at {db_path}')
        return get_db_columns(db_path=db_path, table_name=table_name)
    if os.path.exists(csv_path):
        cteVocabMapDF = pd.read_csv(csv_path, on_bad_lines="skip", delimiter="|")
        logging.info('csv was read successfully. Size:', cteVocabMapDF.shape)
    else:
        cteVocabMapDF = create_fake_df()
        logging.info('test df was created successfully. Size:', cteVocabMapDF.shape)
    conn = sqlite3.connect(db_path)
    cteVocabMapDF.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()
    logging.info(f'db created at {db_path}')
    return cteVocabMapDF.columns.tolist()

def str_to_query(search_str, columns, table_name) -> str:
    select_clause = ", ".join([f'"{col}"' for col in columns])
    like_clauses = [f'"{col}" LIKE "%{search_str}%"' for col in columns]
    query_condition = " OR ".join(like_clauses)
    query = f"SELECT {select_clause} FROM {table_name} WHERE {query_condition}"
    return query 

def search(search_str, columns, table_name):
    logging.info(f'searching: {search_str}')
    if not search_str: return []
    query = str_to_query(search_str, columns, table_name)
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        logging.info("Connected to database successfully")
        cursor = conn.cursor()
        cursor.execute(query)
        logging.info("Query executed successfully")
        results = cursor.fetchall()
        logging.info("Query executed successfully")
        return results
    
    except Error as e:
        logging.info(f"Error occurred: {e}")
        return None
    
    finally:
        if conn:
            conn.close()
            print("Database connection is closed")


parent_dir = os.path.dirname(os.getcwd())
csv_path = parent_dir + "/cteVocabMapDF.csv"
db_path = parent_dir + "/database.db"
table_name = 'data'
COLUMNS = ['source_code', 'source_code_description', 'source_vocabulary_id', 'source_domain_id', 'source_concept_class_id', 'target_concept_id', 'target_concept_name', 'target_vocabulary_id', 'target_domain_id', 'target_concept_class_id']

if 'columns' not in st.session_state: 
    st.session_state.columns = create_db(csv_path=csv_path, db_path=db_path, table_name=table_name)
if os.path.exists(csv_path): st.session_state.columns = COLUMNS
print('columns = ', st.session_state.columns)

st.title('JHU ETL Mapping Dictionary')
user_query = st.text_input("Enter your search string, it could be a description a concept code (ex: levophed)", "")

def getDf(user_query, columns, table_name):
    return pd.DataFrame(search(user_query, columns, table_name), columns=columns)

if 'unfiltered_df' not in st.session_state: st.session_state.unfiltered_df = pd.DataFrame()

if st.button('Search'):
    st.session_state.unfiltered_df = getDf(user_query, st.session_state.columns, table_name)

if 'filtered_df' not in st.session_state: st.session_state.filtered_df = pd.DataFrame()
st.session_state.filtered_df = st.session_state.unfiltered_df

st.sidebar.markdown("### Filters")
filter_expander = {}
filter_dict = {}
columns_to_be_filtered = ['source_vocabulary_id', 'source_domain_id', 'source_concept_class_id', 'target_vocabulary_id', 'target_domain_id', 'target_concept_class_id']

for c in columns_to_be_filtered:
    no_filter = True
    if c not in st.session_state.unfiltered_df: continue
    expander = st.sidebar.expander(c)
    filter_dict[c] = {}
    for t in st.session_state.unfiltered_df[c].unique():
        filter_dict[c][t] = expander.checkbox(f"{t} ({(st.session_state.unfiltered_df[c]==t).sum()})", False, c+','+t)
        if filter_dict[c][t]: 
            no_filter = False
    if no_filter == False:
        for t in st.session_state.unfiltered_df[c].unique():
            if not filter_dict[c][t]:
                st.session_state.filtered_df = st.session_state.filtered_df[st.session_state.filtered_df[c] != t]        

st.write(f"{len(st.session_state.unfiltered_df)} results found for '{user_query}'")

if len(st.session_state.filtered_df) > 0:
    st.write(st.session_state.filtered_df)