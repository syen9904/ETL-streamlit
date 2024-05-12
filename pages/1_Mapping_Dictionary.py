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
        conn = sqlite3.connect(st.session_state.db_path)
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
            logging.info("Database connection is closed")

def getDf(user_query, columns, table_name):
    df = pd.DataFrame(search(user_query, columns, table_name), columns=columns).set_index("source_code")
    df['target_concept_id'] = st.session_state.athena_prefix + df['target_concept_id'].apply(str)
    return df

st.set_page_config(
    layout="wide",
)

if 'columns' not in st.session_state: 
    st.session_state.parent_dir = os.path.dirname(os.getcwd())
    st.session_state.csv_path = st.session_state.parent_dir + "/cteVocabMapDF.csv"
    st.session_state.db_path = st.session_state.parent_dir + "/database.db"
    st.session_state.table_name = 'data'
    st.session_state.COLUMNS = ['source_code', 'target_concept_id', 'source_code_description', 'target_concept_name', 'source_vocabulary_id', 'target_vocabulary_id', 'target_domain_id', 'target_concept_class_id']
    st.session_state.columns = create_db(csv_path=st.session_state.csv_path, db_path=st.session_state.db_path, table_name=st.session_state.table_name)
    st.session_state.map = {"source_code": "Source Code", "target_concept_id": "Target Concept ID", "source_code_description": "Source Code Description", "target_concept_name": "Target Concept Name", "source_vocabulary_id": "Source Vocabulary ID", "target_vocabulary_id": "Target Vocabulary ID", "target_domain_id": "Target Domain ID", "target_concept_class_id": "Target Concept Class ID"}
    st.session_state.default_str = "levophed"
    st.session_state.user_query = st.session_state.default_str
    st.session_state.athena_prefix = "https://athena.ohdsi.org/search-terms/terms/"
    if os.path.exists(st.session_state.csv_path): st.session_state.columns = st.session_state.COLUMNS
    st.session_state.column_config = {
        "source_code": st.column_config.Column("Source Code", disabled=True), 
        "target_concept_id": st.column_config.LinkColumn("Target Concept ID", width="small", disabled=True, display_text=st.session_state.athena_prefix + "([^&]*)"), 
        "source_code_description": st.column_config.Column("Source Code Description", width="large", disabled=True), 
        "target_concept_name": st.column_config.Column("Target Concept Name", width="medium", disabled=True), 
        "source_vocabulary_id": st.column_config.Column("Source Vocabulary ID", width="small", disabled=True), 
        "target_vocabulary_id": st.column_config.Column("Target Vocabulary ID", width="small", disabled=True), 
        "target_domain_id": st.column_config.Column("Target Domain ID", width="small", disabled=True), 
        "target_concept_class_id": st.column_config.Column("Target Concept Class ID", width="small", disabled=True)        
        }
    logging.info(f"columns = {st.session_state.columns}")

st.title('JHU ETL Mapping Dictionary')

user_query = st.text_input(f"Enter your search string, it could be a description a concept code (ex: {st.session_state.default_str})", value=st.session_state.default_str)

if 'unfiltered_df' not in st.session_state: 
    st.session_state.unfiltered_df = getDf(st.session_state.default_str, st.session_state.columns, st.session_state.table_name)

if st.button('Search'):
    st.session_state.user_query = user_query
    st.session_state.unfiltered_df = getDf(st.session_state.user_query, st.session_state.columns, st.session_state.table_name)

if 'filtered_df' not in st.session_state: st.session_state.filtered_df = pd.DataFrame()
st.session_state.filtered_df = st.session_state.unfiltered_df

st.sidebar.markdown("### Filters")
filter_expander = {}
filter_dict = {}
columns_to_be_filtered = ['source_vocabulary_id', 'target_vocabulary_id', 'target_domain_id', 'target_concept_class_id']

for c in columns_to_be_filtered:
    no_filter = True
    if c not in st.session_state.unfiltered_df: continue
    expander = st.sidebar.expander(st.session_state.map[c])
    filter_dict[c] = {}
    for t in st.session_state.unfiltered_df[c].unique():
        filter_dict[c][t] = expander.checkbox(f"{t} ({(st.session_state.unfiltered_df[c]==t).sum()})", False, c+','+t)
        if filter_dict[c][t]: 
            no_filter = False
    if no_filter == False:
        for t in st.session_state.unfiltered_df[c].unique():
            if not filter_dict[c][t]:
                st.session_state.filtered_df = st.session_state.filtered_df[st.session_state.filtered_df[c] != t]        

if user_query != '':
    st.write(f'{len(st.session_state.unfiltered_df)} results found for "{st.session_state.user_query}".')
    st.write("If your source code is not found in the results, it is highly possible that the data element with that source code is not being mapped to OMOP. Please contact JH OMOP team.")

if len(st.session_state.filtered_df) > 0:
    st.data_editor(st.session_state.filtered_df, use_container_width=True, column_config=st.session_state.column_config)