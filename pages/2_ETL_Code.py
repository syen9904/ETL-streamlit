import streamlit as st
import os
import sys
sys.path.append(os.getcwd() + '/src')
from functions import getCodes

def demo(file):    
    st.markdown("## " + file)
    container = st.container(border=True)
    with open(st.session_state.codes[file]['path'], 'r') as code:
        text = ""
        for line in code:
            text = text + line
        container.code(text, language=st.session_state.codes[file]['language'])

if 'codes' not in st.session_state: 
    st.session_state.codes = getCodes(os.getcwd())

st.title('ETL Code')
container = st.container(border=True)

expander = {
    'raw2pmap': st.sidebar.expander('Preparation (derived)'), 
    'pmap2omop': st.sidebar.expander('ETL Process')
}

for file in sorted(list(st.session_state.codes.keys())):
    file_link = expander[st.session_state.codes[file]['category']].button(file)
    if file_link: demo(file)