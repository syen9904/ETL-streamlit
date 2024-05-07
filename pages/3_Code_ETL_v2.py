import streamlit as st
from collections import OrderedDict
import os

directory = os.getcwd() + '/PMAP_Derived_DML/'
if 'files' not in st.session_state: 
    st.session_state.files = {}
    for filename in list(os.walk(directory))[0][2]:
        if filename[::-1].find('.scala'[::-1]) == 0:
            st.session_state.files[filename] = 'scala'
        elif filename[::-1].find('.sql'[::-1]) == 0:
            st.session_state.files[filename] = 'sql'

for file in st.session_state.files:     
    file_link = st.sidebar.button(file)
    if file_link:
        st.markdown("## " + file)
        container = st.container(border=True)
        with open(directory + file, 'r') as code:
            text = ""
            for line in code:
                text = text + line
            container.code(text, language=st.session_state.files[file])


