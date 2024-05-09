import os
import sys
import streamlit as st
import networkx as nx
from pyvis.network import Network
import streamlit.components.v1 as components

sys.path.append(os.getcwd() + '/src')
from functions import getCodes

def find_node(line, keys, false_keys, comment_keys):
    for start_key in keys:
        if line.find(start_key) >= 0:
            start = line.find(start_key) + len(start_key)
            end = len(line) + 1
            for stop_code in keys[start_key]:
                if line[start:].find(stop_code) >= 0:
                    if (start > len(start_key) and ' ' in keys[start_key]) and line[start-len(start_key)-1] != ' ':
                        continue
                    end = min(start + line[start:].find(stop_code), end)
            if end > len(line):
                continue

            node = line[start:end]
            key = True

            for comment_key in comment_keys:
                if line.find(comment_key) >= 0 and line.find(comment_key) < line.find(start_key): 
                    key = False
                    break
            if not key: continue

            for false_key in false_keys:
                if node.find(false_key) == 0: 
                    key = False
                    break
            if not key: continue
            
            if node != '':
                if node[0] == '"' and node[-1] == '"':
                    node = node[1:-1]
                if node.find("$sourcetablesdb") >= 0:
                    node = 'derived' + node[node.find('.'):-1]
                return node
    return None

def parse(codes, file):
    result = {'input': {}, 'output': {}, 'cte': {}}
    input_keys = {}
    input_keys['sql'] = {'from ': [' ', '"""', '\t', ')'], 'join ': [' ', '\t', ')'], 'using ': [' ', '\t', ')'], 'table_changes("': ['"']}
    input_keys['scala'] = {'read.table(': [')']}
    output_keys = {}
    output_keys['sql'] = {'merge ': [' ', '\t'], 'insert ': [' ', '\t'],'overwrite ': [' ', '\t'],'table ': [' ', '\t']}
    output_keys['scala'] = {'saveastable(': [')']}
    cte_keys = {'with ' : [' as'], ',' : [' as'], ', ': [' as']}
    comment_keys = {'//', '--'}
    false_keys = ['select', 'into', 'table', 'overwrite', '*', 'if', '(', 'table_changes']

    with open(codes[file]['path'], 'r') as f:
        text = ""
        filetype = codes[file]['language']
        for line in f:
            text = text + line
            line = line.lower()[:-1] + ' '
            input_key = find_node(line, input_keys[filetype], false_keys, comment_keys)
            if input_key is not None:
                result['input'][input_key] = (file, line)
            output_key = find_node(line, output_keys[filetype], false_keys, comment_keys)
            if output_key is not None:
                result['output'][output_key] = (file, line)
        
        cte = {}
        for input_key in result['input']:
            if text.find(f"{input_key} as") >= 0:       #CTE
                cte[input_key] = True
        for input_key in cte:
            result['input'].pop(input_key)

    return result

def getEdges(omop_tables):
    codes = getCodes(os.getcwd())
    nodes = {'input': {}, 'output': {}}
    edges = []
    check = ['']
    for file in codes: 
        result = parse(codes, file)
        for type in result:
            for node in list(result[type].keys()):
                if node in check: print(f"[{node}]\n{result[type][node]}\n")
                nodes[type][node] = (nodes[type][node] + 1) if node in nodes[type] else 1
        for a in list(result['input'].keys()):
            for b in list(result['output'].keys()):
                if a == b: continue
                if a in omop_tables or b in omop_tables:
                    edges.append((a, b))

    return edges

omop_tables = ['person', 'observation_period', 'visit_occurrence', 'visit_detail', 'condition_occurrence', 'drug_exposure', 'procedure_occurrence', 'device_exposure', 'measurement', 'observation', 'death', 'note', 'note_nlp', 'specimen', 'fact_relationship', 'location', 'care_site', 'provider', 'payer_plan_period', 'cost', 'drug_era', 'dose_era', 'condition_era', 'episode', 'episode_event', 'metadata', 'cdm_source', 'concept', 'vocabulary', 'domain', 'concept_class', 'concept_relationship', 'relationship', 'concept_synonym', 'concept_ancestor', 'source_to_concept_map', 'drug_strength']

st.title('Knowledge Graph')

if 'G' not in st.session_state:
    st.session_state.G = nx.DiGraph()
    st.session_state.edges = getEdges(omop_tables)
    for a, b in st.session_state.edges:
        st.session_state.G.add_edge(a, b)
    st.session_state.net = Network(height="750px", width="100%", bgcolor="#222222", font_color="white")
    st.session_state.net.from_nx(st.session_state.G)
    st.session_state.net_html = st.session_state.net.generate_html()

for node in omop_tables:
    if node in st.session_state.G.nodes:
        st.session_state.G.nodes[node]['group'] = 'OMOP'

st.write(f"Number of edges from PMAP to OMOP", len(st.session_state.edges))
st.write("Is this graph a DAG: ", nx.is_directed_acyclic_graph(st.session_state.G))

# Display in Streamlit
components.html(st.session_state.net_html, height=800)