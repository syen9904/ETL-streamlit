import streamlit as st

st.set_page_config(
    page_title="The JHM Epic to OMOP ETL Guideline"
)

"""
# Introduction

Welcome to the Epic to OMOP ETL implementation guide.  

## How to use this guide

This guide is intended for clinical customers of Epic to be able to convert their clarity patient record data into the OMOP common data format.  We have provided scripts that should aid sites in reducing the time and cost to convert their data into this powerful common data model.  The chapters are arranged around the main steps you will need to undertake to build an ETL pipeline to build and maintain an OMOP CDM.

Those steps are.

- Setting up the database environment
-- Installing the base OMOP CDM schema
-- Creating the run_log tracking table
- Uploading the OMOP vocabulary tables
- Reviewing the scripts and identify concept maps that need to be localized to your institution
- Scripts to prep the database
- Scripts to fill the main tables
- Scripts to fill the measurements table
"""
