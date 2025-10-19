(.venv) randy83442@penguin:~/providers-new$ python3
Python 3.11.2 (main, Apr 28 2025, 14:11:48) [GCC 12.2.0] on linux
Type "help", "copyright", "credits" or "license" for more information.
>>> # app_readonly.py
>>> # -*- coding: utf-8 -*-
>>> from __future__ import annotations
>>> 
>>> import os
>>> import pandas as pd
>>> import sqlalchemy as sa
>>> from sqlalchemy import text as T
>>> import streamlit as st
>>> 
>>> # ---- Streamlit page config MUST be first ----
>>> st.set_page_config(page_title="Providers — Read-Only", page_icon="📒", layout="wide")
2025-10-19 01:40:31.184 WARNING streamlit.runtime.scriptrunner_utils.script_run_context: Thread 'MainThread': missing ScriptRunContext! This warning can be ignored when running in bare mode.
>>> 
>>> DB_PATH = os.environ.get("PROVIDERS_DB", "providers.db")
>>> ENG = sa.create_engine(f"sqlite:///{DB_PATH}", pool_pre_ping=True)
>>> 
>>> DDL = """
... CREATE TABLE IF NOT EXISTS vendors (
...   id INTEGER PRIMARY KEY,
...   business_name TEXT NOT NULL,
...   category TEXT NOT NULL,
...   service TEXT NOT NULL,
...   contact_name TEXT,
...   phone TEXT,
...   email TEXT,
...   website TEXT,
...   address TEXT,
...   city TEXT,
...   state TEXT DEFAULT 'TX',
...   zip TEXT,
...   notes TEXT,
...   created_at TEXT,
...   updated_at TEXT,
...   computed_keywords TEXT,
...   ckw_locked INTEGER DEFAULT 0,
...   ckw_version TEXT
... );
... """
>>> 
>>> def ensure_schema():
...     with ENG.begin() as cx:
...         for stmt in [s.strip() for s in DDL.split(";") if s.strip()]:
...             cx.execute(T(stmt))
... 
>>> @st.cache_data(show_spinner=False)
... def load_df(q: str) -> pd.DataFrame:
...     ensure_schema()
...     with ENG.connect() as cx:
...         if q:
...             return pd.read_sql_query(
...                 T("""
...                   SELECT id,business_name,category,service,contact_name,phone,email,website,
...                          address,city,state,zip,notes
...                   FROM vendors
...                   WHERE (business_name LIKE :x OR category LIKE :x OR service LIKE :x
...                          OR COALESCE(computed_keywords,'') LIKE :x
...                          OR COALESCE(city,'') LIKE :x OR COALESCE(state,'') LIKE :x)
...                   ORDER BY business_name COLLATE NOCASE ASC
...                 """),
...                 cx,
...                 params={"x": f"%{q}%"},
...             )
...         return pd.read_sql_query(
...             T("""
...               SELECT id,business_name,category,service,contact_name,phone,email,website,
...                      address,city,state,zip,notes
...               FROM vendors
...               ORDER BY business_name COLLATE NOCASE ASC
...             """),
...             cx,
...         )
... 
2025-10-19 01:40:31.255 WARNING streamlit.runtime.caching.cache_data_api: No runtime found, using MemoryCacheStorageManager
>>> st.title("Providers (Read-Only)")
2025-10-19 01:40:31.256 WARNING streamlit.runtime.scriptrunner_utils.script_run_context: Thread 'MainThread': missing ScriptRunContext! This warning can be ignored when running in bare mode.
2025-10-19 01:40:31.442 WARNING streamlit: 
  Warning: to view a Streamlit app on a browser, use Streamlit in a file and
  run it with the following command:

    streamlit run [FILE_NAME] [ARGUMENTS]
2025-10-19 01:40:31.442 WARNING streamlit.runtime.scriptrunner_utils.script_run_context: Thread 'MainThread': missing ScriptRunContext! This warning can be ignored when running in bare mode.
DeltaGenerator()
>>> 
>>> left, right = st.columns([3, 1])
2025-10-19 01:40:31.443 WARNING streamlit.runtime.scriptrunner_utils.script_run_context: Thread 'MainThread': missing ScriptRunContext! This warning can be ignored when running in bare mode.
2025-10-19 01:40:31.443 WARNING streamlit.runtime.scriptrunner_utils.script_run_context: Thread 'MainThread': missing ScriptRunContext! This warning can be ignored when running in bare mode.
2025-10-19 01:40:31.443 WARNING streamlit.runtime.scriptrunner_utils.script_run_context: Thread 'MainThread': missing ScriptRunContext! This warning can be ignored when running in bare mode.
>>> with left:
...     q = st.text_input("Search", value="", placeholder="name, category, service, city, keyword…").strip()
... with right:
  File "<stdin>", line 3
    with right:
    ^^^^
SyntaxError: invalid syntax
>>>     if st.button("Clear"):
  File "<stdin>", line 1
    if st.button("Clear"):
IndentationError: unexpected indent
>>>         q = ""
  File "<stdin>", line 1
    q = ""
IndentationError: unexpected indent
>>> 
>>> df = load_df(q)
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
NameError: name 'q' is not defined
>>> 
>>> if df.empty:
...     st.info("No matching providers.")
... else:
...     st.caption(f"Results: {len(df)}")
...     st.dataframe(df, use_container_width=True)
...     st.download_button(
...         "Download CSV",
...         df.to_csv(index=False).encode("utf-8"),
...         file_name="providers.csv",
...         mime="text/csv",
...     )
... 
