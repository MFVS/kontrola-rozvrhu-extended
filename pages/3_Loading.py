import streamlit as st
import time

with st.spinner("Tato stránka je zatím nevytvořena."):
    time.sleep(5)
st.success("Redirecting.")
st.switch_page("app_ps.py")