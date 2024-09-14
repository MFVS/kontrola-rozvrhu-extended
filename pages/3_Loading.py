import streamlit as st
#import time
import stag_bombator_raw as sbr

# my_year = st.session_state["year"].split("/")[0]
# st.write(my_year)
st.set_page_config(page_title="Načítání...",page_icon=":hourglass",layout="wide", initial_sidebar_state="collapsed")

with st.spinner("Zpracovávám data. Prosím, počkejte..."):
    sbr.send_the_bomb(
        search_type=st.session_state["search_option"],
        search_target=st.session_state["search_field"],
        stag_username=st.session_state["stagRoleName"],
        user_ticket=st.session_state["stagUserTicket"],
        year=st.session_state["year"],
        lang=st.session_state["lang"]
    )
st.success("Redirecting.")
st.switch_page("pages/1_Výpis_výsledků.py")