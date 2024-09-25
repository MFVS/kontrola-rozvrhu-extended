import streamlit as st
import xlsx_generator as tablegen

st.dataframe(tablegen.pull_data(
    search_type=st.session_state["search_option"],
    search_target=st.session_state["search_field"],
    stag_user=st.session_state["stagRoleName"],
    ticket_over=st.session_state["stagUserTicket"],
    year=st.session_state["year"],
    lang=st.session_state["lang"]
))

