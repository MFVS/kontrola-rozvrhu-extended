import streamlit as st
message = "You're logged in!" if "stagUserName" in st.session_state else "You're not logged in..."

st.write(message)