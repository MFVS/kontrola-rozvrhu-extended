import streamlit as st
st.set_page_config(page_title="Hledání chyb",page_icon=":left_speech_bubble:",layout="wide", initial_sidebar_state="expanded")
st.markdown("""<style>
                {visibility: hidden;}
                {footer {visibility: hidden;}
                </style>
                """, unsafe_allow_html=True)
st.title("Služba na hledání chyb v IS STAG")
st.subheader("Vyplňte následující dotazník:")

main_options = st.selectbox("Hledat chyby podle:",["Fakulta","Katedra","Studijní program","Učitel"])
if main_options == "Fakulta":
    st.selectbox("Zvolte fakultu:",["PřF","PF","FSE","FSI","FF","FZS","FŽP","FUD"])
if main_options == "Katedra":
    st.selectbox("Zvolte katedru:",["KMA","KI","zbytek doplníme"])
if main_options == "Studijní program":
    st.selectbox("Zvolte studijní program:",["MFVS","Aplikovaná informatika","Ekonomika a management","Chemie a toxikologie","Geografie","a tak dále"])
if main_options == "Učitel":
    st.selectbox("Zvolte učitele:",["učitel1","učitel2","učitel3","a tak dále"])

# Fakulta = st.selectbox("Zvolte fakultu:",["By default","","","","",""])
# if Fakulta == "By default":
    

st.selectbox("Zvolte akademický rok:",["2023/2024","2022/2023","2021/2022","2020/2021","a tak dále"])

st.multiselect("Zvolte, které chyby chcete zobrazit:",["chyba 1","chyba 2","chyba 3", "chyba 4", "chyba 5"])

st.selectbox("Zvolte jazyk:",["čeština","angličtina"])

st.selectbox("Zvolte požadovaný formát výstupního souboru:",["CSV","XLS","XLSX"])

st.button("Spustit")
