import streamlit as st
st.set_page_config(page_title="Hledání chyb",page_icon=":left_speech_bubble:",layout="wide", initial_sidebar_state="expanded")

hide_streamlit_style = """
            <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            </style>
            """
st.markdown(hide_streamlit_style, unsafe_allow_html=True)

st.title("Služba na hledání chyb v IS STAG")
st.subheader("Vyplňte následující dotazník:")

main_options = st.selectbox("Hledat chyby podle:",["Fakulta","Katedra","Studijní program","Učitel"])
if main_options == "Fakulta":
    st.multiselect("Zvolte fakultu:",["PřF","PF","FSE","FSI","FF","FZS","FŽP","FUD"])
if main_options == "Katedra":
    st.multiselect("Zvolte katedru:",["KMA","KI","zbytek doplníme"])
if main_options == "Studijní program":
    st.multiselect("Zvolte studijní program:",["MFVS","Aplikovaná informatika","Ekonomika a management","Chemie a toxikologie","Geografie","a tak dále"])
if main_options == "Učitel":
    st.multiselect("Zvolte učitele:",["učitel1","učitel2","učitel3","a tak dále"])

# Fakulta = st.selectbox("Zvolte fakultu:",["By default","","","","",""])
# if Fakulta == "By default":
    

st.selectbox("Zvolte akademický rok:",["2023/2024","2022/2023","2021/2022","2020/2021","a tak dále"])

cols = st.columns(3) 
for i in range(3): 
    with cols[i]: 
        num = st.checkbox(f'chyba {i+1}', value = True) 

st.selectbox("Zvolte jazyk:",["čeština","angličtina"])

st.selectbox("Zvolte požadovaný formát výstupního souboru:",["CSV","XLS","XLSX"])

st.button("Spustit")
