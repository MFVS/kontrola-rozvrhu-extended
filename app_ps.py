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

col1, col2, col3 = st.columns(3)

with col1:
    main_options = st.selectbox("Hledat chyby podle:",["Fakulta","Katedra","Studijní program","Učitel"])

with col2:
    if main_options == "Fakulta":
        target = st.multiselect("Zvolte fakultu:",["PřF","PF","FSE","FSI","FF","FZS","FŽP","FUD"])
    if main_options == "Katedra":
        target = st.multiselect("Zvolte katedru:",["KMA","KI","zbytek doplníme"]) #TODO: Tohle by odněkud mohlo jít získat, takže bychom to nemuseli psát ručně, a mohlo by se to updateovat
    if main_options == "Studijní program":
        target = st.multiselect("Zvolte studijní program:",["MFVS","Aplikovaná informatika","Ekonomika a management","Chemie a toxikologie","Geografie","a tak dále"]) #TODO: Ditto
    if main_options == "Učitel":
        target = st.multiselect("Zvolte učitele:",["učitel1","učitel2","učitel3","a tak dále"]) #TODO: Ditto

# Fakulta = st.selectbox("Zvolte fakultu:",["By default","","","","",""])
# if Fakulta == "By default":
    
with col3:
    year = st.selectbox("Zvolte akademický rok:",["2023/2024","2022/2023","2021/2022","2020/2021","a tak dále"]) # TODO: Přidat automatickou generaci školního roku (also, roky potřebujeme ve formátu {počáteční rok ŠR})

st.subheader("Filtrování typů chyb")

chyby = [
    "Bez garanta",
    "Bez přednášejících",
    "Bez cvičích",
    "Bez seminařicích",
    "Garant nepřednáší",
    "Garant neučí",
    "Přednášející bez přednášek",
    "Cvičící bez cvičení",
    "Seminařící bez seminářů",
    "Přednášky bez přednášejících",
    "Cvičení bez cvičících",
    "Semináře bez seminařicích"
]

# ALTERNATIVNÍ ŘEŠENÍ

# display_list = st.multiselect(
#     label="Filtrování typů chyb",
#     options=chyby,
#     default=chyby # Idk zda tohle funguje
# )

cols1 = st.columns(7)
for i in range(7): 
    with cols1[i]: 
        num = st.checkbox(chyby[i], value = True)

cols2 = st.columns(4) 
for i in range(4): 
    with cols2[i]: 
        num = st.checkbox(chyby[i], value = True) 

lang = st.selectbox("Zvolte jazyk:",["čeština","angličtina"])

output_format = st.selectbox("Zvolte požadovaný formát výstupního souboru:",["CSV","XLS","XLSX"])

st.button("Spustit")
