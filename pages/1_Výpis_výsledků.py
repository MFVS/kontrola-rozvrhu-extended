chyby_translator = {
    "Bez garanta":"bez_garanta",
    "Bez přednášejících":"chybi_prednasejici",
    "Bez cvičích":"chybi_cvicici",
    "Bez seminařicích":"chybi_seminarici",
    "Vice garantu":"vice_garantu",
    "Garant nepřednáší":"garant_neprednasi",
    "Garant neučí":"predmety_kde_garant_neuci",
    "Přednášející bez přednášek":"prednasejici_bez_prednasek",
    "Cvičící bez cvičení":"cvicici_bez_cviceni",
    "Seminařící bez seminářů":"seminarici_bez_seminare",
    "Přednášející mimo sylabus":"prednasky_bez_prednasejicich",
    "Cvičicí mimo sylabus":"cviceni_bez_cvicich",
    "Seminářicí mimo sylabus":"seminare_bez_seminaricich",
}

# chyby_translator = {
#     "Bez garanta":"bez_garanta",
#     "Bez přednášejících":"chybi_prednasejici",
#     "Bez cvičích":"chybi_cvicici",
#     "Bez seminařicích":"chybi_seminarici",
#     "Vice garantu":"vice_garantu",
#     "Garant nepřednáší":"garant_neprednasi",
#     "Garant neučí":"predmety_kde_garant_neuci",
#     "Přednášející bez přednášek":"prednasejici_bez_prednasek",
#     "Cvičící bez cvičení":"sample_file",
#     "Seminařící bez seminářů":"sample_file",
#     "Přednášející mimo sylabus":"sample_file",
#     "Cvičicí mimo sylabus":"sample_file",
#     "Seminářicí mimo sylabus":"sample_file",
# }

import pandas as pd
import streamlit as st
chyby = [name for name in st.session_state["wishes"].keys() if st.session_state["wishes"][name] == True]

st.set_page_config(page_title="Výpis výsledků",page_icon="random",layout="wide", initial_sidebar_state="collapsed")

hide_streamlit_style = """
            <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            </style>
            """
st.markdown(hide_streamlit_style, unsafe_allow_html=True)

col1, col2 = st.columns([0.8,0.2])

with col1:
    st.header("Výpis chyb")
with col2:
    if st.button("Zpět"):
        st.switch_page("app_ps.py")

tab_folder = st.tabs(chyby)

for tab_index,tab in enumerate(tab_folder):
    pre_ansi = ""
    with open("results_csv/"+chyby_translator[chyby[tab_index]]+"_"+st.session_state["stagRoleName"]+".csv", "rb") as file:
        tab.download_button("Stáhnout CSV", file, file_name=chyby[tab_index]+".csv") #TODO: Překonvertovat soubory z utf-8 na ANSI. Jinak to v excelu vyplivne gibberish.
    tab.dataframe(pd.read_csv("results_csv/"+chyby_translator[chyby[tab_index]]+"_"+st.session_state["stagRoleName"]+".csv", encoding='ansi', sep=";"))




#TODO: ...všechno.
# 1) Názvy chyb a tabs podle nich.
# 2) Výpis tabulek s chybama. QoL feature.
# 3) Download tlačítko pro každou tab + download všech chyb jako CSV.