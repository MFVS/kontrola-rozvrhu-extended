#TODO: Zkontrolovat zda chyby jsou gramaticky správně.
#TODO: Nezobrazovat prázdné DFs.
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


import pandas as pd
import streamlit as st
chyby = [name for name in st.session_state["wishes"].keys() if st.session_state["wishes"][name] == True]
file_names = [f".\\results_csv\\"+chyby_translator[chyba]+"_"+st.session_state["stagRoleName"]+".csv" for chyba in chyby]

def zip_it_up():
    import zipfile
    storage = zipfile.ZipFile(f"zips/Chyby v rozvrhu {st.session_state["stagRoleName"]}.zip", mode="w") 
    for index, file in enumerate(file_names):
        storage.write(file, chyby[index]+".csv", compress_type=zipfile.ZIP_STORED)

    storage.close()



st.set_page_config(page_title="Výpis výsledků",page_icon="random",layout="wide", initial_sidebar_state="collapsed")

hide_streamlit_style = """
            <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            </style>
            """
st.markdown(hide_streamlit_style, unsafe_allow_html=True)

#TODO: Pozměnit hodnoty tak aby to nedělalo blbosti.
#Blbosti: Pokud se zmenší okno, tlačítko rozdělí text na několik řádků. Pokud je okno moc velké, je mezi tlačítky díra a zpět není úplně u kraje
col1, col2, col3 = st.columns([0.80,0.15,0.05])

with col1:
    st.header("Výpis chyb")
with col2:
    zip_it_up()
    with open(f"zips/Chyby v rozvrhu {st.session_state["stagRoleName"]}.zip", "rb") as myzip:
        st.download_button("Stáhnout všechno", myzip, "Chyby v rozvrhu.zip")
with col3:
    if st.button("Zpět"):
        st.switch_page("app_ps.py")

tab_folder = st.tabs(chyby)

for tab_index,tab in enumerate(tab_folder):
    with open(file_names[tab_index], "rb") as file:
        tab.download_button(f"Stáhnout {st.session_state["output_format"]}", file, file_name=chyby[tab_index]+"."+st.session_state["output_format"]) #TODO: Zobrazit soubor dle file-typu. Asi to bude vyžadovat funkci read_xlsx or sumin. Oh yea, obecně načítání s podporou více output formátů je fucked.
    if st.session_state["output_format"] == "CSV":
        tab.dataframe(pd.read_csv(file_names[tab_index], encoding='cp1250', sep=";"))
    else:
        tab.dataframe(pd.read_excel(file_names[tab_index])) #TODO: Ok this surely won't work
