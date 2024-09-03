# Ok, takže:
# 1) Tenhle blok kódu si osvěží, jaký všechny pracoviště jsou na UJEPu (na vytvoření jejich seznamu) vždycky, když si někdo otevře tuhle stránku. Velmi neefektivní.
#   - V ideálním světě by se seznam pracovišť osvěžil jednou za den (nebo za týden, whatever) a uložil do JSONu, který potom importujem. Nicméně, nevím jak spustit nějakej skript jenom jednou každý den, takže tohle zatím postačí.
# 2) Vynechávám učitele. Ať tam zadaj ten čtyřčíselný kód, whatever. Zatím mi to přijde jako moc úsilí pro něco kde hledat učitele bude legit nemožný.
#   - Už hledat katedry bude fakt slast...
# 3) NEOTESTOVANÝ. MOŽNÁ TO CRASHNE, NETUŠÍM CO DĚLÁM LMAO

# --- BLOCK OF STUPID ---
def workplace_list_gen(wplace_type:str | None = None):
    assert wplace_type != None, "Workplace type not specified."
    from xlsx_generator import fetch_csv
    import polars as pl

    return pl.read_csv(fetch_csv("/ciselniky/getSeznamPracovist", params_plus={"typPracoviste":wplace_type, "zkratka":"%","nadrazenePracoviste":"%"}), separator=";").to_series(2).to_list()

search_fields = {
    "Fakulta":workplace_list_gen("F"),
    "Katedra":workplace_list_gen("K")
}
# --- ---

# --- Actual stránka ---
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
    if main_options == "Fakulta": #TODO: Přidat zprávu, že tohle bude trvat...
        target = st.multiselect("Zvolte fakultu:",search_fields["Fakulta"])
    if main_options == "Katedra":
        target = st.multiselect("Zvolte katedru:",search_fields["Katedra"]) #NOTE: Tohle ZAHLTÍ uživatele volbami
    if main_options == "Studijní program": #NOTE: Studijní programy nejsou zatím podporovány.
        target = st.multiselect("Zvolte studijní program:",["MFVS","Aplikovaná informatika","Ekonomika a management","Chemie a toxikologie","Geografie","a tak dále"]) #TODO: Tohle by odněkud mohlo jít získat, takže bychom to nemuseli psát ručně, a mohlo by se to updateovat
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
