# Importy
from typing import Dict, List
from datetime import datetime

# --- POMOCNÉ FUNKCE 1 ---

# Funkce pro uložení výběru zobrazení chyb a přechod na výpis výsledků
def page_escape(to_convert:Dict[str, bool]):
    st.session_state["wishes"] = to_convert
    st.switch_page("pages/1_Výpis_výsledků.py")

# Přečte, zda byly dány nějaké query parametry (extra parametry v adrese) a zapíše je do stavu systému. Neohrabané ale funkční.
def read_query_params() -> dict | None: #TODO: Parseování stagUser dat
    if "stagUserTicket" not in st.query_params:
        return None
    
    all_params = st.query_params.to_dict()

    return all_params

# Ok, takže:
# 1) Tenhle blok kódu si osvěží, jaký všechny pracoviště jsou na UJEPu (na vytvoření jejich seznamu) vždycky, když si někdo otevře tuhle stránku. Velmi neefektivní.
#   - V ideálním světě by se seznam pracovišť osvěžil jednou za den (nebo za týden, whatever) a uložil do JSONu, který potom importujem. Nicméně, nevím jak spustit nějakej skript jenom jednou každý den, takže tohle zatím postačí.
# 2) Vynechávám učitele. Ať tam zadaj ten čtyřčíselný kód, whatever. Zatím mi to přijde jako moc úsilí pro něco kde hledat učitele bude legit nemožný. Minimálně zatím. Máme před sebou důležitější věci.
#   - Už hledat katedry bude fakt slast...

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

# --- POMOCNÉ FUNKCE 2 ---

# Vygeneruje školní roky až do roku 2010/2011. Dunno proč, prostě to přišlo pod ruku.
def generate_years() -> List[str]:
    return [f"{year}/{year + 1}" for year in range(datetime.today().year, 2010, -1)]

# Nastavuje výchozí výběr roku v kolonce výběru roku. Nastaveno na momentální akademický rok, mimo prázdnin, kde už to hází další akademický rok.
def default_year() -> str:
    return 0 if datetime.today().month > 6 else 1


# --- Actual stránka ---
import streamlit as st
st.set_page_config(page_title="Hledání chyb",page_icon=":left_speech_bubble:",layout="wide", initial_sidebar_state="expanded")

# Pokračování zápisu do stavu sezení.
read_qp = read_query_params()
if read_qp != None:
    for key in read_qp.keys():
        st.session_state[key] = read_qp[key]

# Schování streamlit loga.
hide_streamlit_style = """
            <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            </style>
            """
st.markdown(hide_streamlit_style, unsafe_allow_html=True)

title_container, login_container = st.columns(spec=[0.8, 0.2])

# Nadpis
with title_container:
    st.title("Služba na hledání chyb v IS STAG")

# Login tlačítko
with login_container:
    # Změna zprávy při přihlášení/nepřihlášení
    if "stagUserTicket" not in st.session_state:
        login_message = "Nejste přihlášen/a."
        login_button_mess = "Přihlášení"
    else:
        login_message = "Jste přihlášen/a."
        login_button_mess = "Změna uživatele"
    
    st.caption(login_message)
    #Tlačítko
    st.write(f'''
            <a target="_self" href="https://ws.ujep.cz/ws/login?originalURL=http://localhost:8501">
                <button>
                    {login_button_mess}
                </button>
            </a>
        ''',
        unsafe_allow_html=True)
    
st.divider()

st.subheader("Vyplňte následující dotazník:") #TODO: Přidat reálnej formulář pomocí st.form.

col1, col2, col3 = st.columns(3)

with col1:
    st.session_state["search_option"] = st.selectbox("Hledat chyby podle:",["Fakulta","Katedra","Studijní program","Učitel"])

with col2:
    if st.session_state["search_option"] == "Fakulta": #TODO: Přidat zprávu, že tohle bude trvat...
        st.session_state["search_field"] = st.multiselect("Zvolte fakultu:",search_fields["Fakulta"])

    elif st.session_state["search_option"] == "Katedra":
        st.session_state["search_field"] = st.multiselect("Zvolte katedru:",search_fields["Katedra"]) #NOTE: Tohle ZAHLTÍ uživatele volbami, also možná lepší jména alá STAG?

    elif st.session_state["search_option"] == "Studijní program": #TODO: Studijní programy nejsou zatím podporovány.
        raise NotImplementedError("Tohle by mi zabralo další týden. Fuck you.")
        #target = st.multiselect("Zvolte studijní program:",["MFVS","Aplikovaná informatika","Ekonomika a management","Chemie a toxikologie","Geografie","a tak dále"]) #TODO: Tohle by odněkud mohlo jít získat, takže bychom to nemuseli psát ručně, a mohlo by se to updateovat
    
    elif st.session_state["search_option"] == "Učitel":
        st.session_state["search_field"] = st.multiselect("Zvolte učitele:",["učitel1","učitel2","učitel3","a tak dále"]) #TODO: Ditto

# Fakulta = st.selectbox("Zvolte fakultu:",["By default","","","","",""])
# if Fakulta == "By default":
    
with col3:
    year = st.selectbox(label="Zvolte akademický rok:",options=generate_years(),index=default_year()) #NOTE: Vrací stringy s {první rok}/{druhý rok}. Dají se z toho parseovat actually useful data, ale bylo by milé vrátit actually použitelný formát. No biggie tho.

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
    "Přednášející mimo sylabus",
    "Cvičicí mimo sylabus",
    "Seminářicí mimo sylabus"
]

# Dict pro zápis zda to reálně uživatel chce zobrazit nebo ne
wishes = {chyba:True for chyba in chyby}

# Formátování sloupců (jejich počet dle chyb)
num_of_issues = len(chyby)
half_issues = num_of_issues // 2

cols1 = st.columns(half_issues)
for a in range(half_issues): 
    with cols1[a]: 
        wishes[chyby[a]] = st.checkbox(chyby[a], value = True)

cols2 = st.columns(num_of_issues - half_issues) 
for b in range(num_of_issues - half_issues): 
    with cols2[b]: 
        wishes[chyby[b + half_issues]] = st.checkbox(chyby[b + half_issues], value = True) 

st.session_state["lang"] = st.selectbox("Zvolte jazyk:",["čeština","angličtina"])

#NOTE: Output format přesunut na stránku Výpis výsledků. Tam je to relevantnější, takže to potom bude méně cluttered.
#output_format = st.selectbox("Zvolte požadovaný formát výstupního souboru:",["CSV","XLS","XLSX"])

if st.button(label="Spustit"):
    page_escape(wishes)