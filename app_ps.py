# Importy
from typing import Dict, List
from datetime import datetime
from xlsx_generator import fetch_csv
import polars as pl

# --- POMOCNÉ FUNKCE 1 ---

# Funkce pro uložení výběru zobrazení chyb a přechod na výpis výsledků
def page_escape(to_convert:Dict[str, bool]):
    st.session_state["wishes"] = to_convert
    #st.switch_page("pages/4_Docker_testing.py")
    st.switch_page("pages/3_Loading.py")

def get_best_role(encoded:str):
    import base64
    import json
    decoded = base64.b64decode(encoded)
    decoded = decoded.decode("utf-8")

    roles = json.loads(decoded)["stagUserInfo"]
    
    return roles[0]["userName"]

# Přečte, zda byly dány nějaké query parametry (extra parametry v adrese) a zapíše je do stavu systému. Neohrabané ale funkční.
def read_query_params() -> None:
    if "stagUserTicket" not in st.query_params:
        return
    
    st.session_state["stagUserTicket"] = st.query_params["stagUserTicket"]
    st.session_state["stagUserName"] = st.query_params["stagUserName"]
    st.session_state["stagRoleName"] = get_best_role(st.query_params["stagUserInfo"])

def get_teachers() -> Dict[int, str]:
    import polars as pl
    from xlsx_generator import fetch_csv
    excel_ucitele = pl.read_csv(fetch_csv("/ciselniky/getCiselnik", params_plus={"domena":"UCITELE"}), separator=";")
    excel_ucitele.write_csv("source_tables/ciselnik_ucitelu.csv")

    names = excel_ucitele.to_series(excel_ucitele.get_column_index("nazev")).to_list()
    numbers = excel_ucitele.to_series(excel_ucitele.get_column_index("key")).to_list()

    return {numbers[x]:names[x] for x in range(len(names))}

def get_program_names(fakulta:List[str]=[], forma:List[str]=[], typ:List[int]=[]):
    params_sp = {"pouzePlatne":True, "lang":"cs"}
    params_sp["rok"] = 2024
    sp_table = pl.read_csv(fetch_csv("/programy/getStudijniProgramy", params_plus=params_sp), separator=";").select("nazev", "stprIdno", "typ", "forma", "fakulta", "nazevAn", "kod")

    fak_check = pl.col("fakulta").is_in(fakulta) if len(fakulta) > 0 else True
    form_check = pl.col("forma").is_in(forma) if len(forma) > 0 else True
    typ_check = pl.col("typ").is_in(typ) if len(typ) > 0 else True

    sp_table = sp_table.filter(fak_check & form_check & typ_check).with_columns(pl.concat_str(
        pl.when(pl.col("nazevAn").str.len_chars() > 0).then(pl.col("nazevAn")).otherwise(pl.col("nazev")),
        pl.concat_str(
            pl.col("typ").str.slice(0,1),
            pl.col("forma").str.slice(0,1),
            pl.col("fakulta"),
            pl.col("kod"),
        separator=","), separator=" - ").alias("Hello")).select("stprIdno", "Hello", "nazevAn")

    ids = sp_table.to_series(0).to_list()
    names = sp_table.to_series(1).to_list()
    return {ids[x]:names[x] for x in range(len(ids))}

    #raise NotImplementedError("Today. But later.")
    # information = pl.read_csv(fetch_csv(service="/ciselniky/getCiselnik", params_plus={"domena":"OBOR", "lang":"cs"}))
    # names:List[str] = information.to_series(information.get_column_index("nazev")).to_list()
    # codes = information.to_series(information.get_column_index("key")).to_list()

    # for row in names:
    #     pass


# Ok, takže:
# 1) Tenhle blok kódu si osvěží, jaký všechny pracoviště jsou na UJEPu (na vytvoření jejich seznamu) vždycky, když si někdo otevře tuhle stránku. Velmi neefektivní.
#   - V ideálním světě by se seznam pracovišť osvěžil jednou za den (nebo za týden, whatever) a uložil do JSONu, který potom importujem. Nicméně, nevím jak spustit nějakej skript jenom jednou každý den, takže tohle zatím postačí.
# 2) Vynechávám učitele. Ať tam zadaj ten čtyřčíselný kód, whatever. Zatím mi to přijde jako moc úsilí pro něco kde hledat učitele bude legit nemožný. Minimálně zatím. Máme před sebou důležitější věci.
#   - Už hledat katedry bude fakt slast...

# --- BLOCK OF STUPID ---
def workplace_list_gen(wplace_type:str | None = None):
    assert wplace_type != None, "Workplace type not specified."

    return pl.read_csv(fetch_csv("/ciselniky/getSeznamPracovist", params_plus={"typPracoviste":wplace_type, "zkratka":"%","nadrazenePracoviste":"%"}), separator=";").to_series(2).to_list()

search_fields = {
    "Fakulta":workplace_list_gen("F"),
    "Katedra":workplace_list_gen("K"),
    #"Studijní program":get_program_names()
}

# --- POMOCNÉ FUNKCE 2 ---

# Vygeneruje školní roky až do roku 2010/2011. Dunno proč, prostě to přišlo pod ruku.
def generate_years() -> List[str]:
    return [year for year in range(datetime.today().year, 2010, -1)]

sp_type_list = [
    "Navazující",
    "Univerzita 3. věku",
    "Mezinárodně uznávaný kurz",
    "Celoživotní",
    "Ostatní",
    "Rigorózní",
    "Bakalářský",
    "Navazující",
    "Doktorský"
]

sp_form_list = [
    "Distanční",
    "Kombinovaná",
    "Prezenční"
]


# --- Actual stránka ---
import streamlit as st
st.set_page_config(page_title="Hledání chyb",page_icon=":left_speech_bubble:",layout="wide", initial_sidebar_state="collapsed")

# Nastavuje výchozí výběr roku v kolonce výběru roku. Nastaveno na momentální akademický rok, mimo prázdnin, kde už to hází další akademický rok.
def default_year() -> str:
    yearmod = 0 if datetime.today().month > 6 else 1
    if "year" not in st.session_state:
        return yearmod
# --- EXPERIMENTÁLNÍ ---
    else:
        return datetime.today().year - st.session_state["year"] - yearmod

# Pokračování zápisu do stavu sezení.
read_query_params()

# Schování streamlit loga.
hide_streamlit_style = """
            <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            </style>
            """
st.markdown(hide_streamlit_style, unsafe_allow_html=True)

# Schování sidebaru
# TODO: Přidat do ostatních stránek nebo smazat.
st.markdown(
    """
<style>
    [data-testid="collapsedControl"] {
        display: none
    }
</style>
""",
    unsafe_allow_html=True,
)

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

st.subheader("Vyplňte následující dotazník:") #TODO: Přidat reálnej formulář pomocí st.form. Volitelné, asi to reálně jenom věci zkomplikuje. Whatever.

col1, col2, col3 = st.columns(spec=3, gap="small")

with col1:
    st.session_state["search_option"] = st.selectbox("Hledat chyby podle:",["Fakulta","Katedra","Studijní program","Učitel"])

with col2:
    if st.session_state["search_option"] == "Fakulta":
        st.session_state["search_field"] = st.multiselect("Zvolte fakultu:",search_fields["Fakulta"])
        st.info("Zpracování může trvat i pár minut... Prosím, mějte strpení.")

    elif st.session_state["search_option"] == "Katedra":
        st.session_state["search_field"] = st.multiselect("Zvolte katedru:",search_fields["Katedra"]) #NOTE: Tohle ZAHLTÍ uživatele volbami, also možná lepší jména alá STAG?

    elif st.session_state["search_option"] == "Studijní program":
        sp_fakulta = st.multiselect("Fakulta studijního programu:",search_fields["Fakulta"])
        sp_type = st.multiselect("Typ studijního programu:", sp_type_list)
        sp_form = st.multiselect("Forma studijního programu:", sp_form_list)
        picks = get_program_names(fakulta=sp_fakulta, typ=sp_type, forma=sp_form)
        st.session_state["search_field"] = st.multiselect(label="Zvolte studijní program:",options=picks.keys(),format_func=lambda x:picks[x])
    
    elif st.session_state["search_option"] == "Učitel":
        teacher_translator = get_teachers()
        st.session_state["search_field"] = st.multiselect("Zvolte učitele:",teacher_translator.keys(), format_func=lambda x:f"{teacher_translator[x]} ({str(x)})")
    
with col3: #TODO: Experimentální zapamatování roku v default year. Zkontrolovat funkčnost.
    st.session_state["year"] = st.selectbox(label="Zvolte akademický rok:",options=generate_years(),index=default_year(),format_func=lambda x: f"{x}/{x+1}") #NOTE: Vrací stringy s {první rok}/{druhý rok}. Dají se z toho parseovat actually useful data, ale bylo by milé vrátit actually použitelný formát. No biggie tho.

st.subheader("Filtrování typů chyb")

# TODO: Opravit pravopisné/syntaktické/whatever chyby
chyby = [
    "Bez garanta",
    "Bez přednášejících",
    "Bez cvičích",
    "Bez seminařicích",
    "Více garantů",
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

# TODO: Zlepšit formátování (pozměnit počet sloupců tak aby se rovnal)
# - Generovat half_issues + num_of_issues % 2 sloupců, přistoupit k chybě x a chybě x + half_issues
cols1 = st.columns(half_issues)
for a in range(half_issues): 
    with cols1[a]: 
        wishes[chyby[a]] = st.checkbox(chyby[a], value = True)

cols2 = st.columns(num_of_issues - half_issues) 
for b in range(num_of_issues - half_issues): 
    with cols2[b]: 
        wishes[chyby[b + half_issues]] = st.checkbox(chyby[b + half_issues], value = True) 

lang_translate = {
    "cs":"Čeština",
    "en":"Angličtina"
}

st.session_state["lang"] = st.selectbox("Zvolte jazyk:",lang_translate.keys(), 0, lambda x: lang_translate[x])

st.session_state["output_format"] = st.selectbox("Zvolte požadovaný formát výstupního souboru:",["CSV","XLSX"])

if "stagUserTicket" not in st.session_state.keys():
    st.warning("Uživatel nepřihlášen.")
elif st.session_state["search_field"] == []:
    st.warning("Hledaný termín nevybrán.")
else:
    if st.button(label="Spustit"):
        page_escape(wishes)