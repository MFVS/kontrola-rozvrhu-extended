import polars as pl
from typing import Tuple, Dict, List
from io import StringIO

# --- LOGIN ---
def login(over_name:str | None = None, over_pass:str | None = None) -> Tuple[str, str] | None: # Over_name a over_pass jsou override parametry pro účely lazení
    import os
    from dotenv import load_dotenv

    load_dotenv()
    user = os.getenv("STAG_USER")
    password = os.getenv("STAG_PASSWORD")

    if user == None or password == None:
        return login_correction(over_name, over_pass)
    else:
        return (user, password)

def login_correction(manual_login:Tuple[str, str]) -> Tuple[str, str] | None:
    if not isinstance(manual_login[0], str) or not isinstance(manual_login[1], str) or manual_login == None:
        return None
    else:
        return (manual_login[0], manual_login[1])
    

# --- CSV FETCHING ---
def fetch_csv(service:str = "",ticket:str = "", params_plus:dict = {}, auth:Tuple[str, str] = None) -> 'StringIO': # manual_login formát: (jméno, heslo)
    import requests

    assert service != "", "Service is necessary"
    
    url = "https://ws.ujep.cz/ws/services/rest2" + service

    params = {
        "outputFormat":"CSV",
        "outputFormatEncoding":"utf-8"
    }

    params.update(params_plus)

    cookies = {}
    if ticket != "":
        cookies.update({"WSCOOKIE":ticket})

    data = requests.get(url, params=params, cookies=cookies, auth=auth)

    wrap = StringIO(data.text)
    return wrap

# --- MISC METODY ---
def type_check(dataframe1:"pl.DataFrame", dataframe2:"pl.DataFrame") -> Dict[pl.DataType, List[str]]:
    """Checks if there are type differences between two dataframes. Created to fix type mismatches between two dataframes with same columns.

    Args:
        dataframe1 (pl.DataFrame): The "example" dataframe. The columns in the other df are converted to types of this dataframe.
        dataframe2 (pl.DataFrame): The corrected dataframe.

    Returns:
        Dict[pl.DataType, List[str]]: Dictionary in format: {type to convert to : names of columns to convert}
    """
    types1 = dataframe1.dtypes
    types2 = dataframe2.dtypes
    problems = {}
    for index in range(len(types1)):
        if types1[index] != types2[index]:
            if types1[index] in problems.keys():
                problems[types1[index]].append(dataframe1.columns[index])
            else:
                problems.update({types1[index]:[dataframe1.columns[index]]})

    return problems

def get_academic_year() -> int: #UNTESTED
    from datetime import datetime

    cur_date = datetime.today()
    cur_year = cur_date.year

    if cur_date.month < 8:
        cur_year -= 1
    
    return cur_year

def null_out(dataframe:"pl.DataFrame", columns:List[str]) -> "pl.DataFrame":
    """Changes empty values to none, for int conversion purposes.

    Args:
        dataframe (pl.DataFrame): Source for the data.
        columns (List[str]): Columns which should be nulled out.

    Returns:
        pl.DataFrame: Fixed dataframe.
    """
    for column in columns:
        dataframe = dataframe.with_columns(
            pl.when(pl.col(column).str.len_chars() == 0)
            .then(None)
            .otherwise(pl.col(column))
            .name.keep()
            )
        
    return dataframe

def get_teachers() -> None:
    """A function to generate all teacher names and ID's. Saved directly to file, neccessary only for bombator purposes.
    """
    excel_ucitele = pl.read_csv(fetch_csv("/ciselniky/getCiselnik", params_plus={"domena":"UCITELE"}), separator=";")
    excel_ucitele.write_csv("source_tables/ciselnik_ucitelu.csv")


# ----- FUNKCE GENERUJÍCÍ CSV -----

def katedra(katedra:str, ticket:str, auth:Tuple[str, str] = None, year:str | None = None) -> None:

    params_rozvrh = {
        "stagUser": "F23112", # Fix this
        "semestr":"%",
        "vsechnyCasyKonani":"false",
        "jenRozvrhoveAkce":"true",
        "vsechnyAkce":"false",
        "jenBudouciAkce":"false",
        "lang":"cs",
        "katedra":katedra,
        "rok":year
    }
    params_predmety = {
        "lang":"cs",
        "katedra":katedra,
        "jenNabizeneECTSPrijezdy":"false",
        "rok":year
    }

    # Excel rozvrhy funguje jen s validním přihlášením
    excel_rozvrhy = pl.read_csv(fetch_csv(service="/rozvrhy/getRozvrhByKatedra", params_plus=params_rozvrh, ticket=ticket, manual_login=auth), separator=";")
    excel_rozvrhy.write_csv("source_tables/by_type/rozvrh_katedra.csv")

    # Excel předměty funguje i bez přihlášení
    excel_predmety = pl.read_csv(fetch_csv(service="/predmety/getPredmetyByKatedraFullInfo", params_plus=params_predmety), separator=";")
    excel_predmety.write_csv("source_tables/by_type/predmety_katedra.csv")

def fakulta(fakulta:str, ticket:str, auth:Tuple[str, str] = None, year:str | None = None) -> None:
    # Malá poznámka: Neexistuje (minimálně jsem jej nenašel) způsob jak získat rozvrh fakulty, takže procházím rozvrh všech kateder a lepím je na sebe Herkulesem

    params_kateder = {
        "typPracoviste":"K",
        "zkratka":"%",
        "nadrazenePracoviste":fakulta
    }

    # Načítá seznam kateder pod fakultou
    katedry_csv = pl.read_csv(fetch_csv(service="/ciselniky/getSeznamPracovist", params_plus=params_kateder, ticket=ticket, manual_login=auth), separator=";")

    katedry_list = katedry_csv.to_series(2)
    katedry_list = katedry_list.to_list()

    loner = katedry_list.pop(0)

    params_rozvrh = {
        "stagUser": "F23112",
        "semestr":"%",
        "vsechnyCasyKonani":"false",
        "jenRozvrhoveAkce":"true",
        "vsechnyAkce":"false",
        "jenBudouciAkce":"false",
        "lang":"cs",
        "katedra":loner,
        "rok":year
    }
    params_predmety = {
        "lang":"cs",
        "fakulta":fakulta,
        "jenNabizeneECTSPrijezdy":"false",
        "rok":year
    }

    excel_rozvrhy = pl.read_csv(fetch_csv(service="/rozvrhy/getRozvrhByKatedra", params_plus=params_rozvrh, ticket=ticket, manual_login=auth), separator=";")
    excel_predmety = pl.read_csv(fetch_csv(service="/predmety/getPredmetyByFakultaFullInfo", params_plus=params_predmety, ticket=ticket, manual_login=auth), separator=";")

    for katedra in katedry_list:
        print("Moving to: " + katedra)
        params_rozvrh["katedra"] = katedra

        print("Fetching CSVs.")
        temp_rozvrhy = pl.read_csv(fetch_csv(service="/rozvrhy/getRozvrhByKatedra", params_plus=params_rozvrh, ticket=ticket, manual_login=auth), separator=";")

        print("Fetched CSVs successfully.")
        if temp_rozvrhy.__len__() == 0:
            print("One or more CSV's are empty. Continuing to next item.")
            continue

        print("Ensuring type consistency.")
        fix_list_rozvrhy = type_check(excel_rozvrhy, temp_rozvrhy) # Potenciálně se dá hodit rovnou do funkce, možná ušetřit trochu prostoru v paměti

        for col_type in fix_list_rozvrhy.keys():
            if col_type == pl.Int64:
                temp_rozvrhy = null_out(temp_rozvrhy, fix_list_rozvrhy[col_type]) # Mění "" na None, přechází erroru při konverzi na int
            temp_rozvrhy = temp_rozvrhy.with_columns(pl.col(fix_list_rozvrhy[col_type]).cast(col_type))

        print("Type consistency ensured. Saving testing file and appending the main dataframes.")
        # temp_predmety.write_csv("source_testing/predmety"+str(num)+".csv")
        
        excel_rozvrhy = excel_rozvrhy.vstack(temp_rozvrhy)
        print("Dataframes appended. Continuing to next item.")

    excel_rozvrhy.write_csv("source_tables/rozvrhy_fakulta.csv")
    excel_predmety.write_csv("source_tables/predmety_fakulta.csv")
    

def ucitel(id_ucitele:int, ticket:str, auth:Tuple[str, str] = None, year:str | None = None): # Tady se dějou nějaký weird věci... Znovu se na to koukni a porovnej to s tím jak handleuješ fakultu.
    params_rozvrh = {
        "stagUser": "F23112",
        "semestr":"%",
        "vsechnyCasyKonani":"false",
        "jenRozvrhoveAkce":"true",
        "vsechnyAkce":"false",
        "jenBudouciAkce":"false",
        "lang":"cs",
        "ucitIdno":id_ucitele,
        "rok":year
    }
    params_predmety = {
        "lang":"cs",
        "ucitIdno":id_ucitele,
        "jenCoMajiVyuku":True,
        "rok":year
    }

    # Rozvrh
    rozvrh_ucitel = pl.read_csv(fetch_csv("/rozvrhy/getRozvrhByUcitel", ticket, params_rozvrh, auth), separator=";")
    rozvrh_ucitel.write_csv("source_tables/rozvrh_ucitel.csv")

    # Předměty
    predmety_ucitel_list = pl.read_csv(fetch_csv("/predmety/getPredmetyByUcitel", ticket, params_predmety, auth), separator=";")
    predmety_ucitel_list.write_csv("source_testing/ucitel_predmety_lite")
    katedry_list = predmety_ucitel_list.to_series(2).unique().to_list()
    print(katedry_list)

    loner = katedry_list.pop(0)

    params_kat_predmety = {
        "lang":"cs",
        "katedra":loner,
        "jenNabizeneECTSPrijezdy":"false",
        "rok":year
    }
    
    katedra_predmety = pl.read_csv(fetch_csv("/predmety/getPredmetyByKatedraFullInfo", ticket, params_kat_predmety, auth), separator=";")

    predmety_complete = predmety_ucitel_list.filter(pl.col("katedra") == loner).select("zkratka").join(katedra_predmety, "zkratka", "inner")

    for katedra in katedry_list:
        params_kat_predmety["katedra"] = katedra
        print(params_kat_predmety["katedra"])
        katedra_predmety = pl.read_csv(fetch_csv("/predmety/getPredmetyByKatedraFullInfo", ticket, params_kat_predmety, auth), separator=";")
        print(katedra_predmety.head())
        temp_predmety = predmety_ucitel_list.filter(pl.col("katedra") == katedra).select("zkratka").join(katedra_predmety, "zkratka", "inner")
        predmety_complete = predmety_complete.vstack(temp_predmety)

    predmety_complete.write_csv("source_tables/predmety_ucitel.csv")

def studijni_program():
    raise NotImplemented("Maybe To-do? No clue how we would go about doing this.")

# ----- HANDLER ----- 

def pull_data(search_type:str, search_target:str, ticket:str | None = None, auth_over:Tuple[str, str] | None = None, year:str | None = None):
    # TODO: Přidej dynamické pojmenování vygenerovaných tabulek

    assert search_type != None, "Missing type of search."
    assert search_target != None, "Missing search keyword."

    auth = login(auth_over[0], auth_over[1])

    get_teachers()

    if ticket == None:
        ticket = "30088f13cc4a64c91aef019587bf2a31f7ff7055306e11abaef001d927dd099a"

    if year == None:
        year = str(get_academic_year())

    search_areas = {
        "Fakulta":fakulta,
        "Katedra":katedra,
        "Studijní program":studijni_program,
        "Učitel":ucitel
    }

    search_areas[search_type](search_target, ticket, auth, year)

# ----------

if __name__ == '__main__':
    pull_data()