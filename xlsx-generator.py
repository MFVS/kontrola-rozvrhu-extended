import polars as pl
from typing import Tuple, Dict, List
from io import StringIO

def fetch_csv(service:str = "",ticket:str = "", params_plus:dict = {}, manual_login:Tuple[str, str] = None) -> 'StringIO': # manual_login formát: (jméno, heslo)
    import requests
    import os
    from dotenv import load_dotenv

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

    load_dotenv()

    user = os.getenv("STAG_USER")
    password = os.getenv("STAG_PASSWORD")

    if user == None or password == None:
        auth = login_correction(manual_login)
    else:
        auth = (user, password)

    data = requests.get(url, params=params, cookies=cookies, auth=auth)

    wrap = StringIO(data.text)
    return wrap

# Manuální login override (technicky underride ale meh) pro testování
def login_correction(manual_login:Tuple[str, str]) -> Tuple[str, str] | None:
    if not isinstance(manual_login[0], str) or not isinstance(manual_login[1], str) or manual_login == None:
        return None
    else:
        return (manual_login[0], manual_login[1])

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

def katedra(katedra:str, ticket:str, auth:tuple=None, year:str | None = None) -> None:
    #import polars as pl

    if year == None:
        year = str(get_academic_year())

    # Testovací data
    # DATA REDIGOVÁNA (nebudu se doxovat)
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

    # Side note: Obecně čtení v pythonu se silně nelíbí když neexistujicí složky kam maj chodit...
    # Excel rozvrhy funguje jen s validním přihlášením
    excel_rozvrhy = pl.read_csv(fetch_csv(service="/rozvrhy/getRozvrhByKatedra", params_plus=params_rozvrh, ticket=ticket, manual_login=auth), separator=";")
    excel_rozvrhy.write_csv("source_tables/rozvrh_complete.csv")

    # Excel předměty funguje i bez přihlášení
    excel_predmety = pl.read_csv(fetch_csv(service="/predmety/getPredmetyByKatedraFullInfo", params_plus=params_predmety), separator=";")
    excel_predmety.write_csv("source_tables/predmety_complete.csv")

    get_teachers()

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

def fakulta(fakulta:str, ticket:str, auth:tuple = None, year:str | None = None) -> None:

    get_teachers()

    if year == None:
        year = str(get_academic_year())

    params_kateder = {
        "typPracoviste":"K",
        "zkratka":"%",
        "nadrazenePracoviste":fakulta
    }
    #print(params_kateder["nadrazenePracoviste"])
    katedry_csv = pl.read_csv(fetch_csv(service="/ciselniky/getSeznamPracovist", params_plus=params_kateder, ticket=ticket, manual_login=auth), separator=";")
    katedry_list = katedry_csv.to_series(2)
    katedry_list = katedry_list.to_list()
    #print(katedry_list)


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
    # excel_predmety.write_csv("source_testing/predmety-1.csv")

    for katedra in katedry_list:
        print("Moving to: " + katedra)
        params_rozvrh["katedra"] = katedra
        # params_predmety["katedra"] = katedra

        print("Fetching CSVs.")

        temp_rozvrhy = pl.read_csv(fetch_csv(service="/rozvrhy/getRozvrhByKatedra", params_plus=params_rozvrh, ticket=ticket, manual_login=auth), separator=";")
        # temp_predmety = pl.read_csv(fetch_csv(service="/predmety/getPredmetyByKatedraFullInfo", params_plus=params_predmety), separator=";")

        print("Fetched CSVs successfully.")

        if temp_rozvrhy.__len__() == 0: # or temp_predmety.__len__() == 0
            print("One or more CSV's are empty. Continuing to next item.")
            continue

        print("Ensuring type consistency.")

        fix_list_rozvrhy = type_check(excel_rozvrhy, temp_rozvrhy)
        #fix_list_predmety = type_check(excel_predmety, temp_predmety)

        # for col_type in fix_list_predmety.keys():
        #     if col_type == pl.Int64:
        #         temp_predmety = null_out(temp_predmety, fix_list_predmety[col_type])
        #     temp_predmety = temp_predmety.with_columns(pl.col(fix_list_predmety[col_type]).cast(col_type))

        for col_type in fix_list_rozvrhy.keys():
            if col_type == pl.Int64:
                temp_rozvrhy = null_out(temp_rozvrhy, fix_list_rozvrhy[col_type])
            temp_rozvrhy = temp_rozvrhy.with_columns(pl.col(fix_list_rozvrhy[col_type]).cast(col_type))

        print("Type consistency ensured. Saving testing file and appending the main dataframes.")

        # temp_predmety.write_csv("source_testing/predmety"+str(num)+".csv")
        
        # excel_predmety = excel_predmety.vstack(temp_predmety)
        excel_rozvrhy = excel_rozvrhy.vstack(temp_rozvrhy)
        print("Dataframes appended. Continuing to next item.")

    excel_rozvrhy.write_csv("source_tables/rozvrhy_fakulta.csv")
    excel_predmety.write_csv("source_tables/predmety_fakulta.csv")
    

def ucitel(id_ucitele:int, ticket:str, auth:tuple=None, year:str | None = None):
    if year == None:
        year = str(get_academic_year())

    # Params
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

    # Číselník
    get_teachers()

    # Rozvrh
    rozvrh_ucitel = pl.read_csv(fetch_csv("/rozvrhy/getRozvrhByUcitel", ticket, params_rozvrh, auth), separator=";")
    rozvrh_ucitel.write_csv("source_tables/rozvrh_ucitel.csv")

    # Předměty
    # Problém: Je třeba full info.
    # Workaround: Left join seznamu předmětů dle katedry.
    predmety_ucitel_list = pl.read_csv(fetch_csv("/predmety/getPredmetyByUcitel", ticket, params_predmety, auth), separator=";")
    predmety_ucitel_list.write_csv("source_testing/ucitel_predmety_lite")
    katedry_list = predmety_ucitel_list.to_series(2).unique().to_list()
    print(katedry_list)

    loner = katedry_list.pop(0)
    print(loner)
    params_kat_predmety = {
        "lang":"cs",
        "katedra":loner,
        "jenNabizeneECTSPrijezdy":"false",
        "rok":year
    }
    
    katedra_predmety = pl.read_csv(fetch_csv("/predmety/getPredmetyByKatedraFullInfo", ticket, params_kat_predmety, auth), separator=";")
    print(katedra_predmety.head())

    predmety_complete = predmety_ucitel_list.filter(pl.col("katedra") == loner).select("zkratka").join(katedra_predmety, "zkratka", "inner")
    print(predmety_complete.head())

    for katedra in katedry_list:
        params_kat_predmety["katedra"] = katedra
        print(params_kat_predmety["katedra"])
        katedra_predmety = pl.read_csv(fetch_csv("/predmety/getPredmetyByKatedraFullInfo", ticket, params_kat_predmety, auth), separator=";")
        print(katedra_predmety.head())
        temp_predmety = predmety_ucitel_list.filter(pl.col("katedra") == katedra).select("zkratka").join(katedra_predmety, "zkratka", "inner")
        predmety_complete = predmety_complete.vstack(temp_predmety)
        #print(predmety_complete.head())

    predmety_complete.write_csv("source_tables/predmety_ucitel.csv")



def studijni_program():
    pass

if __name__ == '__main__':
    ticket = "30088f13cc4a64c91aef019587bf2a31f7ff7055306e11abaef001d927dd099a"
    auth = ("st101885", "x0301093100")

    #katedra(katedra="KI", ticket=ticket, auth=auth)
    #fakulta(fakulta="PRF", ticket=ticket, auth=auth)
    ucitel(261, ticket, auth)

