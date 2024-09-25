import polars as pl
from typing import Tuple, Dict, List
from io import StringIO

# Notes:
# - Remove all auth things

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

def relist(dataframe:pl.DataFrame) -> pl.DataFrame:
    casting = {dataframe.columns[col_index]:pl.List(col_type) for col_index, col_type in enumerate(dataframe.dtypes) if col_type != pl.String}
    return dataframe.cast(casting)

# ----- FUNKCE GENERUJÍCÍ CSV -----

def katedra(katedra:str, ticket:str, auth:Tuple[str, str] = None, year:str | None = None, stag_user:str | None = None, lang:str = "cs") -> Dict[str, str]: # Vrací jméno souboru
    assert stag_user != None, "This requires the user to login."

    params_rozvrh = {
        "stagUser": stag_user, # Fix this
        "semestr":"%",
        "vsechnyCasyKonani":"false",
        "jenRozvrhoveAkce":"true",
        "vsechnyAkce":"false",
        "jenBudouciAkce":"false",
        "lang":lang,
        "katedra":katedra[0],
        "rok":year
    }
    params_predmety = {
        "lang":lang,
        "katedra":katedra[0],
        "jenNabizeneECTSPrijezdy":"false",
        "rok":year
    }

    # Excel rozvrhy funguje jen s validním přihlášením
    
    excel_rozvrhy = pl.read_csv(fetch_csv(service="/rozvrhy/getRozvrhByKatedra", params_plus=params_rozvrh, ticket=ticket, auth=auth), separator=";")
    # excel_rozvrhy.write_csv(file_names["rozvrhy"])

    # # Excel předměty funguje i bez přihlášení
    excel_predmety = pl.read_csv(fetch_csv(service="/predmety/getPredmetyByKatedraFullInfo", params_plus=params_predmety), separator=";")
    # excel_predmety.write_csv(file_names["predmety"])

    for subkatedra in katedra[1:]:
        params_rozvrh["katedra"] = subkatedra
        params_predmety["katedra"] = subkatedra

        temp_rozvrhy = pl.read_csv(fetch_csv(service="/rozvrhy/getRozvrhByKatedra", params_plus=params_rozvrh, ticket=ticket, auth=auth), separator=";")
        temp_rozvrhy.write_csv(f"source_testing/temp_rozvrh_{subkatedra}.csv")
        fix_list_rozvrhy = type_check(excel_rozvrhy, temp_rozvrhy) # Potenciálně se dá hodit rovnou do funkce, možná ušetřit trochu prostoru v paměti

        for col_type in fix_list_rozvrhy.keys():
            if col_type in [pl.Int64, pl.Float64]:
                temp_rozvrhy = null_out(temp_rozvrhy, fix_list_rozvrhy[col_type]) # Mění "" na None, přechází erroru při konverzi na int
            temp_rozvrhy = temp_rozvrhy.with_columns(pl.col(fix_list_rozvrhy[col_type]).cast(col_type))

        excel_rozvrhy = excel_rozvrhy.vstack(other=temp_rozvrhy)
        temp_predmety = pl.read_csv(fetch_csv(service="/predmety/getPredmetyByKatedraFullInfo", params_plus=params_predmety),separator=";")
        temp_predmety.write_csv(f"source_testing/temp_predmety_{subkatedra}.csv")
        excel_predmety = excel_predmety.vstack(other=temp_predmety)


    excel_rozvrhy.write_csv("source_testing/rozvrhy_katedra.csv")
    excel_predmety.write_csv("source_testing/predmety_katedra.csv")
    return {
        "rozvrhy":excel_rozvrhy,
        "predmety":excel_predmety
    }

def fakulta(fakulta:str, ticket:str, auth:Tuple[str, str] = None, year:str | None = None, stag_user:str | None = None, lang:str = "cs") -> Dict[str, str]: 
    from stag_bombator_raw import prep_csv
    # Malá poznámka: Neexistuje (minimálně jsem jej nenašel) způsob jak získat rozvrh fakulty, takže procházím rozvrh všech kateder a lepím je na sebe Herkulesem
    assert stag_user != None, "This requires the user to login."

    print("hello")
    params_kateder = {
        "typPracoviste":"K",
        "zkratka":"%",
        "nadrazenePracoviste":""
    }
    katedry_list = set()

    print("am")
    # Načítá seznam kateder pod fakultou
    for subfakulta in fakulta:
        params_kateder["nadrazenePracoviste"] = subfakulta
        katedry_csv = pl.read_csv(fetch_csv(service="/ciselniky/getSeznamPracovist", params_plus=params_kateder, ticket=ticket, auth=auth), separator=";")

        katedry_list = katedry_list | set(katedry_csv.to_series(2).to_list())

    katedry_list = list(katedry_list)
    if len(katedry_list) < 1:
        katedry_list.append("Hehe_am_errorous")

    loner = katedry_list.pop(0)

    params_rozvrh = {
        "stagUser": stag_user,
        "semestr":"%",
        "vsechnyCasyKonani":"false",
        "jenRozvrhoveAkce":"true",
        "vsechnyAkce":"false",
        "jenBudouciAkce":"false",
        "lang":lang,
        "katedra":loner,
        "rok":year
    }
    params_predmety = {
        "lang":lang,
        "fakulta":fakulta[0],
        "jenNabizeneECTSPrijezdy":"false",
        "rok":year
    }

    print("here")
    excel_rozvrhy = relist(pl.read_csv(fetch_csv(service="/rozvrhy/getRozvrhByKatedra", params_plus=params_rozvrh, ticket=ticket, auth=auth), separator=";", infer_schema_length=0))
    excel_predmety = pl.read_csv(fetch_csv(service="/predmety/getPredmetyByFakultaFullInfo", params_plus=params_predmety, ticket=ticket, auth=auth), separator=";", infer_schema_length=0)

    for subfakulta in fakulta[1:]:
        params_predmety["fakulta"] = subfakulta
        temp_predmety = pl.read_csv(fetch_csv(service="/predmety/getPredmetyByFakultaFullInfo", params_plus=params_predmety, ticket=ticket, auth=auth), separator=";", infer_schema_length=0)

        fix_list_predmety = type_check(excel_predmety, temp_predmety) # Potenciálně se dá hodit rovnou do funkce, možná ušetřit trochu prostoru v paměti

        for col_type in fix_list_predmety.keys():
            if col_type in [pl.Int64, pl.Float64]:
                temp_predmety = null_out(temp_predmety, fix_list_predmety[col_type]) # Mění "" na None, přechází erroru při konverzi na int
            temp_predmety = temp_predmety.with_columns(pl.col(fix_list_predmety[col_type]).cast(col_type))
        
        prep_csv(temp_predmety).write_csv(f"source_testing/temp_predmety_{subfakulta}.csv")
        excel_predmety = excel_predmety.vstack(other=temp_predmety)

        excel_predmety.vstack(other=temp_predmety, in_place=True)

    print("not")
    for katedra in katedry_list:
        print("Moving to: " + katedra)
        params_rozvrh["katedra"] = katedra

        print("Fetching CSVs.")
        temp_rozvrhy = pl.read_csv(fetch_csv(service="/rozvrhy/getRozvrhByKatedra", params_plus=params_rozvrh, ticket=ticket, auth=auth), separator=";", infer_schema_length=0)

        print("Fetched CSVs successfully.")
        if temp_rozvrhy.__len__() == 0:
            print("One or more CSV's are empty. Continuing to next item.")
            continue

        print("Ensuring type consistency.")
        fix_list_rozvrhy = type_check(excel_rozvrhy, temp_rozvrhy) # Potenciálně se dá hodit rovnou do funkce, možná ušetřit trochu prostoru v paměti

        for col_type in fix_list_rozvrhy.keys():
            if col_type in [pl.Int64, pl.Float64]:
                temp_rozvrhy = null_out(temp_rozvrhy, fix_list_rozvrhy[col_type]) # Mění "" na None, přechází erroru při konverzi na int
            temp_rozvrhy = temp_rozvrhy.with_columns(pl.col(fix_list_rozvrhy[col_type]).cast(col_type))

        print("Type consistency ensured. Saving testing file and appending the main dataframes.")
        # temp_predmety.write_csv("source_testing/predmety"+str(num)+".csv")
        
        excel_rozvrhy = excel_rozvrhy.vstack(temp_rozvrhy)
        print("Dataframes appended. Continuing to next item.")

    print("here")

    prep_csv(excel_rozvrhy).write_csv("source_testing/rozvrhy_PRF.csv")
    prep_csv(excel_predmety).write_csv("source_testing/predmety_PRF.csv")

    return {
        "rozvrhy":excel_rozvrhy,
        "predmety":excel_predmety
    }
    

def ucitel(id_ucitele:int, ticket:str, auth:Tuple[str, str] = None, year:str | None = None, stag_user:str | None = None, lang:str = "cs") -> Dict[str, str]: # Tady se dějou nějaký weird věci... Znovu se na to koukni a porovnej to s tím jak handleuješ fakultu.
    assert stag_user != None, "This requires the user to login."

    params_rozvrh = {
        "stagUser": stag_user,
        "semestr":"%",
        "vsechnyCasyKonani":"false",
        "jenRozvrhoveAkce":"true",
        "vsechnyAkce":"false",
        "jenBudouciAkce":"false",
        "lang":lang,
        "ucitIdno":id_ucitele[0],
        "rok":year
    }
    params_predmety = {
        "lang":lang,
        "ucitIdno":id_ucitele[0],
        "jenCoMajiVyuku":True,
        "rok":year
    }

    # Rozvrh
    rozvrh_ucitel = pl.read_csv(fetch_csv("/rozvrhy/getRozvrhByUcitel", ticket, params_rozvrh, auth), separator=";", infer_schema_length=0)

    # Předměty
    predmety_ucitel_list = pl.read_csv(fetch_csv("/predmety/getPredmetyByUcitel", ticket, params_predmety, auth), separator=";", infer_schema_length=0)

    for subucitel in id_ucitele[1:]:
        params_predmety["ucitIdno"] = subucitel
        params_rozvrh["ucitIdno"] = subucitel

        temp_rozvrhy = pl.read_csv(fetch_csv("/rozvrhy/getRozvrhByUcitel", ticket, params_rozvrh, auth), separator=";", infer_schema_length=0)

        # fix_list_rozvrhy = type_check(rozvrh_ucitel, temp_rozvrhy) # Potenciálně se dá hodit rovnou do funkce, možná ušetřit trochu prostoru v paměti

        # for col_type in fix_list_rozvrhy.keys():
        #     if col_type in [pl.Int64, pl.Float64]:
        #         temp_rozvrhy = null_out(temp_rozvrhy, fix_list_rozvrhy[col_type]) # Mění "" na None, přechází erroru při konverzi na int
        #     temp_rozvrhy = temp_rozvrhy.with_columns(pl.col(fix_list_rozvrhy[col_type]).cast(col_type))

        rozvrh_ucitel.vstack(other=temp_rozvrhy, in_place=True)
        predmety_ucitel_list.vstack(other=pl.read_csv(fetch_csv("/predmety/getPredmetyByUcitel", ticket, params_predmety, auth), separator=";", infer_schema_length=0), in_place=True)

    predmety_ucitel_list.write_csv("source_testing/ucitel_predmety_lite")
    katedry_list = predmety_ucitel_list.to_series(2).unique().to_list()
    print(katedry_list)

    if len(katedry_list) < 1:
        katedry_list.append("Hehe_am_errorous")

    loner = katedry_list.pop(0)

    params_kat_predmety = {
        "lang":lang,
        "katedra":loner,
        "jenNabizeneECTSPrijezdy":"false",
        "rok":year
    }
    
    katedra_predmety = pl.read_csv(fetch_csv("/predmety/getPredmetyByKatedraFullInfo", ticket, params_kat_predmety, auth), separator=";", infer_schema_length=0)

    predmety_complete = predmety_ucitel_list.filter(pl.col("katedra") == loner).select("zkratka").join(katedra_predmety, "zkratka", "inner")

    for katedra in katedry_list:
        params_kat_predmety["katedra"] = katedra
        print(params_kat_predmety["katedra"])
        katedra_predmety = pl.read_csv(fetch_csv("/predmety/getPredmetyByKatedraFullInfo", ticket, params_kat_predmety, auth), separator=";", infer_schema_length=0)
        print(katedra_predmety.head())
        temp_predmety = predmety_ucitel_list.filter(pl.col("katedra") == katedra).select("zkratka").join(katedra_predmety, "zkratka", "inner")
        predmety_complete = predmety_complete.vstack(temp_predmety)

    return {
        "rozvrhy":rozvrh_ucitel,
        "predmety":predmety_complete
    }

def studijni_program(sp_ids:List[str], ticket:str, auth:Tuple[str, str] = None, year:str | None = None, stag_user:str | None = None, lang:str = "cs") -> Dict[pl.DataFrame, pl.DataFrame]:
    #raise NotImplemented("Maybe To-do? No clue how we would go about doing this.")
    params_sp = {
        "oborIdno":"",
        "rok":year,
        "vyznamPredmetu":"B",
        "lang":lang
    }

    katedra_list = set()

    for one_id in sp_ids:
        katedra_temp = pl.read_csv(fetch_csv(service="/predmety/getPredmetyByOborFullInfo", ticket=ticket, params_plus=params_sp), separator=";", infer_schema_length=0).to_series(0).to_list()
        katedra_list = katedra_list | set(katedra_temp)

    katedra_list = list(katedra_list)

    return katedra(katedra=katedra_list, ticket=ticket, auth=auth, year=year, stag_user=stag_user, lang=lang)

# ----- HANDLER ----- 

def pull_data(search_type:str, search_target:str, stag_user:str, ticket_over:str | None = None, auth_over:Tuple[str, str] | None = None, year:str | None = None, lang:str = "cs"):
    # TODO: Přidej dynamické pojmenování vygenerovaných tabulek

    assert search_type != None, "Missing type of search."
    assert search_target != None, "Missing search keyword."

    if not type(search_target) is list:
        assert search_target != "", "Missing search keyword."
        search_target = list(search_target)
    else:
        assert search_target != [], "Missing search keyword."

    auth = auth_over
    
    ticket = ticket_over

    if year == None:
        year = str(get_academic_year()) # For debug

    search_areas = {
        "Fakulta":fakulta,
        "Katedra":katedra,
        "Studijní program":studijni_program,
        "Učitel":ucitel
    }

    return search_areas[search_type](search_target, ticket, auth, year, stag_user=stag_user, lang=lang)

# ----------

if __name__ == '__main__':
    pull_data(
        #ticket_over="30088f13cc4a64c91aef019587bf2a31f7ff7055306e11abaef001d927dd099a",
        ticket_over="0e15b29a9645418d3f5e81442c4139bbb9dfe28fb8e64b2206b9dab3e758ccea",
        search_type="Katedra",
        search_target="KI",
    )