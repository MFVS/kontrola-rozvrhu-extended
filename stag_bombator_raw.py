import polars as pl
from typing import List
import xlsx_generator as tablegen
import utf_ansi_conv as uac

# --- POMOCNÉ FUNKCE ---

# Abych nebyl nařčen z plagiátorství, zdroj: https://stackoverflow.com/questions/75954280/how-to-change-the-position-of-a-single-column-in-python-polars-library
def replace(df:pl.DataFrame, new_position:int, col_name:str) -> List[str]:
    """Removes a column at specified position and inserts a named column in its stead.

    Args:
        df (pl.DataFrame): Modified dataframe.
        new_position (int): Position of removed column.
        col_name (str): Name of replacing column.

    Returns:
        List[str]: List of dataframe columns with correct order. Use on a selection to replace the column.
    """
    neworder=df.columns
    neworder.pop(new_position)
    neworder.remove(col_name)
    neworder.insert(new_position,col_name)
    return neworder

def fix_str_to_int(data:pl.DataFrame, fix_list:list) -> pl.DataFrame:
    """Converts columns from a provided list from string to list of ints.

    Args:
        data (pl.DataFrame): Modified dataframe.
        fix_list (list): List of columns to convert.

    Returns:
        pl.DataFrame: Dataframe with fixed columns.
    """

    column_types = data.select(fix_list).dtypes
    print(fix_list)
    print(column_types)

    # Pokud v žádném sloupci není víc hodnot, polars to převede na int automaticky.
    # for index, column in enumerate(fix_list):
    #     print("Going through:" + column)
    #     if column_types[index] != pl.String:
    #         print("Removing:" + column)
    #         fix_list.remove(column)

    fix_list = [column for index, column in enumerate(fix_list) if column_types[index] == pl.String]

    print(fix_list)

    data = data.with_columns(
        pl.col(fix_list)
        .str.split(",")
        .list.eval(
            pl.element()
            .str.strip_chars()
            .cast(pl.Int64, strict=False)
        )
    )

    # Převedení seznamů s pouze None hodnotami na None
    for column in fix_list:
        data = data.with_columns(pl.when(pl.col(column).list.eval(pl.element().is_null()).list.all()).then(None).otherwise(pl.col(column)).alias("bools"))
        data = data.select(replace(data, data.get_column_index(column), "bools")).rename({"bools":column})

    return data

def prep_csv(dataframe:pl.DataFrame) -> pl.DataFrame:
    """Prepares dataframe for export to CSV. This is done by converting lists to string.

    Args:
        dataframe (pl.DataFrame): Prepared dataframe.

    Returns:
        pl.DataFrame: Dataframe with no lists.
    """
    import numpy as np
    type_list = dataframe.dtypes
    mask = np.array([isinstance(column, pl.List) for column in type_list])
    column_list = np.array(dataframe.columns)

    if "identifier" in column_list:
        dataframe = dataframe.drop("identifier")

    list_columns = column_list[mask]
    print(list_columns)

    for column in list_columns:
        dataframe = dataframe.with_columns(pl.col(column).cast(pl.List(pl.Utf8)).list.join(","))
    return dataframe

# --- VYHLEDÁVÁNÍ CHYB ---

def has_teacher_theoretical(dataframe:pl.DataFrame, teacher_type:str) -> pl.DataFrame:
    """Find subjects with no teachers for a specific period type.

    Args:
        dataframe (pl.DataFrame): Dataframe with annotations of subjects.
        teacher_type (str): Teacher for that type of period (eg. prednasejici).

    Returns:
        pl.DataFrame: Dataframe containing offending subjects. Columns: katedra, zkratka, name of all teachers of that type (should be empty), teachers' ids (ditto), how many periods of that type, unit of the former.
    """
    to_period = {
        "prednasejici":"jednotekPrednasek",
        "cvicici":"jednotekCviceni",
        "seminarici":"jednotekSeminare",
    }
    period_type = to_period[teacher_type]

    has_period_type = pl.col(period_type) != 0
    has_no_teacher = pl.col(teacher_type + "UcitIdno").is_null()

    missing_teach = dataframe.filter(has_period_type & has_no_teacher).select("katedra", "zkratka", "identifier", teacher_type, teacher_type + "UcitIdno", period_type, period_type.replace("ek", "ka", 1).replace("ek","ky",1))

    return missing_teach

# --- HANDLER ---
def send_the_bomb(search_type:str, search_target:str, stag_username:str, user_ticket:str, year:int, lang:str):
    # Načtení všeho
    names = tablegen.pull_data(
        search_type=search_type,
        search_target=search_target,
        ticket_over=user_ticket,
        stag_user=stag_username,
        year=year,
        lang=lang
    )

    # Modifikátor jmen ukládaných souborů
    name_mod = "_"+stag_username

    # Zpracování CSV souborů na DataFrames
    rozvrh_by_kat = pl.read_csv(names["rozvrhy"]).drop("semestr").unique().rename({"predmet" : "zkratka"})
    rozvrh_by_kat = fix_str_to_int(rozvrh_by_kat, ["ucitIdno.ucitel", "vsichniUciteleUcitIdno"])
    rozvrh_by_kat = rozvrh_by_kat.with_columns(pl.concat_str([pl.col("katedra"), pl.col("zkratka")], separator="/").alias("identifier"))

    predmety_by_kat = pl.read_csv(names["predmety"]).drop("semestr").unique().drop(["pocetStudentu", "aSkut", "bSkut", "cSkut"])
    predmety_by_kat = fix_str_to_int(predmety_by_kat, ["garantiUcitIdno", "prednasejiciUcitIdno", "cviciciUcitIdno", "seminariciUcitIdno", "hodZaSemKombForma"])
    predmety_by_kat = predmety_by_kat.with_columns(pl.concat_str([pl.col("katedra"), pl.col("zkratka")], separator="/").alias("identifier"))

    # Vynechání nevalidních předmětů/rozvrhových akcí (pokud předmět nemá žádné korespondující rozvrhové akce a naopak)
    predmety_s_akci = predmety_by_kat.join(other=rozvrh_by_kat, on="identifier", how="inner").select(predmety_by_kat.columns).unique().sort("identifier")

    # Osekané rozvrhové akce
    maly_rozvrh = rozvrh_by_kat.select(["katedra","zkratka", "vsichniUciteleUcitIdno", "typAkceZkr", "rok", "datumOd", "datumDo", "hodinaSkutOd", "hodinaSkutDo"]).with_columns(pl.concat_str(pl.col("zkratka"), pl.col("katedra")).alias("identifier"))
    if maly_rozvrh.dtypes[maly_rozvrh.get_column_index("vsichniUciteleUcitIdno")] == pl.String:
        maly_rozvrh = maly_rozvrh.with_columns(pl.col("vsichniUciteleUcitIdno")).explode("vsichniUciteleUcitIdno")
    maly_rozvrh = maly_rozvrh.rename({"vsichniUciteleUcitIdno": "idno"}).filter(pl.col("datumOd").str.len_chars() > 0)

    # Číselník učitelů
    ciselnik_ucitelu = pl.read_csv("source_tables/ciselnik_ucitelu.csv").select("nazev", "key").rename({"nazev":"jmena", "key":"idno"})

    # Osekané předměty
    male_predmety = predmety_by_kat.with_columns(pl.concat_str(pl.col("zkratka"), pl.col("katedra")).alias("identifier")).select(pl.col(["zkratka", "katedra", "identifier", "rok", "nazev", "garantiUcitIdno", "prednasejici", "prednasejiciUcitIdno","cvicici", "cviciciUcitIdno","seminarici", "seminariciUcitIdno"])).unique(subset="identifier").drop("identifier")

    # Zkratky na přístup k sloupcům
    jednotek_prednasek = pl.col("jednotekPrednasek")
    jednotek_cviceni = pl.col("jednotekCviceni")
    jednotek_seminare = pl.col("jednotekSeminare")

    garant = pl.col("garantiUcitIdno")
    cvicici = pl.col("cviciciUcitIdno")
    prednasejici = pl.col("prednasejiciUcitIdno")
    seminarici = pl.col("seminariciUcitIdno")

    # Předměty bez garantů
    zkratky = predmety_s_akci.filter(garant.is_null()).select(["katedra", "zkratka", "identifier", "rok", "nazev", "nazevDlouhy", "garanti", "garantiSPodily"]).filter(
        # Není SZ
        # pl.col("zkratka").str.starts_with("SZ").is_not()
        True
    )
    prep_csv(zkratky).write_csv("results_csv/bez_garanta"+name_mod+".csv", separator=";")
    uac.convert("results_csv/bez_garanta"+name_mod+".csv")

    # Předměty s více garanty
    # If block pokrývá situace kde není žádný předmět s více garanty
    if predmety_s_akci.dtypes[predmety_s_akci.get_column_index("garantiUcitIdno")] == pl.List:
        vice_garantu = predmety_s_akci.with_columns(
            garant.list.len().alias("pocet garantu")
            ).select(
                ["garantiUcitIdno", "garanti","katedra", "zkratka", "identifier", "pocet garantu"]
            ).filter(pl.col("pocet garantu") > 1)
        #vice_garantu.write_excel("results_xlsx/vice_garantu.xlsx")
        prep_csv(vice_garantu).write_csv("results_csv/vice_garantu"+name_mod+".csv", separator=";")
        uac.convert("results_csv/vice_garantu"+name_mod+".csv")
        vice_garantu.head(10)
    
    # Přednášky bez přednášejících
    chybi_prednasejici = has_teacher_theoretical(predmety_s_akci, "prednasejici")

    prep_csv(chybi_prednasejici).write_csv("results_csv/chybi_prednasejici"+name_mod+".csv", separator=";")
    uac.convert("results_csv/chybi_prednasejici"+name_mod+".csv")

    # Cvičení bez cvičicích
    chybi_cvicici = has_teacher_theoretical(predmety_s_akci, "cvicici")

    prep_csv(chybi_cvicici).write_csv("results_csv/chybi_cvicici"+name_mod+".csv", separator=";")
    uac.convert("results_csv/chybi_cvicici"+name_mod+".csv")

    # Semináře bez seminařicích
    chybi_seminarici = has_teacher_theoretical(predmety_s_akci, "seminarici")

    prep_csv(chybi_seminarici).write_csv("results_csv/chybi_seminarici"+name_mod+".csv", separator=";")
    uac.convert("results_csv/chybi_seminarici"+name_mod+".csv")

    # Předměty kde garant neučí
    predmety_kgn = predmety_s_akci.filter((jednotek_cviceni != pl.lit(0)) | (jednotek_prednasek != pl.lit(0)) | (jednotek_seminare != pl.lit(0)))
    if predmety_kgn.dtypes[predmety_kgn.get_column_index("garantiUcitIdno")] == pl.List:
        predmety_kgn = predmety_kgn.explode("garantiUcitIdno")
    predmety_kgn = predmety_kgn.with_columns(
        ((prednasejici.list.contains(garant)) | (cvicici.list.contains(garant)) | (seminarici.list.contains(garant))).alias("containBool")
    )

    aggreg_kgn = (
        predmety_kgn.lazy().group_by("identifier").agg(
            pl.when(pl.col("containBool") == False).then(garant)
        ).with_columns(garant.list.drop_nulls()).filter(garant.list.len() > 0)
    )

    selection = ["identifier","katedra", "zkratka", "prednasejiciUcitIdno", "cviciciUcitIdno", "seminariciUcitIdno", "jednotekPrednasek", "jednotekCviceni", "jednotekSeminare"]
    predmety_kde_garant_neuci = aggreg_kgn.collect().join(predmety_kgn.select(selection), "identifier", "left").drop("identifier")
    prep_csv(predmety_kde_garant_neuci).write_csv("results_csv/predmety_kde_garant_neuci"+name_mod+".csv", separator=";")
    uac.convert("results_csv/predmety_kde_garant_neuci"+name_mod+".csv")

    # Garant nepřednáší
    garant_neprednasi = predmety_s_akci.filter(jednotek_prednasek != pl.lit(0)).explode("garantiUcitIdno").with_columns(
        prednasejici.list.contains(garant).alias("containBool")
    )

    #print(garant_neprednasi.select("containBool", "garantiUcitIdno").head())

    aggregation = (
        garant_neprednasi.lazy().group_by("identifier").agg(
            #pl.col("identifier"),
            pl.when(pl.col("containBool") == False).then(garant)
        ).with_columns(garant.list.drop_nulls()).filter(garant.list.len() > 0)
    )

    garant_neprednasi_post = aggregation.collect().join(garant_neprednasi.select("katedra", "zkratka","prednasejiciUcitIdno", "identifier", "jednotekPrednasek"), "identifier", "left").drop("identifier")

    # garant_neprednasi.write_excel("results_xlsx/garant_neprednasi.xlsx")
    garant_neprednasi_csv = prep_csv(garant_neprednasi_post)
    garant_neprednasi_csv.write_csv("results_csv/garant_neprednasi"+name_mod+".csv", separator=";")
    uac.convert("results_csv/garant_neprednasi"+name_mod+".csv")

    # přednášející nemá přednášku:
    filtrovani_prednasejici = male_predmety.select("nazev", "zkratka", "prednasejiciUcitIdno").explode("prednasejiciUcitIdno")
    prednasejici_jmena = male_predmety.select("prednasejici").rename({"prednasejici":"jmena"}).with_columns(pl.col("jmena").str.strip_chars().str.split("', ")).explode("jmena")
    prednasejici_jmena = prednasejici_jmena.with_columns(pl.col("jmena").str.replace(",", ""))
    filtrovani_prednasejici = filtrovani_prednasejici.with_columns(prednasejici_jmena).filter(
        pl.col("prednasejiciUcitIdno"
    ).is_not_null()).with_columns(
        prednasejici.alias("idno")
    )
    joined_prednasejici = filtrovani_prednasejici.join(maly_rozvrh, "idno", "left")
    prednasejici_bez_prednasek = joined_prednasejici.filter(pl.col("typAkceZkr").is_null())
    prednasejici_bez_prednasek.select("nazev", "zkratka", "prednasejiciUcitIdno", "jmena").sort("prednasejiciUcitIdno").write_csv("results_csv/prednasejici_bez_prednasek"+name_mod+".csv", separator=";")
    uac.convert("results_csv/prednasejici_bez_prednasek"+name_mod+".csv")

    # Přednášející není v seznamu přednášejících ze sylabu
    male_prednasky = maly_rozvrh.filter(pl.col("typAkceZkr") == "Př")
    joined_prednasky = male_prednasky.join(male_predmety.select("zkratka", "katedra", "nazev", "prednasejici", "prednasejiciUcitIdno"), "zkratka", "left").unique()
    prednasky_bez_prednasejicich = joined_prednasky.filter(pl.col("idno").is_in(prednasejici).not_() & ((pl.col("katedra") == pl.col("katedra_right")) | pl.col("katedra_right").is_null()))

    prednasky_bez_prednasejicich = prednasky_bez_prednasejicich.join(ciselnik_ucitelu, "idno", "left").with_columns(prednasejici.cast(pl.List(pl.Utf8)).list.join(", ")).sort("zkratka")

    prednasky_bez_prednasejicich.write_csv("results_csv/prednasky_bez_prednasejicich"+name_mod+".csv", separator=";")
    uac.convert("results_csv/prednasky_bez_prednasejicich"+name_mod+".csv")

    # cvičící nemá cvičení:
    filtrovani_cvicici = male_predmety.select("nazev", "zkratka", "cviciciUcitIdno").explode("cviciciUcitIdno")
    cvicici_jmena = male_predmety.select("cvicici").rename({"cvicici":"jmena"}).with_columns(pl.col("jmena").str.strip_chars().str.split("', ")).explode("jmena")
    cvicici_jmena = cvicici_jmena.with_columns(pl.col("jmena").str.replace(",", ""))
    filtrovani_cvicici = filtrovani_cvicici.with_columns(cvicici_jmena).filter(
        pl.col("cviciciUcitIdno"
    ).is_not_null()).with_columns(
        cvicici.alias("idno")
    )
    joined_cvicici = filtrovani_cvicici.join(maly_rozvrh, "idno", "left")
    cvicici_bez_cviceni = joined_cvicici.filter(pl.col("typAkceZkr").is_null())
    cvicici_bez_cviceni.select("nazev", "zkratka", "cviciciUcitIdno", "jmena").sort("cviciciUcitIdno").write_csv("results_csv/cvicici_bez_cviceni"+name_mod+".csv", separator=";")
    uac.convert("results_csv/cvicici_bez_cviceni"+name_mod+".csv")

    # cvičicí není v sylabu
    male_cviceni = maly_rozvrh.filter(pl.col("typAkceZkr") == "Cv")
    joined_cviceni = male_cviceni.join(male_predmety.select("zkratka", "katedra", "nazev", "cvicici", "cviciciUcitIdno"), "zkratka", "left").unique()
    cviceni_bez_cvicich = joined_cviceni.filter(pl.col("idno").is_in(cvicici).not_() & ((pl.col("katedra") == pl.col("katedra_right")) | pl.col("katedra_right").is_null()))

    cviceni_bez_cvicich = cviceni_bez_cvicich.join(ciselnik_ucitelu, "idno", "left").with_columns(cvicici.cast(pl.List(pl.Utf8)).list.join(", ")).sort("zkratka")

    cviceni_bez_cvicich.write_csv("results_csv/cviceni_bez_cvicich"+name_mod+".csv", separator=";")
    uac.convert("results_csv/cviceni_bez_cvicich"+name_mod+".csv")

    # seminařicí nemá seminář:
    filtrovani_seminarici = male_predmety.select("nazev", "zkratka", "seminariciUcitIdno").explode("seminariciUcitIdno")
    seminarici_jmena = male_predmety.select("seminarici").rename({"seminarici":"jmena"}).with_columns(pl.col("jmena").str.strip_chars().str.split("', ")).explode("jmena")
    seminarici_jmena = seminarici_jmena.with_columns(pl.col("jmena").str.replace(",", ""))
    filtrovani_seminarici = filtrovani_seminarici.with_columns(seminarici_jmena).filter(
        pl.col("seminariciUcitIdno"
    ).is_not_null()).with_columns(
        seminarici.alias("idno")
    )
    joined_seminarici = filtrovani_seminarici.join(maly_rozvrh, "idno", "left")
    seminarici_bez_seminare = joined_seminarici.filter(pl.col("typAkceZkr").is_null())
    seminarici_bez_seminare.select("nazev", "zkratka", "seminariciUcitIdno", "jmena").sort("seminariciUcitIdno").write_csv("results_csv/seminarici_bez_seminare"+name_mod+".csv", separator=";")
    uac.convert("results_csv/seminarici_bez_seminare"+name_mod+".csv")

    # seminařicí není v sylabu:
    male_seminare = maly_rozvrh.filter(pl.col("typAkceZkr") == "Se")
    joined_seminare = male_seminare.join(male_predmety.select("zkratka", "katedra", "nazev", "seminarici", "seminariciUcitIdno"), "zkratka", "left").unique()
    seminare_bez_seminaricich = joined_seminare.filter(pl.col("idno").is_in(seminarici).not_() & ((pl.col("katedra") == pl.col("katedra_right")) | pl.col("katedra_right").is_null()))

    seminare_bez_seminaricich = seminare_bez_seminaricich.join(ciselnik_ucitelu, "idno", "left").with_columns(seminarici.cast(pl.List(pl.Utf8)).list.join(", ")).sort("zkratka")

    seminare_bez_seminaricich.write_csv("results_csv/seminare_bez_seminaricich"+name_mod+".csv", separator=";")
    uac.convert("results_csv/seminare_bez_seminaricich"+name_mod+".csv")

    #...zabijte mne...

if __name__ == "__main__":
    send_the_bomb(
        user_ticket="fac203f83269fde51f5c90c83229ced8a8efe5971ab45fceb610bb5b4c89320a",
        search_type="Katedra",
        search_target="KMA",
        stag_username="F23112"
    )
    print("Done :)")
