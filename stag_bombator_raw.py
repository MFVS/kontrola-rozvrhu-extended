# This shit sucks. Remake it.

import polars as pl
from typing import List
import xlsx_generator as tablegen
import utf_ansi_conv as uac

# Zkratky na přístup k sloupcům
jednotek_prednasek = pl.col("jednotekPrednasek")
jednotek_cviceni = pl.col("jednotekCviceni")
jednotek_seminare = pl.col("jednotekSeminare")

garant = pl.col("garantiUcitIdno")
cvicici = pl.col("cviciciUcitIdno")
prednasejici = pl.col("prednasejiciUcitIdno")
seminarici = pl.col("seminariciUcitIdno")

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

    # TODO: Smazat pokud vše funguje dobře. Zkontrolovat že tohle dole se fakt nehodí. Jsem moc línej.
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

def save_df_to_file(dataframe:pl.DataFrame, path:str, file_format:str) -> None:
    """Function to simplify saving.

    Args:
        dataframe (pl.DataFrame): Dataframe to save.
        path (str): Path to file in which to save. If the path does not contain the file format at the end, it is pasted there.
        file_format (str): Format in which to save. CSV or XLSX. Exclude the comma. (The function can handle it tho)

    Raises:
        ValueError: If file_format is not either CSV or XLSX, an error is raised, as these are the only two viable save formats.
    """
    #raise NotImplementedError("Hopefully better.")
    file_format = file_format.lower().strip(".")

    if file_format == "xlsx":
        prep_csv(dataframe=dataframe).write_excel(path if ".xlsx" == path[-5:] else f"{path}.xlsx")
        return
    elif file_format == "csv":
        path = path if ".csv" == path[-4:] else f"{path}.csv"
        prep_csv(dataframe=dataframe).write_csv(path, separator=";")
        uac.convert(path)
        return
    else:
        raise ValueError("Undefined file saving format.")
    
def format_rozvrhy(rozvrh_source:pl.DataFrame) -> pl.DataFrame:
    """Function to format scheduled events."""
    #TODO: Zkontrolovat správnost generace rozvrhů. Viz xlsx_gen
    return fix_str_to_int(rozvrh_source.drop("semestr").rename({"predmet" : "zkratka"}), ["ucitIdno.ucitel", "vsichniUciteleUcitIdno"]).with_columns(pl.concat_str([pl.col("katedra"), pl.col("zkratka")], separator="/").alias("identifier"))

def format_predmety(predmety_source:pl.DataFrame) -> pl.DataFrame:
    temp_predmety:pl.DataFrame = fix_str_to_int(predmety_source)
    unit_fix_cols = [col for index,col in enumerate(temp_predmety.select("jednotekPrednasek", "jednotekCviceni", "jednotekSeminare").columns) if temp_predmety.select("jednotekPrednasek", "jednotekCviceni", "jednotekSeminare").dtypes[index] == pl.List]
    temp_predmety = temp_predmety.with_columns(pl.concat_str([pl.col("katedra"), pl.col("zkratka")], separator="/").alias("identifier"))
    for expl_col in unit_fix_cols:
        temp_predmety = temp_predmety.explode(expl_col)
    return temp_predmety


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
def send_the_bomb(search_type:str, search_target:str, stag_username:str, user_ticket:str, year:int, lang:str, file_format:str="csv") -> None:
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

    # TODO: Zkontrolovat, zda nahrazení tohohle bloku modifikovatelnejma funkcema nebude dělat problémy a následně smazat.
    # Zpracování CSV souborů na DataFrames
    # print(names["rozvrhy"].head())
    # rozvrh_by_kat = names["rozvrhy"].drop("semestr").rename({"predmet" : "zkratka"}) # Zde smazán unique, rip
    # rozvrh_by_kat = fix_str_to_int(rozvrh_by_kat, ["ucitIdno.ucitel", "vsichniUciteleUcitIdno"])
    # rozvrh_by_kat = rozvrh_by_kat.with_columns(pl.concat_str([pl.col("katedra"), pl.col("zkratka")], separator="/").alias("identifier"))

    # predmety_by_kat = names["predmety"]
    # predmety_by_kat = fix_str_to_int(predmety_by_kat, ["garantiUcitIdno", "prednasejiciUcitIdno", "cviciciUcitIdno", "seminariciUcitIdno", "hodZaSemKombForma", "jednotekPrednasek","jednotekCviceni","jednotekSeminare"])
    # unit_fix_cols = [col for index,col in enumerate(predmety_by_kat.select("jednotekPrednasek", "jednotekCviceni", "jednotekSeminare").columns) if predmety_by_kat.select("jednotekPrednasek", "jednotekCviceni", "jednotekSeminare").dtypes[index] == pl.List]
    # predmety_by_kat = predmety_by_kat.with_columns(pl.concat_str([pl.col("katedra"), pl.col("zkratka")], separator="/").alias("identifier"))
    # for expl_col in unit_fix_cols:
    #     predmety_by_kat = predmety_by_kat.explode(expl_col)

    rozvrh_by_kat = format_rozvrhy(names["rozvrhy"])
    predmety_by_kat = format_predmety(names["predmety"])

    # Vynechání nevalidních předmětů/rozvrhových akcí (pokud předmět nemá žádné korespondující rozvrhové akce a naopak)
    predmety_s_akci = predmety_by_kat.join(other=rozvrh_by_kat, on="identifier", how="inner").select(predmety_by_kat.columns).unique().sort("identifier")

    # Osekané rozvrhové akce
    maly_rozvrh = rozvrh_by_kat.select(["katedra","zkratka", "vsichniUciteleUcitIdno", "typAkceZkr", "rok", "datumOd", "datumDo", "hodinaSkutOd", "hodinaSkutDo"]).with_columns(pl.concat_str(pl.col("zkratka"), pl.col("katedra")).alias("identifier"))
    if maly_rozvrh.dtypes[maly_rozvrh.get_column_index("vsichniUciteleUcitIdno")] == pl.List:
        maly_rozvrh = maly_rozvrh.with_columns(pl.col("vsichniUciteleUcitIdno")).explode("vsichniUciteleUcitIdno")
    maly_rozvrh = maly_rozvrh.rename({"vsichniUciteleUcitIdno": "idno"})#.filter(pl.col("datumOd").str.len_chars() > 0) #TODO: Tohle možná dělalo problémy; filtrovalo to prázdný rozvrhový akce

    # Číselník učitelů
    ciselnik_ucitelu = pl.read_csv("source_tables/ciselnik_ucitelu.csv").select("nazev", "key").rename({"nazev":"jmena", "key":"idno"})

    # Osekané předměty
    male_predmety = predmety_by_kat.with_columns(pl.concat_str(pl.col("zkratka"), pl.col("katedra")).alias("identifier")).select(pl.col(["zkratka", "katedra", "identifier", "rok", "nazev", "garantiUcitIdno", "prednasejici", "prednasejiciUcitIdno","cvicici", "cviciciUcitIdno","seminarici", "seminariciUcitIdno"])).unique(subset="identifier").drop("identifier")

    # Předměty bez garantů
    zkratky = predmety_s_akci.filter(garant.is_null()).select(["katedra", "zkratka", "identifier", "rok", "nazev", "nazevDlouhy", "garanti", "garantiSPodily"]).filter(
        # Není SZ
        # pl.col("zkratka").str.starts_with("SZ").is_not()
        True
    )
    # prep_csv(zkratky).write_csv(".\\results_csv\\bez_garanta"+name_mod+".csv", separator=";")
    # uac.convert(".\\results_csv\\bez_garanta"+name_mod+".csv")
    save_df_to_file(zkratky, ".\\results_csv\\bez_garanta"+name_mod, file_format)


    # Předměty s více garanty
    # If block pokrývá situace kde není žádný předmět s více garanty
    # TODO: This fuckin sucks
    if predmety_s_akci.dtypes[predmety_s_akci.get_column_index("garantiUcitIdno")] == pl.List:
        vice_garantu = predmety_s_akci.with_columns(
            garant.list.len().alias("pocet garantu")
            ).select(
                ["garantiUcitIdno", "garanti","katedra", "zkratka", "identifier", "pocet garantu"]
            ).filter(pl.col("pocet garantu") > 1)
        prep_csv(vice_garantu).write_csv(".\\results_csv\\vice_garantu"+name_mod+".csv", separator=";")
        uac.convert(".\\results_csv\\vice_garantu"+name_mod+".csv")
        vice_garantu.head(10)
    
    # TODO: Otestovat 
    def all_has_teacher_theoretical():
        fields = ["prednasejici", "cvicici", "seminarici"]
        for field in fields:
            cur_res = has_teacher_theoretical(predmety_s_akci, field)
            save_df_to_file(cur_res, f".\\results_csv\\chybi_{field}{name_mod}", file_format)

    all_has_teacher_theoretical()

    # chybi_prednasejici = has_teacher_theoretical(predmety_s_akci, "prednasejici")

    # prep_csv(chybi_prednasejici).write_csv(".\\results_csv\\chybi_prednasejici"+name_mod+".csv", separator=";")
    # uac.convert(".\\results_csv\\chybi_prednasejici"+name_mod+".csv")

    # # Cvičení bez cvičicích
    # chybi_cvicici = has_teacher_theoretical(predmety_s_akci, "cvicici")

    # prep_csv(chybi_cvicici).write_csv(".\\results_csv\\chybi_cvicici"+name_mod+".csv", separator=";")
    # uac.convert(".\\results_csv\\chybi_cvicici"+name_mod+".csv")

    # # Semináře bez seminařicích
    # chybi_seminarici = has_teacher_theoretical(predmety_s_akci, "seminarici")

    # prep_csv(chybi_seminarici).write_csv(".\\results_csv\\chybi_seminarici"+name_mod+".csv", separator=";")
    # uac.convert(".\\results_csv\\chybi_seminarici"+name_mod+".csv")

    # Předměty kde garant neučí
    def garant_doesnt_teach():
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
        return aggreg_kgn.collect().join(predmety_kgn.select(selection), "identifier", "left").drop("identifier")
    
    predmety_kde_garant_neuci = garant_doesnt_teach()
    # prep_csv(predmety_kde_garant_neuci).write_csv(".\\results_csv\\predmety_kde_garant_neuci"+name_mod+".csv", separator=";")
    # uac.convert(".\\results_csv\\predmety_kde_garant_neuci"+name_mod+".csv")
    save_df_to_file(predmety_kde_garant_neuci, ".\\results_csv\\predmety_kde_garant_neuci"+name_mod, file_format)

    # Garant nepřednáší
    def garant_doesnt_lecture():
        garant_neprednasi = predmety_s_akci.filter(jednotek_prednasek != pl.lit(0))
        if garant_neprednasi.dtypes[garant_neprednasi.get_column_index("garantiUcitIdno")] != pl.Int64:
            garant_neprednasi = garant_neprednasi.explode("garantiUcitIdno")
        garant_neprednasi = garant_neprednasi.with_columns(
            prednasejici.list.contains(garant).alias("containBool")
        )

        aggregation = (
            garant_neprednasi.lazy().group_by("identifier").agg(
                #pl.col("identifier"),
                pl.when(pl.col("containBool") == False).then(garant)
            ).with_columns(garant.list.drop_nulls()).filter(garant.list.len() > 0)
        )

        return aggregation.collect().join(garant_neprednasi.select("katedra", "zkratka","prednasejiciUcitIdno", "identifier", "jednotekPrednasek"), "identifier", "left").drop("identifier")

    garant_neprednasi_csv = prep_csv(garant_doesnt_lecture())
    # garant_neprednasi_csv.write_csv(".\\results_csv\\garant_neprednasi"+name_mod+".csv", separator=";")
    # uac.convert(".\\results_csv\\garant_neprednasi"+name_mod+".csv")
    save_df_to_file(garant_neprednasi_csv, ".\\results_csv\\garant_neprednasi"+name_mod, file_format)

    #TODO: Nefunguje. Prioritní cíl.
    def all_no_scheduled_events():
        def no_scheduled_events(sought_field:str):
            st_id = sought_field + "UcitIdno"
            filtrovani_prednasejici = male_predmety.select("nazev", "katedra", "zkratka", st_id).explode(st_id)
            prednasejici_jmena = male_predmety.select(sought_field).rename({sought_field:"jmena"}).with_columns(pl.col("jmena").str.strip_chars().str.split("', ")).explode("jmena")
            prednasejici_jmena = prednasejici_jmena.with_columns(pl.col("jmena").str.replace(",", ""))
            filtrovani_prednasejici = filtrovani_prednasejici.with_columns(prednasejici_jmena).filter(
                pl.col(st_id
            ).is_not_null()).with_columns(
                pl.col(st_id).alias("idno")
            )

            joined_prednasejici = filtrovani_prednasejici.join(maly_rozvrh, "idno", "left")
            prednasejici_bez_prednasek = joined_prednasejici.filter(pl.col("typAkceZkr").is_null())
            return prednasejici_bez_prednasek.select("katedra", "zkratka", "nazev", st_id, "jmena").sort(st_id)
        
        fields = [("prednasejici", "prednasejici_bez_prednasek"), ("cvicici", "cvicici_bez_cviceni"), ("seminarici", "seminarici_bez_seminare")]
        for field in fields:
            cur_res = no_scheduled_events(predmety_s_akci, field)
            save_df_to_file(cur_res, f".\\results_csv\\{field}{name_mod}", file_format)
        
        prednasky = no_scheduled_events("prednasejici")
        prep_csv(prednasky).write_csv(".\\results_csv\\prednasejici_bez_prednasek"+name_mod+".csv", separator=";")
        uac.convert(".\\results_csv\\prednasejici_bez_prednasek"+name_mod+".csv")

        cviceni = no_scheduled_events("cvicici")
        prep_csv(cviceni).write_csv(".\\results_csv\\cvicici_bez_cviceni"+name_mod+".csv", separator=";")
        uac.convert(".\\results_csv\\cvicici_bez_cviceni"+name_mod+".csv")

        seminare = no_scheduled_events("seminarici")
        prep_csv(seminare).write_csv(".\\results_csv\\seminarici_bez_seminare"+name_mod+".csv", separator=";")
        uac.convert(".\\results_csv\\seminarici_bez_seminare"+name_mod+".csv")

    all_no_scheduled_events()

    def all_not_in_sylabus():
        def not_in_sylabus(sought_field:str, abbriviation:str):
            st_id = sought_field + "UcitIdno"
            male_prednasky = maly_rozvrh.filter(pl.col("typAkceZkr") == abbriviation)
            joined_prednasky = male_prednasky.join(male_predmety.select("zkratka", "katedra", "nazev", sought_field, st_id), "zkratka", "left").unique()
            prednasky_bez_prednasejicich = joined_prednasky.filter(pl.col("idno").is_in(pl.col(st_id)).not_() & ((pl.col("katedra") == pl.col("katedra_right")) | pl.col("katedra_right").is_null()))

            return prednasky_bez_prednasejicich.join(ciselnik_ucitelu, "idno", "left").with_columns(pl.col(st_id).cast(pl.List(pl.Utf8)).list.join(", ")).sort("zkratka")
        
        fields = [("prednasejici", "Př", "prednasky_bez_prednasejici"), ("cvicici", "Cv", "cviceni_bez_cvicich"), ("seminarici", "Se", "seminare_bez_seminaricich")]
        for field in fields:
            cur_res = not_in_sylabus(sought_field=field[0], abbriviation=field[1])
            save_df_to_file(cur_res, f".\\results_csv\\{field[2]}{name_mod}", file_format)
        
        # prednasky = not_in_sylabus(sought_field="prednasejici", abbriviation="Př")
        # prep_csv(prednasky).write_csv(".\\results_csv\\prednasky_bez_prednasejicich"+name_mod+".csv", separator=";")
        # uac.convert(".\\results_csv\\prednasky_bez_prednasejicich"+name_mod+".csv")

        # cviceni = not_in_sylabus(sought_field="cvicici", abbriviation="Cv")
        # prep_csv(cviceni).write_csv(".\\results_csv\\cviceni_bez_cvicich"+name_mod+".csv", separator=";")
        # uac.convert(".\\results_csv\\cviceni_bez_cvicich"+name_mod+".csv")

        # seminare = not_in_sylabus(sought_field="seminarici", abbriviation="Se")
        # prep_csv(seminare).write_csv(".\\results_csv\\seminare_bez_seminaricich"+name_mod+".csv", separator=";")
        # uac.convert(".\\results_csv\\seminare_bez_seminaricich"+name_mod+".csv")

    all_not_in_sylabus()

if __name__ == "__main__":
    send_the_bomb(
        user_ticket="fac203f83269fde51f5c90c83229ced8a8efe5971ab45fceb610bb5b4c89320a",
        search_type="Katedra",
        search_target="KMA",
        stag_username="F23112"
    )
    print("Done :)")
