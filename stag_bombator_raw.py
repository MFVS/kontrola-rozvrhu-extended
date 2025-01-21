# This shit sucks. Remake it.

import polars as pl
from typing import List
import xlsx_generator as tablegen
import utf_ansi_conv as uac
import sbr_prob_isolator as spi

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
    """Function to format schedules."""
    #TODO: Zkontrolovat správnost generace rozvrhů. Viz xlsx_gen
    return fix_str_to_int(rozvrh_source.drop("semestr").rename({"predmet" : "zkratka"}), ["ucitIdno.ucitel", "vsichniUciteleUcitIdno"]).with_columns(pl.concat_str([pl.col("katedra"), pl.col("zkratka")], separator="/").alias("identifier"))

def format_predmety(predmety_source:pl.DataFrame) -> pl.DataFrame:
    """Function to format subjects. Duh."""
    temp_predmety:pl.DataFrame = fix_str_to_int(predmety_source, ["garantiUcitIdno", "prednasejiciUcitIdno", "cviciciUcitIdno", "seminariciUcitIdno", "hodZaSemKombForma", "jednotekPrednasek","jednotekCviceni","jednotekSeminare"])
    unit_fix_cols = [col for index,col in enumerate(temp_predmety.select("jednotekPrednasek", "jednotekCviceni", "jednotekSeminare").columns) if temp_predmety.select("jednotekPrednasek", "jednotekCviceni", "jednotekSeminare").dtypes[index] == pl.List]
    temp_predmety = temp_predmety.with_columns(pl.concat_str([pl.col("katedra"), pl.col("zkratka")], separator="/").alias("identifier"))
    for expl_col in unit_fix_cols:
        temp_predmety = temp_predmety.explode(expl_col)
    return temp_predmety


# --- VYHLEDÁVÁNÍ CHYB ---


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

    rozvrh_by_kat = format_rozvrhy(names["rozvrhy"])
    predmety_by_kat = format_predmety(names["predmety"])

    # Vynechání nevalidních předmětů/rozvrhových akcí (pokud předmět nemá žádné korespondující rozvrhové akce a naopak)
    predmety_s_akci = predmety_by_kat.join(other=rozvrh_by_kat, on="identifier", how="inner").select(predmety_by_kat.columns).unique().sort("identifier")

    # Osekané rozvrhové akce
    maly_rozvrh = rozvrh_by_kat.select(["katedra","zkratka","identifier", "vsichniUciteleUcitIdno", "typAkceZkr", "rok", "datumOd", "datumDo", "hodinaSkutOd", "hodinaSkutDo"])#.with_columns(pl.concat_str(pl.col("zkratka"), pl.col("katedra")).alias("identifier"))
    if maly_rozvrh.dtypes[maly_rozvrh.get_column_index("vsichniUciteleUcitIdno")] == pl.List:
        maly_rozvrh = maly_rozvrh.with_columns(pl.col("vsichniUciteleUcitIdno")).explode("vsichniUciteleUcitIdno")
    maly_rozvrh = maly_rozvrh.rename({"vsichniUciteleUcitIdno": "idno"})#.filter(pl.col("datumOd").str.len_chars() > 0) #TODO: Tohle možná dělalo problémy; filtrovalo to prázdný rozvrhový akce

    # Číselník učitelů
    ciselnik_ucitelu = pl.read_csv("source_tables/ciselnik_ucitelu.csv").select("nazev", "key").rename({"nazev":"jmena", "key":"idno"})

    # Osekané předměty
    male_predmety = predmety_by_kat.with_columns(pl.concat_str(pl.col("katedra"), pl.col("zkratka"), separator="/").alias("identifier")).select(pl.col(["zkratka", "katedra", "identifier", "rok", "nazev", "garantiUcitIdno", "prednasejici", "prednasejiciUcitIdno","cvicici", "cviciciUcitIdno","seminarici", "seminariciUcitIdno"])).unique(subset="identifier")

    # Předměty bez garantů
    # TODO: Zkontrolovat zda toto reálně funguje, závisí to na funkcionalitě fix_str_to_int funkce
    zkratky = predmety_s_akci.filter(garant.is_null()).select(["katedra", "zkratka", "identifier", "rok", "nazev", "nazevDlouhy", "garanti", "garantiSPodily"])
    save_df_to_file(zkratky, ".\\results_csv\\bez_garanta"+name_mod, file_format)

    save_df_to_file(spi.vice_garantu_func(predmety_s_akci), f".\\results_csv\\vice_garantu{name_mod}", file_format)
    
    # TODO: Otestovat 
    def all_has_teacher_theoretical():
        """Handles using all the fields on the has_teacher_theoretical function."""
        fields = ["prednasejici", "cvicici", "seminarici"]
        for field in fields:
            cur_res = spi.has_teacher_theoretical(predmety_s_akci, field)
            save_df_to_file(cur_res, f".\\results_csv\\chybi_{field}{name_mod}", file_format)

    all_has_teacher_theoretical()

    predmety_kde_garant_neuci = spi.garant_doesnt_teach(predmety_s_akci)
    save_df_to_file(predmety_kde_garant_neuci, ".\\results_csv\\predmety_kde_garant_neuci"+name_mod, file_format)

    garant_neprednasi_csv = prep_csv(spi.garant_doesnt_lecture(predmety_s_akci))
    save_df_to_file(garant_neprednasi_csv, ".\\results_csv\\garant_neprednasi"+name_mod, file_format)

    #TODO: Nová varianta funkce, nutno pořádně otestovat. Mělo by teď snad fungovat líp
    def all_no_scheduled_events():
        fields = [("prednasejici", "prednasejici_bez_prednasek"), ("cvicici", "cvicici_bez_cviceni"), ("seminarici", "seminarici_bez_seminare")]
        for field in fields:
            cur_res = spi.no_scheduled_events(maly_rozvrh, male_predmety, ciselnik_ucitelu, field[0])
            save_df_to_file(cur_res, f".\\results_csv\\{field[1]}{name_mod}", file_format) 

    all_no_scheduled_events()

    def all_not_in_sylabus():
        fields = [("prednasejici", "Př", "prednasky_bez_prednasejicich"), ("cvicici", "Cv", "cviceni_bez_cvicich"), ("seminarici", "Se", "seminare_bez_seminaricich")]
        for field in fields:
            cur_res = spi.not_in_sylabus(maly_rozvrh, male_predmety, ciselnik_ucitelu, sought_field=field[0], abbriviation=field[1])
            save_df_to_file(cur_res, f".\\results_csv\\{field[2]}{name_mod}", file_format)

    all_not_in_sylabus()

if __name__ == "__main__":
    send_the_bomb(
        user_ticket="fac203f83269fde51f5c90c83229ced8a8efe5971ab45fceb610bb5b4c89320a",
        search_type="Katedra",
        search_target="KMA",
        stag_username="F23112"
    )
    print("Done :)")
