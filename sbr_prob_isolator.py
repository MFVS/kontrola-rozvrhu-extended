#Tento skript obsahuje funkce na nalezení chyb pro stag_bombator_raw
import polars as pl

jednotek_prednasek = pl.col("jednotekPrednasek")
jednotek_cviceni = pl.col("jednotekCviceni")
jednotek_seminare = pl.col("jednotekSeminare")

garant = pl.col("garantiUcitIdno")
cvicici = pl.col("cviciciUcitIdno")
prednasejici = pl.col("prednasejiciUcitIdno")
seminarici = pl.col("seminariciUcitIdno")

# Předměty s více garanty
# If block pokrývá situace kde není žádný předmět s více garanty
# TODO: 1) Nahrazení výběru seznamem, to by mělo fungovat though 2) This fuckin stinks
def vice_garantu_func(predmety_s_akci:pl.DataFrame):
        """Function to find all rows where there are multiple garants."""
        final_selection = ["katedra", "zkratka","garantiUcitIdno", "garanti", "pocet garantu"]
        if predmety_s_akci.dtypes[predmety_s_akci.get_column_index("garantiUcitIdno")] == pl.List:
            vice_garantu = predmety_s_akci.with_columns(
                garant.list.len().alias("pocet garantu")
                ).select(
                    final_selection
                ).filter(pl.col("pocet garantu") > 1)
            return vice_garantu
        else:
            return pl.DataFrame({one:"" for one in final_selection})

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

# Předměty kde garant neučí
def garant_doesnt_teach(predmety_s_akci:pl.DataFrame):
  should_cviceni = (jednotek_cviceni != pl.lit(0)) & (pl.col("jednotkaCviceni") == "HOD/TYD")
  should_prednasek = (jednotek_prednasek != pl.lit(0)) & (pl.col("jednotkaPrednasky") == "HOD/TYD")
  should_seminare = (jednotek_seminare != pl.lit(0)) & (pl.col("jednotkaSeminare") == "HOD/TYD")

  predmety_kgn = predmety_s_akci.filter(should_cviceni | should_prednasek | should_seminare)
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

  selection = ["identifier","katedra", "zkratka", "prednasejiciUcitIdno", "cviciciUcitIdno", "seminariciUcitIdno", "jednotekPrednasek", "jednotkaPrednasky", "jednotekCviceni", "jednotkaCviceni", "jednotekSeminare", "jednotkaSeminare"]
  return aggreg_kgn.collect().join(predmety_kgn.select(selection), "identifier", "left").drop("identifier")

# Garant nepřednáší
def garant_doesnt_lecture(predmety_s_akci:pl.DataFrame):
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


def no_scheduled_events(maly_rozvrh:pl.DataFrame, male_predmety:pl.DataFrame, ciselnik_ucitelu:pl.DataFrame, sought_field:str):
  variant = f"{sought_field}UcitIdno"
  period_trans = {
      "prednasejici":"Př",
      "cvicici":"Cv",
      "seminarici":"Se"
  }

  # Nový způsob:
  # - Vyfiltruj dané akce (např. cvičení), seskup podle identifieru ({katedra}/{zkratka}) a agreguj podle idčka
  # - Spoj tabulky pomocí left joinu (elementy z pravé tabulky jsou nalepeny na levou, ponechány i řádky na které nebylo nic dáno) přes identifier, nech pouze řádky které nejsou identické se sylabem
  # - Exploduj idčka ze sylabu a nech všechna která nejsou v agregovaných id z rozvrhu 
  small_sf_events = maly_rozvrh.filter(pl.col("typAkceZkr") == period_trans[sought_field]).lazy().group_by(pl.col("identifier")).agg(pl.col("idno")).collect().select("identifier", pl.col("idno").list.unique())
  #print(small_sf_events.head())
  #print(male_predmety.columns)
  discordant = male_predmety.join(small_sf_events, "identifier", "left")
  print(discordant.head())
  discordant = discordant.filter(pl.col("idno") != pl.col(variant)).with_columns(pl.col(variant).alias("uciteleBezHodin"))
  print(discordant.head())
  discordant = discordant.explode("uciteleBezHodin").filter(pl.col("uciteleBezHodin").is_in(pl.col("idno")).not_()).join(ciselnik_ucitelu.rename({"idno":"uciteleBezHodin"}), "uciteleBezHodin", how="left")
  print(discordant.head())
  return discordant.select("katedra", "zkratka", sought_field, variant, "idno", "uciteleBezHodin", "jmena")

def not_in_sylabus(maly_rozvrh:pl.DataFrame, male_predmety:pl.DataFrame, ciselnik_ucitelu:pl.DataFrame, sought_field:str, abbriviation:str):
  """Searches through scheduled events to finds all teacher who are not in syllabus."""
  st_id = sought_field + "UcitIdno"
  male_prednasky = maly_rozvrh.filter(pl.col("typAkceZkr") == abbriviation)
  joined_prednasky = male_prednasky.join(male_predmety.select("zkratka", "katedra", "nazev", sought_field, st_id), "zkratka", "left").unique()
  prednasky_bez_prednasejicich = joined_prednasky.filter(pl.col("idno").is_in(pl.col(st_id)).not_() & ((pl.col("katedra") == pl.col("katedra_right")) | pl.col("katedra_right").is_null()))

  return prednasky_bez_prednasejicich.join(ciselnik_ucitelu, "idno", "left").with_columns(pl.col(st_id).cast(pl.List(pl.Utf8)).list.join(", ")).sort("zkratka")