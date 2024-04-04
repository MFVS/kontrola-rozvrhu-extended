def fetch_csv(service:str = "",ticket:str = "", params_plus:dict = {}, manual_login:tuple = None): # manual_login formát: (jméno, heslo)
    import requests
    from io import StringIO
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
def login_correction(manual_login:tuple): #Testování, zda tuple má dva prvky a ty prvky zda jsou stringy

    if manual_login == None:
        return manual_login

    try:
        assert isinstance(manual_login[0], str)
        assert isinstance(manual_login[1], str)
    except:
        auth = None
    else:
        auth = (manual_login[0], manual_login[1])
    
    return auth

def main():
    import polars as pl

    # Testovací data
    # DATA REDIGOVÁNA (nebudu se doxovat)
    params_rozvrh = {
        "stagUser": "F23112",
        "semestr":"%",
        "vsechnyCasyKonani":"true",
        "jenRozvrhoveAkce":"false",
        "vsechnyAkce":"false",
        "jenBudouciAkce":"true",
        "lang":"cs",
        "katedra":"KI",
        "rok":"2023"
    }
    params_predmety = {
        "lang":"cs",
        "katedra":"KI",
        "jenNabizeneECTSPrijezdy":"false",
        "rok":"2023"
    }

    ticket = "30088f13cc4a64c91aef019587bf2a31f7ff7055306e11abaef001d927dd099a"
    auth = ("st101885", "x0301093100")

    # Side note: Obecně čtení v pythonu se silně nelíbí když neexistujicí složky kam maj chodit...
    # Excel rozvrhy funguje jen s validním přihlášením
    excel_rozvrhy = pl.read_csv(fetch_csv(service="/rozvrhy/getRozvrhByKatedra", params_plus=params_rozvrh, ticket=ticket, manual_login=auth), separator=";")
    excel_rozvrhy.write_excel("source_tables/getRozvrhByKatedra-2023-03-15-17-47.xlsx")

    # Excel předměty funguje i bez přihlášení
    excel_predmety = pl.read_csv(fetch_csv(service="/predmety/getPredmetyByKatedraFullInfo", params_plus=params_predmety), separator=";")
    excel_predmety.write_excel("source_tables/getPredmetyByKatedraFullInfo-2023-03-15-17-40.xlsx")

if __name__ == '__main__':
    main()

