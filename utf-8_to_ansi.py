def convert(file_path:str):
    data = ""
    with open(file_path, "r", encoding="utf8") as preconvert:
        data = preconvert.read()
    with open(file_path, "w", encoding="ansi") as postconvert:
        postconvert.write(data)

if __name__ == "__main__":
    convert("results_csv/bez_garanta_F23112.csv")