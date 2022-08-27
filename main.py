import warnings;warnings.simplefilter('ignore')
import os
import time
import shutil
import json as module_json
from collections import Counter

from pd_options import *


# fonction qui crée le fichier json de résultat
def dag(production=False):
    json = csv_to_json(
        merge_csv(
            load_data()
        )
    )
    ts = int(time.time())

    save_json(json, ts, production)
    move_files(ts)


# fonction qui lit les données depuis les dossiers, merge les fichiers s'ils sont plusieurs puis renvoie une liste de df
def load_data() -> [pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    clinicals_trials_folder = "data/clinical_trials/"
    drugs_folder = "data/drugs/"
    publication_medical_folder = "data/pubmed/"

    clinicals_trials_files = [clinicals_trials_folder + file for file in os.listdir(clinicals_trials_folder)]
    clinicals_trials_files.remove(f"{clinicals_trials_folder}.gitkeep")
    drugs_files = [drugs_folder + file for file in os.listdir(drugs_folder)]
    drugs_files.remove(f"{drugs_folder}.gitkeep")
    publication_medical_files = [publication_medical_folder + file for file in os.listdir(publication_medical_folder)]
    publication_medical_files.remove(f"{publication_medical_folder}.gitkeep")

    # on vérifie que les dossiers contenant les données ne sont pas vides
    assert clinicals_trials_files, exit("le dossier des tests clinique est vide")
    assert drugs_files, exit("le dossier des médicaments est vide")
    assert publication_medical_files, exit("le dossier des publications médicales est vide")

    df_clinicals_trials = pd.DataFrame()
    df_drugs = pd.DataFrame()
    df_publication_medical = pd.DataFrame()

    # on aurait pu aussi partir sans enumerate et faire la distinction en faisant un split du chemin du fichier
    for idx, files in enumerate([clinicals_trials_files, drugs_files, publication_medical_files]):
        for file in files:
            if idx == 0:
                df_clinicals_trials = df_clinicals_trials.append(pd.read_csv(file,encoding="utf8"))
            elif idx == 1:
                df_drugs = df_drugs.append(pd.read_csv(file))
            elif idx == 2:
                # on considère pour simplifier le code qu'il ne peut y avoir que des fichiers jsons venant de ce dossier
                if file.endswith("json"):
                    df_publication_medical = df_publication_medical.append(pd.read_json(file))
                else:
                    df_publication_medical.append(pd.read_csv(file))

    return [df_clinicals_trials, df_drugs, df_publication_medical]


# fonction qui merge les données csv et retourne un csv unique
def merge_csv(df_array) -> pd.DataFrame:
    df_clinicals_trials, df_drugs, df_publication_medical = df_array

    # on aurait pu éviter la déclaratioin de la variable, mais çela aurait rendu le code dur à comprendre
    # on stocke dans une liste toutes les drogues existantes
    drugs = df_drugs["drug"].str.lower().tolist()

    # map = performances
    # on ajoute les drogues citées dans les titres des expériences cliniques
    df_clinicals_trials["drug"] = df_clinicals_trials["scientific_title"].str.lower() \
        .map(lambda x: "".join([drug.upper() if drug in x else "" for drug in drugs]))

    # on ajoute les drogues citées dans les titres des publications médicales
    df_publication_medical["drug"] = df_publication_medical["title"].str.lower() \
        .map(lambda x: "".join([drug.upper() if drug in x else "" for drug in drugs]))

    # on merge les données venant des trois df dans un seul df
    df_merged = pd.merge(
        pd.merge(df_drugs, df_clinicals_trials, on="drug", how="left"),
        pd.merge(df_drugs, df_publication_medical, on="drug", how="left"),
        on="drug", how="left"
    ).rename(columns={"atccode_x": "atccode", "id_x": "id_clinical_trial", "scientific_title": "title_clinical_trial",
                      "date_x": "date_clinical_trial", "journal_x": "journal_clinical_trial",
                      "id_y": "id_medical_publication", "title": "title_medical_publication",
                      "date_y": "date_medical_publication", "journal_y": "journal_medical_publication"}) \
        .drop(columns=["atccode_y"])
    # met toutes les dates au même format
    df_merged["date_medical_publication"] = df_merged["date_medical_publication"].dt.strftime("%-d %B %Y")
    # enlève les caractères non ascii
    df_merged["journal_clinical_trial"] = df_merged["journal_clinical_trial"].astype("string")\
        .replace(to_replace=r"\\xc3\\x28", value="", regex=True)

    df_merged["title_clinical_trial"] = df_merged["title_clinical_trial"].astype("string")\
        .replace(to_replace=r" \\xc3\\xb1", value="", regex=True)

    # on formate le df de telle sorte à faciliter la création de la structure du fichier json
    df_result = pd.DataFrame()
    for column in df_merged.columns.drop(["atccode", "drug"]).tolist():
        df_result[column] = df_merged[column].groupby([df_merged.atccode, df_merged.drug], group_keys=False).apply(list)

    # reset_index() permet de récupérer les colonnes ["atccode", "drug"] utilisées pour le group by
    return df_result.reset_index()


# fonction qui crée un fichier json
def csv_to_json(df) -> list:
    return [
        {
            "atccode": df["atccode"].iloc[i],
            "drug": df["drug"].iloc[i],
            "clinical_trial": [
                {
                    "id_clinical_trial": df["id_clinical_trial"].iloc[i][j],
                    "title_clinical_trial": df["title_clinical_trial"].iloc[i][j],
                    "date_clinical_trial": df["date_clinical_trial"].iloc[i][j],
                    "journal_clinical_trial": df["journal_clinical_trial"].iloc[i][j]
                }
                for j in range(len(df["id_clinical_trial"].iloc[i]))
                if type(df["title_clinical_trial"].iloc[i][0]) is str
            ],
            "medical_publication": [
                {
                    "id_medical_publication": df["id_medical_publication"].iloc[i][j],
                    "title_medical_publication": df["title_medical_publication"].iloc[i][j],
                    "date_medical_publication": df["date_medical_publication"].iloc[i][j],
                    "journal_medical_publication": df["journal_medical_publication"].iloc[i][j]
                }
                for j in range(len(df["id_medical_publication"].iloc[i]))
                if type(df["title_medical_publication"].iloc[i][0]) is str
            ]
        }
        for i in range(len(df))
    ]


# fonction qui enregistre le fichier json
# si production = False, enregistre aussi le json selon un format facilement lisible
def save_json(json, timestamp, production=False):
    with open(f'./output/production/drugs_informations_{timestamp}.json', 'w') as f:
        f.write(module_json.dumps(json))
    # format plus facile à lire
    if not production:
        with open(f'./output/development/drugs_informations_{timestamp}.json', 'w') as f:
            f.write(module_json.dumps(json, indent=4))


# bouge les fichiers d'un dossier à un autre ;Sur le cloud, on aurait migré les fichiers sur un cold bucket
# ça permet de générer à chaque fois des résultats sans prendre en compte ceux d'avant
def move_files(ts):
    folders = os.listdir("data")
    for folder in folders:
        for file in os.listdir(f"data/{folder}"):
            if file != ".gitkeep":
                start, end = file.split(".")
                shutil.move(f"data/{folder}/{file}", f"data_processed/{folder}/{start}_{ts}.{end}")


# inutile en prod, son seul but est de remettre les fichiers dans leur dossier d'origine sans avoir à le faire à la main
def reverse_move_files():
    folders = os.listdir("data_processed")
    for folder in folders:
        for file in os.listdir(f"data_processed/{folder}"):
            if file != ".gitkeep":
                start, end = file.split(".")
                shutil.move(f"data_processed/{folder}/{file}", f"data/{folder}/{'_'.join(start.split('_')[:-1])}.{end}")


# récupère le dernier fichier json puis regarde quel est le journal qui mentionne le plus de médicaments différents
def most_mentioned_journal():
    output_folder = "output/production/"
    files = [output_folder + file for file in os.listdir(output_folder)]
    files.remove(f"{output_folder}.gitkeep")
    # extrait les timestamps des noms des fichiers puis récupère l'index correspondant au plus grand timestamp
    list_ts = list(map(lambda x: int(x.split("_")[-1].split(".")[0]), files))
    idx_last_file = list_ts.index(max(list_ts))
    # charge le fichier
    json = module_json.load(open(files[idx_last_file]))
    # map = performances
    journals = list(map(lambda x: extract_journal(x), json))
    # clean result
    journals = [x for sublist in journals for x in sublist]
    # affiche le journal le plus cité parmi les médicaments
    # "Journal of photochemistry and photobiology. B, Biology" est le seul à être cité dans 2 médicaments différents
    print(list(Counter(journals))[0])


# récupère pour chaque médicament les journaux associés
def extract_journal(json):
    journal_medical = [medical["journal_medical_publication"] for medical in json["medical_publication"]]
    journal_clinical = [clinical["journal_clinical_trial"] for clinical in json["clinical_trial"]]

    return list(set(journal_medical + journal_clinical))


if __name__ == "__main__":
    dag()
    reverse_move_files()
    most_mentioned_journal()
