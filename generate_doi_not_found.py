"""
Script para consultar se os DOIS dados ainda estao apresentando erro 404
"""

import pandas as pd
import requests
from tqdm import tqdm
from requests.exceptions import HTTPError


def main():

    doi_files = pd.read_csv("./SciELO_Brazil_DOI.csv", delimiter=";", low_memory=False)
    doi_not_found = []

    for index, row in tqdm(doi_files.iterrows(), total=len(doi_files)):
        doi = row["doi"]
        try:
            r = requests.get(f"https://www.doi.org/doi/{doi}")
            r.raise_for_status()
        except HTTPError as exc:
            row["request"] = str(exc)
            if r.status_code == 404:
                doi_not_found.append(row)

    df_doi_not_found = pd.DataFrame(doi_not_found)
    # Salvado arquivos para consultas futuras
    df_doi_not_found.to_csv("./df_doi_not_found.csv")


if __name__ == "__main__":
    main()
