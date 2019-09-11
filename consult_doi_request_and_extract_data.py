"""
Script para consultar se os DOIS dados ainda estao apresentando erro 404
"""
import sys
import argparse
import pandas as pd
import pandas.io.sql as sqlio
import psycopg2


def main(sargs):

    parser = argparse.ArgumentParser(description="")
    parser.add_argument(
        "--host", required=True, help="""Host to connect to Postgres DB"""
    )
    parser.add_argument(
        "--port", required=True, help="""Port to connect to Postgres DB"""
    )
    parser.add_argument("--user", required=True, help="User to connect")
    parser.add_argument("--password", required=True, help="Password to connect")
    parser.add_argument("--database", help="Database to connect")

    args = parser.parse_args(sargs)
    df_doi_not_found = pd.read_csv("./df_doi_not_found.csv")
    connection = psycopg2.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database,
    )

    SQL_DOI_PROCESSED = """
    SELECT
        journal, pid, doi, submission_status, coalesce(feedback_status, 'semValor') as feedback_status, feedback_xml
    FROM deposit
    WHERE code IN ( {0} )""".format(
        ", ".join(
            [
                "'{0}_{1}'".format(c, p)
                for c, p in zip(
                    df_doi_not_found.collection.to_list(),
                    df_doi_not_found.pid.to_list(),
                )
            ]
        )
    )
    df_doi_processed = sqlio.read_sql_query(SQL_DOI_PROCESSED, connection)

    SQL_ALL_DOI_PROCESSED = """ SELECT pid FROM deposit WHERE prefix LIKE '10.15%' """
    df_all_processed = sqlio.read_sql_query(SQL_ALL_DOI_PROCESSED, connection)

    pids = df_all_processed.pid.to_list()
    df_doi_not_processed = df_doi_not_found.query("pid not in @pids")

    # Salvado arquivos para consultas futuras
    print("Gerando arquivo df_doi_not_processed.csv")
    df_doi_not_processed.to_csv("./df_doi_not_processed.csv")

    print("Gerando arquivo df_doi_processed.csv")
    df_doi_processed.to_csv("./df_doi_processed.csv")

if __name__ == "__main__":
    main(sys.argv[1:])
