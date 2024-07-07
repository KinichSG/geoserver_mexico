import psycopg2
from psycopg2 import sql
import pathlib
import geopandas as gpd

host = 'localhost'
dbname = 'seguridad_vial'
user = 'seguridad_vial'
port = 5432
password = 'popcorning'
schema_name = 'marco_geoestadistico_22'
dir_vmrc_anual_csv = 'mg2022_integrado'

def create_schema(host, dbname, user, port, password, schema_name):
    query = sql.SQL(
        """
        CREATE SCHEMA IF NOT EXISTS {0};
        """
    ).format(sql.Identifier(schema_name))

    with psycopg2.connect(host=host, dbname=dbname, user=user, port=port, password=password, options=f'-c search_path={schema_name}') as conn:
        with conn.cursor() as cur:
            cur.execute(query)

def create_AGE_Estatales(host, dbname, user, port, password, schema_name):
    query = sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS agee(
            "CVEGEO" varchar(2),
            "CVE_ENT" varchar(2),
            "NOMGEO" varchar(150),
            PRIMARY KEY("CVEGEO")
        );
        """
    )

    with psycopg2.connect(host=host, dbname=dbname, user=user, port=port, password=password, options=f'-c search_path={schema_name}') as conn:
        with conn.cursor() as cur:
            cur.execute(query)

def load_AGE_Estatales(host, dbname, user, port, password, schema_name):
    copy = """
        COPY agee("CVEGEO", "CVE_ENT", "NOMGEO")
        FROM STDIN
        DELIMITER ','
        HEADER CSV;
        """

    with psycopg2.connect(host=host, dbname=dbname, user=user, port=port, password=password, options=f'-c search_path={schema_name}') as conn:
        with conn.cursor() as cur_copy:
            with open()
            cur_copy.copy_expert(sql=copy)

def create_AGE_Municipales(host, dbname, user, port, password, schema_name):
    query = sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS agem(
            "CVEGEO" varchar(5),
            "CVE_ENT" varchar(2),
            "CVE_MUN" varchar(3),
            "NOMGEO" varchar(150),
            PRIMARY KEY("CVEGEO"),
            FOREIGN KEY("CVE_ENT") REFERENCES agee("CVEGEO")
        );
        """
    )

    with psycopg2.connect(host=host, dbname=dbname, user=user, port=port, password=password, options=f'-c search_path={schema_name}') as conn:
        with conn.cursor() as cur:
            cur.execute(query)

def create_AGE_Localidad(host, dbname, user, port, password, schema_name):
    query = sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS loc(
            "CVEGEO" varchar(9),
            "CVE_ENT" varchar(2),
            "CVE_MUN" varchar(3),
            "CVE_LOC" varchar(4),
            "NOM_GEO" varchar(150),
            "AMBITO" varchar(6),
            PRIMARY KEY("CVEGEO"),
            FOREIGN KEY("CVE_ENT") REFERENCES agee("CVEGEO"),
            FOREIGN KEY("CVE_ENT", "CVE_MUN") REFERENCES agem("CVE_ENT", "CVE_MUN")
        );"""
    )

    with psycopg2.connect(host=host, dbname=dbname, user=user, port=port, password=password, options=f'-c search_path={schema_name}') as conn:
        with conn.cursor() as cur:
            cur.execute(query)

def create_AGE_Basicas(host, dbname, user, port, password, schema_name):
    query = sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS ageb(
            "CVEGEO" varchar(13),
            "CVE_ENT" varchar(2),
            "CVE_MUN" varchar(3),
            "CVE_LOC" varchar(4),
            "CVE_AGEB" varchar(4),
            "AMBITO" varchar(6),
            PRIMARY KEY("CVEGEO"),
            FOREIGN KEY("CVE_ENT") REFERENCES agee,
            FOREIGN KEY("CVE_MUN") REFERENCES agem,
            FOREGIN KEY("CVE_LOC") REFERENCES {loc
        )"""
    ).format(sql.Identifier(schema_name))

    with psycopg2.connect(host=host, dbname=dbname, user=user, port=port, password=password, options=f'-c search_path={schema_name}') as conn:
        with conn.cursor() as cur:
            cur.execute(query)

def create_AGE_Manzana(host, dbname, user, port, password, schema_name):
    query = sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS ageb(
            "CVEGEO" varchar(16),
            "CVE_ENT" varchar(2),
            "CVE_MUN" varchar(3),
            "CVE_LOC" varchar(4),
            "CVE_AGEB" varchar(4),
            "CVE_MZA" varchar(3),
            "NOM_GEO" varchar(150),
            "PLANO" varchar(7),
            "GEOM" geometry,
            PRIMARY KEY("CVEGEO"),
            FOREIGN KEY("CVE_ENT") REFERENCES agee,
            FOREIGN KEY("CVE_MUN") REFERENCES agem,
            FOREGIN KEY("CVE_LOC") REFERENCES loc,
            FOREIGN KEY("CVE_AGEB") REFERENCES ageb
        )"""
    ).format(sql.Identifier(schema_name))

    with psycopg2.connect(host=host, dbname=dbname, user=user, port=port, password=password, options=f'-c search_path={schema_name}') as conn:
        with conn.cursor() as cur:
            cur.execute(query)

if __name__ == '__main__':
    create_schema(host, dbname, user, port, password, schema_name)
    create_AGE_Estatales(host, dbname, user, port, password, schema_name)
    create_AGE_Municipales(host, dbname, user, port, password, schema_name)
    create_AGE_Localidad(host, dbname, user, port, password, schema_name)
    create_AGE_Basicas(host, dbname, user, port, password, schema_name)
    create_AGE_Manzana(host, dbname, user, port, password, schema_name)