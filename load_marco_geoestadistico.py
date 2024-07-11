import psycopg2
from psycopg2 import sql
import pathlib
import geopandas as gpd
from sqlalchemy import create_engine
from sqlalchemy import types

host = 'localhost'
dbname = 'seguridad_vial'
user = 'seguridad_vial'
port = 5432
password = 'popcorning'
schema_name = 'geodata'
year = 2022
dir_mg_integrado = f'mg{year}_integrado'

def create_schema(host, dbname, user, port, password, schema_name):
    query = sql.SQL(
        """
        CREATE SCHEMA IF NOT EXISTS {0};
        """
    ).format(sql.Identifier(schema_name))

    with psycopg2.connect(host=host, dbname=dbname, user=user, port=port, password=password, options=f'-c search_path={schema_name}') as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            print(f'schema {schema_name} created')

def create_AGE_Estatales(host, dbname, user, port, password, schema_name, dir_mg_integrado, year):
    path = pathlib.Path(f'{dir_mg_integrado}/conjunto_de_datos/00ent.shp')
    gdf = gpd.read_file(path)
    engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}")
    gdf.to_postgis(name=f'agee_{year}', con=engine, if_exists='replace', schema=schema_name, index=False,
                   dtype={'CVEGEO':types.String(length=2), 'CVE_ENT':types.String(length=2), 'NOMGEO':types.String(length=150)})
    print(f'{schema_name}.agee_{year}: created')

def alter_AGE_Estatales(host, dbname, user, port, password, schema_name, year):
    with psycopg2.connect(host=host, dbname=dbname, user=user, port=port, password=password, options=f'-c search_path={schema_name}') as conn:
        with conn.cursor() as cur:
            alter = sql.SQL("""
                ALTER TABLE {0}
                ADD PRIMARY KEY ("CVE_ENT");
                """).format(sql.Identifier(f'agee_{year}'))
            cur.execute(query=alter)
            print(f'{schema_name}.agee_{year}: altered')

def create_AGE_Municipales(host, dbname, user, port, password, schema_name, dir_mg_integrado, year):
    path = pathlib.Path(f'{dir_mg_integrado}/conjunto_de_datos/00mun.shp')
    gdf = gpd.read_file(path)
    engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}')
    gdf.to_postgis(name=f'agem_{year}', con=engine, if_exists='replace', schema=schema_name, index=False,
                   dtype={'CVEGEO':types.String(length=5), 'CVE_ENT':types.String(length=2),
                          'CVE_MUN':types.String(length=3), 'NOMGEO':types.String(length=150)})
    print(f'{schema_name}.agem_{year}: created')

def alter_AGE_Municipales(host, dbname, user, port, password, schema_name, year):
    with psycopg2.connect(host=host, dbname=dbname, user=user, port=port, password=password, options = f'-c search_path={schema_name}') as conn:
        with conn.cursor() as cur:
            alter = sql.SQL("""
                ALTER TABLE {0}
                ADD PRIMARY KEY ("CVE_ENT", "CVE_MUN"),
                ADD FOREIGN KEY ("CVE_ENT") REFERENCES {1}("CVE_ENT");
                """).format(sql.Identifier(f'agem_{year}'), sql.SQL(f'agee_{year}'))
            cur.execute(query=alter)
        print(f'{schema_name}.agem_{year}: altered')

def create_AGE_Localidad(host, dbname, user, port, password, schema_name, dir_mg_integrado, year):
    path = pathlib.Path(f'{dir_mg_integrado}/conjunto_de_datos/00l.shp')
    gdf = gpd.read_file(path)
    engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}')
    gdf.to_postgis(name=f'loc_{year}', con=engine, if_exists='replace', schema=schema_name, index=False,
                   dtype={'CVEGEO':types.String(length=9), 'CVE_ENT':types.String(length=2),
                          'CVE_MUN':types.String(length=3), 'CVE_LOC':types.String(length=4),
                          'NOMGEO':types.String(length=150), 'AMBITO':types.String(length=6)})
    print(f'{schema_name}.loc_{year}: created')

def alter_AGE_Localidad(host, dbname, user, port, password, schema_name, year):
    with psycopg2.connect(host=host, dbname=dbname, user=user, port=port, password=password, options=f'-c search_path={schema_name}') as conn:
        with conn.cursor() as cur:
            alter = sql.SQL("""
                ALTER TABLE {0}
                ADD PRIMARY KEY ("CVE_ENT", "CVE_MUN", "CVE_LOC"),
                ADD FOREIGN KEY ("CVE_ENT") REFERENCES {1}("CVE_ENT"),
                ADD FOREIGN KEY ("CVE_ENT", "CVE_MUN") REFERENCES {2}("CVE_ENT", "CVE_MUN");
                """).format(sql.Identifier(f'loc_{year}'), sql.Identifier(f'agee_{year}'), sql.Identifier(f'agem_{year}'))
            cur.execute(query=alter)
            print(f'{schema_name}.loc_{year}: altered')

def create_AGE_Basicas(host, dbname, user, port, password, schema_name, dir_mg_integrado, year):
    path = pathlib.Path(f'{dir_mg_integrado}/conjunto_de_datos/00a.shp')
    gdf = gpd.read_file(path)
    gdf_urbana = gdf.loc[gdf['AMBITO']=='Urbana']
    gdf_rural = gdf.loc[gdf['AMBITO']=='Rural', gdf.columns!='CVE_LOC']
    engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}')
    gdf_urbana.to_postgis(name=f'ageb_urbana_{year}', con=engine, if_exists='replace', schema=schema_name, index=False,
                          dtype={'CVEGEO':types.String(length=13), 'CVE_ENT':types.String(length=2),
                                 'CVE_MUN':types.String(length=3), 'CVE_LOC':types.String(length=4),
                                 'CVE_AGEB':types.String(length=4), 'AMBITO':types.String(length=6),})
    print(f'{schema_name}.ageb_urbana_{year}: created')
    gdf_rural.to_postgis(name=f'ageb_rural_{year}', con=engine, if_exists='replace', schema=schema_name, index=False,
                         dtype={'CVEGEO':types.String(length=13), 'CVE_ENT':types.String(length=2),
                                'CVE_MUN':types.String(length=3), 'CVE_AGEB':types.String(length=4), 
                                'AMBITO':types.String(length=6),})
    print(f'{schema_name}.ageb_rural_{year}: created')

def alter_AGE_Basicas(host, dbname, user, port, password, schema_name, year):
    with psycopg2.connect(host=host, dbname=dbname, user=user, port=port, password=password, options=f'-c search_path={schema_name}') as conn:
        with conn.cursor() as cur_urbana:
            alter_urbana = sql.SQL("""
                ALTER TABLE {0}
                ADD PRIMARY KEY ("CVE_ENT", "CVE_MUN", "CVE_LOC", "CVE_AGEB"),
                ADD FOREIGN KEY ("CVE_ENT") REFERENCES {1}("CVE_ENT"),
                ADD FOREIGN KEY ("CVE_ENT", "CVE_MUN") REFERENCES {2}("CVE_ENT", "CVE_MUN"),
                ADD FOREIGN KEY ("CVE_ENT", "CVE_MUN", "CVE_LOC") REFERENCES {3}("CVE_ENT", "CVE_MUN", "CVE_LOC");
                """).format(sql.Identifier(f'ageb_urbana_{year}'), sql.Identifier(f'agee_{year}'),
                            sql.Identifier(f'agem_{year}'), sql.Identifier(f'loc_{year}'))
            cur_urbana.execute(query=alter_urbana)
            print(f'{schema_name}.ageb_urbana_{year}: altered')
        with conn.cursor() as cur_rural:
            alter_rural = sql.SQL("""
                ALTER TABLE {0}
                ADD PRIMARY KEY ("CVE_ENT", "CVE_MUN", "CVE_AGEB"),
                ADD FOREIGN KEY ("CVE_ENT") REFERENCES {1}("CVE_ENT"),
                ADD FOREIGN KEY ("CVE_ENT", "CVE_MUN") REFERENCES {2}("CVE_ENT", "CVE_MUN");
                """).format(sql.Identifier(f'ageb_rural_{year}'), sql.Identifier(f'agee_{year}'),
                            sql.Identifier(f'agem_{year}'))
            cur_rural.execute(query=alter_rural)
            print(f'{schema_name}.ageb_rural_{year}: altered')

def create_AGE_Localidades_Puntuales_Rurales(host, dbname, user, port, password, schema_name, dir_mg_integrado, year):
    path = pathlib.Path(f'{dir_mg_integrado}/conjunto_de_datos/00lpr.shp')
    gdf = gpd.read_file(path)
    gdf_mza = gdf.loc[gdf['PLANO']=='Si']
    gdf_pto = gdf.loc[gdf['PLANO']!='Si']
    engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}')
    gdf_mza.to_postgis(name=f'lpr_mza_{year}', con=engine, if_exists='replace', schema=schema_name, index=False,
                       dtype={'CVEGEO':types.String(length=16), 'CVE_ENT':types.String(length=2),
                              'CVE_MUN':types.String(length=3), 'CVE_LOC':types.String(length=4),
                              'CVE_AGEB':types.String(length=4), 'CVE_AGEB':types.String(length=4),
                              'CVE_MZA':types.String(length=3), 'NOMGEO':types.String(length=150),
                              'PLANO':types.String(length=7)})
    print(f'{schema_name}.lpr_mza_{year}: created')
    gdf_pto.to_postgis(name=f'lpr_pto_{year}', con=engine, if_exists='replace', schema=schema_name, index=False,
                       dtype={'CVEGEO':types.String(length=16), 'CVE_ENT':types.String(length=2),
                              'CVE_MUN':types.String(length=3), 'CVE_LOC':types.String(length=4),
                              'CVE_AGEB':types.String(length=4), 'CVE_AGEB':types.String(length=4),
                              'CVE_MZA':types.String(length=3), 'NOMGEO':types.String(length=150),
                              'PLANO':types.String(length=7)})
    print(f'{schema_name}.lpr_pto_{year}: created')

def alter_AGE_Localidades_Puntuales_Rurales(host, dbname, user, port, password, schema_name, year):
    with psycopg2.connect(host=host, dbname=dbname, user=user, port=port, password=password, options=f'-c search_path={schema_name}') as conn:
        with conn.cursor() as cur_mza:
            alter_mza = sql.SQL("""
                ALTER TABLE {0}
                ADD PRIMARY KEY ("CVE_ENT", "CVE_MUN", "CVE_LOC", "CVE_AGEB", "CVE_MZA"),
                ADD FOREIGN KEY ("CVE_ENT") REFERENCES {1}("CVE_ENT"),
                ADD FOREIGN KEY ("CVE_ENT", "CVE_MUN") REFERENCES {2}("CVE_ENT", "CVE_MUN"),
                ADD FOREIGN KEY ("CVE_ENT", "CVE_MUN", "CVE_LOC") REFERENCES {3}("CVE_ENT", "CVE_MUN", "CVE_LOC");
                """).format(sql.Identifier(f'lpr_mza_{year}'), sql.Identifier(f'agee_{year}'),
                            sql.Identifier(f'agem_{year}'), sql.Identifier(f'loc_{year}'))
            cur_mza.execute(query=alter_mza)
            print(f'{schema_name}.lpr_mza_{year}: altered')
        with conn.cursor() as cur_pto:
            alter_pto = sql.SQL("""
                ALTER TABLE {0}
                ADD PRIMARY KEY ("CVE_ENT", "CVE_MUN", "CVE_LOC", "CVE_AGEB", "CVE_MZA"),
                ADD FOREIGN KEY ("CVE_ENT") REFERENCES {1}("CVE_ENT"),
                ADD FOREIGN KEY ("CVE_ENT", "CVE_MUN") REFERENCES {2}("CVE_ENT", "CVE_MUN"),
                ADD FOREIGN KEY ("CVE_ENT", "CVE_MUN", "CVE_AGEB") REFERENCES {3}("CVE_ENT", "CVE_MUN", "CVE_AGEB");
                """).format(sql.Identifier(f'lpr_pto_{year}'), sql.Identifier(f'agee_{year}'),
                            sql.Identifier(f'agem_{year}'), sql.Identifier(f'ageb_rural_{year}'))
            cur_pto.execute(query=alter_pto)
            print(f'{schema_name}.lpr_pto_{year}: altered')

if __name__ == '__main__':
    create_schema(host, dbname, user, port, password, schema_name)
    create_AGE_Estatales(host, dbname, user, port, password, schema_name, dir_mg_integrado, year)
    alter_AGE_Estatales(host, dbname, user, port, password, schema_name, year)
    create_AGE_Municipales(host, dbname, user, port, password, schema_name, dir_mg_integrado, year)
    alter_AGE_Municipales(host, dbname, user, port, password, schema_name, year)
    create_AGE_Localidad(host, dbname, user, port, password, schema_name, dir_mg_integrado, year)
    alter_AGE_Localidad(host, dbname, user, port, password, schema_name, year)
    create_AGE_Basicas(host, dbname, user, port, password, schema_name, dir_mg_integrado, year)
    alter_AGE_Basicas(host, dbname, user, port, password, schema_name, year)
    create_AGE_Localidades_Puntuales_Rurales(host, dbname, user, port, password, schema_name, dir_mg_integrado, year)
    alter_AGE_Localidades_Puntuales_Rurales(host, dbname, user, port, password, schema_name, year)