import os
import psycopg2
from subprocess import run, Popen, PIPE, DEVNULL
from psycopg2.extras import RealDictCursor
#from osgeonorge.geonorge import list_feeds, atom_url, parse_feed, download_geonorge

from functools import partial
from multiprocessing import Pool

import numpy as np
from osgeo import ogr

from osgeonorge.postgis import PostGISAdapter
from osgeonorge.geonorge import GeonorgeAdapter

# export LD_LIBRARY_PATH=/tmp/gdal_inst/lib:$LD_LIBRARY_PATH
# export PYTHONPATH=/tmp/gdal_inst/lib/python3/dist-packages:$PYTHONPATH

basedir = "/data/scratch"
schema = "tomtereserver_fritidsbolig"
workdir = os.path.join(basedir, schema)
downloaddir = os.path.join(workdir, "ATOM")

download = True

srs = 25833

if not os.path.exists(downloaddir):
    os.makedirs(downloaddir)


# Initialize ETL classes
ogna = GeonorgeAdapter(cores=5, download_dir=downloaddir, work_dir=workdir)
ognp = PostGISAdapter(host="gisdata-db.nina.no", db_name="gisdata", user=os.getlogin(), active_schema=schema, cores=5)


# Create target schema if needed
ognp.schema_check('PG schema for project "Tomtereserver for fritidsbygg"', owner="stefan.blumentrath", users=["trond.simensen"], reader="gisuser")


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Fritidsbygg
fritidsbygg = "fritidsbygg"
ogna.fetch("FKB_Bygning", "FGDB", "Landsdekkende", layer='fkb_bygning_omrade', download=download)
ognp.import_files(fritidsbygg, ogna, where_clause="bygningstype IN (161, 162, 163)", overwrite=True)
ognp.run_maintenance(fritidsbygg,
        [
            ("geom", "gist", True),
            ("kommunenummer", "btree", False),
            ("bygningsstatus", "btree", False),
            ("datafangstdato", "btree", False),
        ],
    )

# Legg til centroid til byggningspolygoner (gjør analysene raskere)
with ognp.connection.cursor() as cur:
    cur.execute(f"""ALTER TABLE "{ognp.active_schema}"."{fritidsbygg}" ADD COLUMN centroid geometry(Point, {srs});
UPDATE "{ognp.active_schema}"."{fritidsbygg}" SET centroid = ST_Centroid(geom);""")
    ognp.run_maintenance(fritidsbygg, [("centroid", "GIST", True)])


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Matrikkel bygg
matrikkel_bygg = "MatrikkelenBygning"
ogna.fetch(matrikkel_bygg, "FGDB", "Landsdekkende", layer='bygning', download=download)
ognp.import_files(matrikkel_bygg, ogna, overwrite=True)
ognp.run_maintenance(matrikkel_bygg.lower(),
        [
            ("geom", "gist", True),
            ("kommunenummer", "btree", False),
            ("bygningsstatus", "btree", False),
            ("oppdateringsdato", "btree", False),
            ("bygningsnummer", "btree", False),
            ("bygningstype", "btree", False),

        ],
    )

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Kommuner
ogna.fetch("AdministrativeEnheterKommuner", "FGDB", "Landsdekkende", layer="kommune", download=download, overwrite=True)
ognp.import_files("kommuner", ogna, overwrite=True)
ognp.run_maintenance("kommuner",
        [
            ("geom", "gist", True),
            ("kommunenummer", "btree", False),
        ],
    )
ods = ogr.Open("/data/scratch/tomtereserver_fritidsbolig/Basisdata_0000_Norge_25833_Kommuner_FGDB.gdb")
ods_layer = ods.GetLayerByName("administrativenhetnavn")

with ognp.connection.cursor() as cur:
    column = "navn"
    if column not in ognp.get_column_names("kommuner"):
        cur.execute(f"""ALTER TABLE "{ognp.active_schema}"."kommuner" ADD COLUMN IF NOT EXISTS {column} varchar(50);""")
    for f in ods_layer:
        attrs = f.items()
        if attrs['sprak'] == 'nor':
            print(f"""UPDATE "{ognp.active_schema}"."kommuner" SET {column} = '{attrs['navn']}'::varchar(50) WHERE objid = {attrs['kommune_fk']};""")
            cur.execute(f"""UPDATE "{ognp.active_schema}"."kommuner" SET {column} = '{attrs['navn']}'::varchar(50) WHERE objid = {attrs['kommune_fk']};""")
ods = None

# Compute land area per municipality
with ognp.connection.cursor() as cur:
    column = "landareal_km2"
    if column not in ognp.get_column_names("kommuner"):
        cur.execute(f"""ALTER TABLE "{ognp.active_schema}"."kommuner" ADD COLUMN IF NOT EXISTS {column} double precision;""")
    cur.execute(f"""UPDATE "{ognp.active_schema}"."kommuner" SET {column} = ST_Area(geom) - x.areal FROM
                 (SELECT a.gid AS id, sum(ST_Area(ST_Intersection(a.geom, b.geom))) AS areal FROM "{ognp.active_schema}"."kommuner" AS a, "{ognp.active_schema}"."n50_arealdekke_omrade" AS b WHERE ST_Intersects(a.geom, b.geom) AND b."objtype" IN ('Innsjø', 'Hav', 'ElvBekk') GROUP BY a.gid) AS x WHERE gid = x.id""")

# Compute number of existing cabins
with ognp.connection.cursor() as cur:
    column = "antall_fritidsbolig"
    if column not in ognp.get_column_names("kommuner"):
        cur.execute(f"""ALTER TABLE "{ognp.active_schema}"."kommuner" ADD COLUMN IF NOT EXISTS {column} integer;""")
    cur.execute(f"""UPDATE "{ognp.active_schema}"."kommuner" SET {column} = antall FROM
                 (SELECT a.gid AS id, count(b.gid) AS antall FROM "{ognp.active_schema}"."kommuner" AS a, "{ognp.active_schema}"."matrikkelenbygning" AS b WHERE b.bygningstype IN (161, 162, 163) AND ST_DWithin(a.geom, b.geom, 0) GROUP BY a.gid) AS x WHERE gid = x.id""")


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Fylker
ogna.fetch("AdministrativeEnheterFylker", "FGDB", "Landsdekkende", layer="fylke", download=download)
ognp.import_files("fylker", ogna, overwrite=True)
ognp.run_maintenance("fylker",
        [
            ("geom", "gist", True),
            ("fylkesnummer", "btree", False),
        ],
    )
ods = ogr.Open("/data/scratch/tomtereserver_fritidsbolig/Basisdata_0000_Norge_25833_Fylker_FGDB.gdb")
ods_layer = ods.GetLayerByName("administrativenhetnavn")
with ognp.connection.cursor() as cur:
    column = "navn"
    if column not in ognp.get_column_names("fylker"):
        cur.execute(f"""ALTER TABLE "{ognp.active_schema}"."fylker" ADD COLUMN IF NOT EXISTS {column} varchar(50);""")
    for f in ods_layer:
        attrs = f.items()
        if attrs['sprak'] == 'nor':
            cur.execute(f"""UPDATE "{ognp.active_schema}"."fylker" SET {column} = '{attrs['navn']}' WHERE objid = {attrs['fylke_fk']};""")
ods = None

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# SSB Arealbruk
ogna.fetch("Arealbruk", "FGDB", "Kommunevis", crs=(25833,))
ognp.import_files("arealbruk_ssb", ogna, overwrite=False)

# Correct geometry issues
with ognp.connection.cursor() as cur:
    column = "geom_valid"
    cur_table = "arealbruk_ssb_ssbarealbrukflate"
    if column not in ognp.get_column_names("arealbruk_ssb_ssbarealbrukflate"):
        cur.execute(f"""SELECT AddGeometryColumn('{ognp.active_schema}'::varchar, '{cur_table}'::varchar, '{column}'::varchar, 25833, 'MULTIPOLYGON'::varchar, 2);""")
    cur.execute(f"""UPDATE "{ognp.active_schema}"."{cur_table}" SET {column} = CASE WHEN ST_IsValid(geom) THEN geom ELSE ST_CollectionExtract(ST_MakeValid(geom), 3) END;""")
ognp.run_maintenance("arealbruk_ssb_ssbarealbrukflate",
        [
            ("geom_valid", "gist", True),
            ("objtype", "btree", False),
            ("hovedklasse", "btree", False),
        ],
    )


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Reguleringsplaner
ogna.fetch("Reguleringsplaner", "PostGIS", "Landsdekkende", get_metadata=False, download=download)

indices = {"omrade": ("omrade", "gist", True),
"grense": ("grense", "gist", True),
"posisjon": ("posisjon", "gist", True),
"arealformal": ("arealformal", "btree", False),
"kommunenummer": ("kommunenummer", "btree", False),
"lokalid": ("lokalid", "btree", False),
"plantype": ("plantype", "btree", False),
"planstatus": ("planstatus", "btree", False),
"planidentifiksjon": ("planidentifiksjon", "btree", False),
"utnyttingstype": ("utnyttingstype", "btree", False),
"eierform": ("eierform", "btree", False),
"ikrafttredelsesdato": ("ikrafttredelsesdato", "btree", False),
"vertikalniva": ("vertikalniva", "btree", False),
"utnyttingstall": ("utnyttingstall", "btree", False),
}
ognp.import_pg_backup(ogna, indices=indices)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Kommuneplaner
ogna.fetch("Kommuneplaner", "SOSI", "Kommunevis", crs=(25833,), split_by_objtype=True, layer="polygons", download=download)
ognp.import_files("kommuneplaner", ogna, remove_prefix="kp", target_crs=25833, overwrite=False)

indices = [("geom", "gist", True),
("plannavn", "btree", False),
("plantype", "btree", False),
("arealformal", "btree", False),
("planstatus", "btree", False),
("planbestemmelse", "btree", False),
("ikrafttredelsesdato", "btree", False),
("vedtakendeligplandato", "btree", False),
("kunngjøringsdato", "btree", False),
("kommunenummer", "btree", False),
("planidentifikasjon", "btree", False),
]

for table in ["kommuneplaner_arealformalomrade",
"kommuneplaner_omrade",
"kommuneplaner_arealbrukomrade",]:
    ognp.run_maintenance(table,indices)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Tettsteder2021_AtomFeedPostGIS
ogna.fetch("Tettsteder2021", "FGDB", 'Landsdekkende', crs=(25833,))
ognp.import_files(None, ogna, target_crs=25833)
indices = [("geom", "gist", True),
("totalbefolkning", "btree", False),
("befolkningstetthet", "btree", False),
]
ognp.run_maintenance("tettsteder2021_tettsted", indices)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Reindrift
rb_datasets = [("avtaleomrade", "flate"),
    ("beitehage", "flate"),
    ("ekspropriasjonsomrade", "flate"),
    ("konsesjonsomrade", "flate"),
    ("trekklei", "linje"),
    ("konvensjonsomrade", "flate"),
    ("flyttlei", "flate"),
    ("oppsamlingsomrade", "flate"),
    ("reinbeiteomrade", "flate"),
    ("reinbeitedistrikt", "flate"),
    ("reindriftsanlegg", "punkt"),
    ("siidaomrade", "flate"),
    ("restriksjonsomrade", "flate"),
    ]
for dataset in rb_datasets:
    ogna.fetch(f"https://kart8.nibio.no/uttak_Download/reindrift/0000_25833_reindrift-{dataset[0]}_gdb.zip", "FGDB", None, is_url=True)

ognp.import_files("reindrift", ogna, target_crs=25833)

for dataset in rb_datasets:
    ognp.run_maintenance(f"reindrift_reindrift_{dataset[0]}_{dataset[1]}", [("geom", "gist", True)])


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Datasets from Miljoedirektoratet
imp_list = {}
for topic in [("ForeslattVern","https://nedlasting.miljodirektoratet.no/Miljodata/ForeslattVern/FILEGDB/25833/Natur_ForeslattVern_norge_med_svalbard_25833.zip"),
("Villrein","https://nedlasting.miljodirektoratet.no/Miljodata/Villrein/FILEGDB/25833/Natur_Villrein_norge_med_svalbard_25833.zip"),
("Vern","https://nedlasting.miljodirektoratet.no/Miljodata/Vern/FILEGDB/25833/Natur_Vern_norge_med_svalbard_25833.zip"),
("NaturtyperUtvalgte","https://nedlasting.miljodirektoratet.no/Miljodata/NaturtyperUtvalgte/FILEGDB/25833/Natur_NaturtyperUtvalgte_norge_med_svalbard_25833.zip"),
# ("ArtFunksjon","https://nedlasting.miljodirektoratet.no/Miljodata/ArtFunksjon/FILEGDB/25833/Natur_ArtFunksjon_norge_med_svalbard_25833.zip"),
# ("Kulturlandskap","https://nedlasting.miljodirektoratet.no/Miljodata/Kulturlandskap/FILEGDB/25833/Natur_Kulturlandskap_norge_med_svalbard_25833.zip"),
("Naturtyper_NiN","https://nedlasting.miljodirektoratet.no/Miljodata/Naturtyper_nin/FILEGDB/25833/Natur_Naturtyper_NiN_norge_med_svalbard_25833.zip"),
("Naturtyper_hb19","https://nedlasting.miljodirektoratet.no/Miljodata/Naturtyper_hb19/FILEGDB/25833/Natur_Naturtyper_hb19_norge_med_svalbard_25833.zip"),
("Naturtyper_hb13","https://nedlasting.miljodirektoratet.no/Miljodata/Naturtyper_hb13/FILEGDB/25833/Natur_Naturtyper_hb13_norge_med_svalbard_25833.zip"),
]:
    ogna.fetch(topic[1], "FGDB", None, is_url=True)

ognp.import_files("mdir", ogna, target_crs=25833, overwrite=True)

    imp_list[topic[0]] = ogna.current_ogr_files_metadata

indices = [("geom", "gist", True),
('objtype', "btree", False),]

ognp.run_maintenance('mdir_foreslattvern_norge_med_svalbard', indices + [('verneform', "btree", False),])

ognp.run_maintenance('mdir_naturtyper_hb13_norge_med_svalbard', indices + [
('bmverdi', "btree", False),
('utvalgtnaturtype', "btree", False),
])

ognp.run_maintenance('mdir_villrein_norge_med_svalbard', indices + [
('funksjon', "btree", False),
('objtype', "btree", False),
])

ognp.run_maintenance('mdir_naturvernområde', indices + [
('iucn', "btree", False),
('verneform', "btree", False),
])

ognp.run_maintenance('mdir_naturtyper_nin_norge_med_svalbard'
, indices + [
('lokalitetskvalitet', "btree", False),
])

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# BefolkningsstatistikkRutenett250m2019
ogna.fetch("BefolkningsstatistikkRutenett250m2019", data_format="POSTGIS", 'Landsdekkende', crs=(25833,))
indices = [("omrade", "gist", True),
("poptot", "btree", False),
]
ognp.import_pg_backup(ogna, None)
ognp.run_maintenance("befolkningparuter250m_ffda34" ,indices)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Statlige planrettningslinjer for strandsonen
ogna.fetch("Sprstrandsoner", "FGDB", "Landsdekkende", layer="rpromrade", download=download, overwrite=True)
ognp.import_files("planrettningslinjer_strandsonen", ogna, target_crs=25833)
for layer in ["rprgrense","rpromrade"]:
    ognp.run_maintenance(f"planrettningslinjer_strandsonen_{layer}",
        [
            ("geom", "gist", True),
            ("kommune", "btree", False),
            ("objtype", "btree", False),
            ("planretningslinjerstrandsone", "btree", False),
        ],
    )


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# NVDB nettverk
ogna.fetch("NVDBRuteplanNettverksdatasett", "FGDB", "Landsdekkende", download=download)
ognp.import_files("nvdb_nettverk", ogna, target_crs=25833)
ognp.run_maintenance("nvdb_nettverk_erfkps",
        [
            ("geom", "gist", True),
            #("tf_fart", "btree", False),
            #("tk_fart", "btree", False),
        ],
    )


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Flomsone / Skredfaresone / Kvikkleire
zone_layers = {"Flomsoner": "flomsoner_flomareal",
"Skredfaresoner": "skredfaresone",
"Kvikkleire": None,  # ["kvikkleire_utlopomr", "kvikkleire_utlosningomr"]
}
for zone, layer in zone_layers.items():
    ogna.fetch(zone, "FGDB", "Landsdekkende", crs=(25833,), layer=layer, download=download)
    ognp.import_files(None, ogna, target_crs=25833, overwrite=True)

for layer in ["flomsoner_flomareal", "skredfaresone", "kvikkleire_utlopomr", "kvikkleire_utlosningomr"]:
    ognp.run_maintenance(layer,
        [
            ("geom", "gist", True),
            ("skredstatistikksannsynlighet", "btree", False),
            ("gjentaksinterval", "btree", False),
        ],
    )


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# NiN Landskap
ogna.fetch("NaturtyperINorgeLandskap", "FGDB", "Landsdekkende", download=download)
ognp.import_files(None, ogna, target_crs=25833)
ognp.run_maintenance("nin_landskap",
        [
            ("geom", "gist", True),
            ("grunntypenavn", "btree", False),
            ("objtype", "btree", False),
        ],
    )

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# N50 Kartdata
ogna.fetch("N50Kartdata", "FGDB", "Landsdekkende", download=download)
ognp.import_files(None, ogna, target_crs=25833)
with ognp.connection.cursor() as cur:
    cur.execute(f"""UPDATE "{ognp.active_schema}"."n50_arealdekke_omrade" SET geom = ST_MakeValid(geom) WHERE ST_IsValid(geom) = False;""")

ognp.run_maintenance("n50_arealdekke_omrade",
        [
            ("geom", "gist", True),
            ("objtype", "btree", False),
        ],
    )
ognp.run_maintenance("n50_arealdekke_grense",
        [
            ("geom", "gist", True),
            ("objtype", "btree", False),
        ],
    )


# Compute land area per municipality
with ognp.connection.cursor() as cur:
    column = "landareal_km2"
    if column not in ognp.get_column_names("kommuner"):
        cur.execute(f"""ALTER TABLE "{ognp.active_schema}"."kommuner" ADD COLUMN IF NOT EXISTS {column} double precision;""")
    cur.execute(f"""UPDATE "{ognp.active_schema}"."kommuner" SET {column} = CASE WHEN x.areal IS NULL THEN ST_Area(geom) ELSE (ST_Area(geom) - x.areal) END / 1000000.0 FROM
                 (SELECT a.kommunenummer AS kn, sum(ST_Area(ST_MakeValid(ST_Intersection(a.geom, b.geom)))) AS areal
		 FROM "{ognp.active_schema}"."kommuner" AS a,
		 "{ognp.active_schema}"."n50_arealdekke_omrade" AS b
		 WHERE ST_Intersects(a.geom, b.geom) AND b."objtype" IN ('Innsjø', 'Hav', 'ElvBekk')
		 GROUP BY a.kommunenummer) AS x WHERE kommunenummer = x.kn""")


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Kommuneplandata fra SSB
files = ["F1_ArealformalUtenDoble.shp", "F2_OplarealUtenDoble.shp"]
for f in files:
    layer = f.rstrip(".shp")
    f = os.path.join(workdir, f)
    run(
        [
            "ogr2ogr",
            "-overwrite",
            "--config",
            "PG_USE_COPY",
            "YES",
            "-lco",
            "LAUNDER=NO",
            "-a_srs",
            f"EPSG:{srs}",
            "-f",
            "PostgreSQL",
            "-lco",
            "GEOMETRY_NAME=geom",
            "-lco",
            "FID=gid",
            "-lco",
            "SPATIAL_INDEX=NONE",
            "-lco",
            "PRECISION=NO",
            "-nlt",
            "PROMOTE_TO_MULTI",
            "-nln",
            f"{schema}.{layer}",
            f"PG:host={host} dbname={dbname} user={user} active_schema={schema}",
            f,
            layer,
        ]
    )
    if layer == "F1_ArealformalUtenDoble":
        indices = [
                ("geom", "gist", True),
                ("KOMMUNENR", "btree", False),
                ("KPAREALFOR", "btree", True),
                ]
    else:
        indices = [
                ("geom", "gist", True),
                ("KOMMUNENR", "btree", False),
                ("OPLAREAL", "btree", True),
                ("FID_D2_Opl", "btree", True),
                ]

    with psycopg2.connect(connection_string) as con:
        con.set_session(autocommit=True)
        run_maintenance(
            con,
            schema,
            layer,
            indices,
        )


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Union reguleringsplan data
columns_planomrade = ['plannavn',
 'plantype',
 'planstatus',
 'planbestemmelse',
 'ikrafttredelsesdato',
 'vedtakendeligplandato',
 'kunngjoringsdato',
 'forslagsstillertype',
 'lovreferanse',
 'lovreferansebeskrivelse',
 'opprinneligplanid',
 'opprinneligadministrativenhet',
 'prosesshistorie',
 'informasjon',
 'link']

arealformal = ["1120", "1121", "1122", "1123", "1171", "1173", "5200", "5220"]
arealformal = f"""'{"','".join(arealformal)}'"""
union_sql = []

# Plan status to filter out
invalid_planstatus = [4, 5, 8]
invalid_planstatus = ",".join([f"'{str(ps)}'" for ps in invalid_planstatus])


for niva in range(1, 6):
    ognp.run_maintenance(f"reguleringsplaner_rpomrade_omrade_vn{niva}", [("kommunenummer", "btree", True), ("planidentifikasjon", "btree", False), ("vertikalniva", "btree", False), ("plantype", "btree", False)
])
    with ognp.connection.cursor() as cur:
        cur.execute(f"""DROP TABLE IF EXISTS "{ognp.active_schema}"."reguleringsplaner_omrade_guldig_vn{niva}";
CREATE TABLE "{ognp.active_schema}"."reguleringsplaner_omrade_guldig_vn{niva}" AS SELECT kommunenummer, planidentifikasjon, vertikalniva, plantype, planstatus, geom
FROM (SELECT DISTINCT ON (kommunenummer, planidentifikasjon, vertikalniva, plantype)
	  kommunenummer, planidentifikasjon, vertikalniva, plantype, planstatus, omrade AS geom FROM "{ognp.active_schema}"."reguleringsplaner_rpomrade_omrade_vn{niva}"
	  ORDER BY kommunenummer, planidentifikasjon, vertikalniva, plantype, CAST(versjonid AS timestamp) DESC, ikrafttredelsesdato DESC, vedtakendeligplandato DESC) AS x WHERE planstatus NOT IN ('4','5','8');""")
    ognp.run_maintenance(f"reguleringsplaner_omrade_guldig_vn{niva}", [("kommunenummer", "btree", True), ("planidentifikasjon", "btree", False), ("vertikalniva", "btree", False), ("plantype", "btree", False)
])

    p_cols = ", ".join([f"vn{niva}_p.{col}" for col in columns_planomrade])
    p_cols += f", CAST({niva} AS smallint) AS niva"
    union_sql.append(f"""SELECT vn{niva}_po.*, vn{niva}_o.description AS arealformal_tekst, vn{niva}_s.description AS planstatus_tekst FROM (SELECT vn{niva}_x.*, vn{niva}_p.omrade AS planomrade, {p_cols} FROM "{ognp.active_schema}"."reguleringsplaner_rparealformalomrade_omrade_vn{niva}" AS vn{niva}_x,
(SELECT DISTINCT ON (omrade, kommunenummer, planidentifikasjon, vertikalniva, plantype) a{niva}.* FROM "{ognp.active_schema}"."reguleringsplaner_rpomrade_omrade_vn{niva}" AS a{niva} NATURAL INNER JOIN "{ognp.active_schema}"."reguleringsplaner_omrade_guldig_vn{niva}" AS b{niva}) AS vn{niva}_p
WHERE vn{niva}_x.kommunenummer = vn{niva}_p.kommunenummer
AND vn{niva}_x.planidentifikasjon = vn{niva}_p.planidentifikasjon
AND vn{niva}_x.vertikalniva = vn{niva}_p.vertikalniva
AND ST_Intersects(vn{niva}_x.omrade, vn{niva}_p.omrade)
) AS vn{niva}_po
LEFT JOIN "{ognp.active_schema}"."reguleringsplaner_rparealformal_vn{niva}" AS vn{niva}_o ON (vn{niva}_po.arealformal = vn{niva}_o.identifier) LEFT JOIN "{ognp.active_schema}"."reguleringsplaner_planstatus_vn{niva}" AS vn{niva}_s ON (vn{niva}_po.planstatus = vn{niva}_s.identifier)""")
union_sql = " UNION ALL ".join(union_sql)

with ognp.connection.cursor(cursor_factory = RealDictCursor) as cur:
    cur.execute(f"""DROP TABLE IF EXISTS "{ognp.active_schema}"."reguleringsplaner_rpomrader_samlet" CASCADE;""")
    cur.execute(f"""CREATE TABLE "{ognp.active_schema}"."reguleringsplaner_rpomrader_samlet" AS {union_sql};""")
ognp.run_maintenance("reguleringsplaner_rpomrader_samlet", [("omrade", "GIST", True),("planomrade", "GIST", False), ("arealformal", "btree", True), ("arealformal_tekst", "btree", True), ("planstatus_tekst", "btree", True)])


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Union kommuneplan data while excluding all outdated versions of a specific plan as well as "båndleggingssoner" and invalid planstatus
#kp_uniq = "kommuneplaner_samlet"
#kp_uniq_sql
kp_samlet = "kommuneplaner_samlet"
kp_samlet_sql = f"""CREATE TEMP TABLE planids AS SELECT DISTINCT ON (kommunenummer, planidentifikasjon, plantype) kommunenummer || '_' || planidentifikasjon || '_' || plantype AS pid FROM "{ognp.active_schema}"."kommuneplaner_omrade" ORDER BY kommunenummer, planidentifikasjon, plantype, CAST(versjonid AS timestamp) DESC, ikrafttredelsesdato DESC, vedtakendeligplandato DESC;
CREATE TABLE "{ognp.active_schema}"."{kp_samlet}" AS
SELECT DISTINCT ON (geom)
x."gid",
x."lokalid",
x."navnerom",
x."versjonid",
x."arealformål",
x."arealbruksstatus",
x."kommunenummer",
x."kopidato",
x."objekttypenavn",
x."områdeid",
x."områdenavn",
x."oppdateringsdato",
x."originaldatavert",
x."planidentifikasjon",
x."geom",
p.geom AS planomrade, p.plannavn, p.plantype, p.planstatus, p.planbestemmelse, p.ikrafttredelsesdato, p.vedtakendeligplandato, p.lovreferansetype, p.opprinneligplanid, p.opprinneligadministrativenhet, p.link FROM "{ognp.active_schema}"."kommuneplaner_arealformalomrade" AS x,
(
SELECT DISTINCT ON (geom, kommunenummer, planidentifikasjon, plantype) * FROM "{ognp.active_schema}"."kommuneplaner_omrade"
WHERE planstatus NOT IN ('4','5','8') AND  kommunenummer || '_' || planidentifikasjon || '_' || plantype IN (SELECT pid FROM planids)
ORDER BY geom, kommunenummer, planidentifikasjon, plantype, CAST(versjonid AS timestamp) DESC, ikrafttredelsesdato DESC, vedtakendeligplandato DESC) AS p
WHERE x.kommunenummer = p.kommunenummer
AND x.planidentifikasjon = p.planidentifikasjon
AND ST_Intersects(x.geom, p.geom)
UNION ALL
SELECT x."gid", x."lokalid",
x."navnerom", x."versjonid",
x."arealbruk" AS "arealformål",
x."arealbruksstatus",
x."kommunenummer",
x."kopidato",
x."objekttypenavn",
x."områdeid",
x."områdenavn",
x."oppdateringsdato",
x."originaldatavert",
x."planidentifikasjon",
x."geom",
p.geom AS planomrade, p.plannavn, p.plantype, p.planstatus, p.planbestemmelse, p.ikrafttredelsesdato, p.vedtakendeligplandato, p.lovreferansetype, p.opprinneligplanid, p.opprinneligadministrativenhet, p.link FROM "{ognp.active_schema}"."kommuneplaner_arealbrukomrade" AS x,
(
SELECT DISTINCT ON (geom, kommunenummer, planidentifikasjon, plantype) * FROM "{ognp.active_schema}"."kommuneplaner_omrade"
WHERE planstatus NOT IN ('4','5','8') AND  kommunenummer || '_' || planidentifikasjon || '_' || plantype IN (SELECT pid FROM planids)
ORDER BY geom, kommunenummer, planidentifikasjon, plantype, CAST(versjonid AS timestamp) DESC, ikrafttredelsesdato DESC, vedtakendeligplandato DESC) AS p
WHERE x.kommunenummer = p.kommunenummer
AND x.planidentifikasjon = p.planidentifikasjon
AND ST_Intersects(x.geom, p.geom)
-- Exclude båndleggingssoner
AND x."arealbruk" NOT BETWEEN 400 AND 499;
"""

with ognp.connection.cursor() as cur:
    cur.execute(f"""DROP TABLE IF EXISTS "{ognp.active_schema}"."{kp_samlet}" CASCADE;""")
    cur.execute(kp_samlet_sql)

ognp.run_maintenance("kommuneplaner_samlet", [
    ("geom", "GIST", True),
    ("planomrade", "GIST", False),
    ("gid", "btree", True),
    ("arealformal", "btree", True),
    ("planstatus", "btree", True),
    ("plantype", "btree", True),
    ("kommunenummer", "btree", True),
    ("ikrafttredelsesdato", "btree", True),
    ])


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Merge municipal planning data (create view)
merge_sql = f"""DROP VIEW IF EXISTS "{ognp.active_schema}"."kommunale_planer_samlet";
CREATE OR REPLACE VIEW "{ognp.active_schema}".kommunale_planer_samlet AS
SELECT * FROM (SELECT
  gid,
  "områdeid" AS omradeid,
  geom,
  CAST(lokalid AS text),
  CAST(kommunenummer AS text),
  CAST(navnerom AS text),
  CAST(planidentifikasjon AS text),
  CAST('Kommuneplan' AS varchar(20)) AS plankategori,
  CAST("arealformål" AS integer) AS arealformal,
  arealbruksstatus,
  oppdateringsdato,
  CAST(versjonid AS text),
  vedtakendeligplandato,
  ikrafttredelsesdato,
  kopidato,
  CAST(NULL AS timestamp with time zone) AS forstedigitaliseringsdato,
  CAST(NULL AS text) AS prosesshistorie,
  CAST(objekttypenavn AS text),
  CAST("områdenavn" AS text) AS omradenavn,
  CAST(NULL AS text) AS beskrivelse,
  CAST(originaldatavert AS text),
  CAST(plannavn AS text),
  plantype,
  planstatus,
  planbestemmelse,
  CAST(opprinneligplanid AS text),
  CAST(opprinneligadministrativenhet AS text),
  lovreferansetype,
  CAST(NULL AS text) AS vertikalniva,
  CAST(link AS text),
  CAST(NULL AS text) AS utnyttingstype,
  CAST(NULL AS double precision) AS utnyttingstall,
  CAST(NULL AS double precision) AS utnyttingstall_minimum,
  CAST(NULL AS text) AS informasjon
FROM
  "{ognp.active_schema}".kommuneplaner_samlet) AS a
UNION
SELECT * FROM (SELECT
  objid  AS gid,
  CAST(omradeid AS integer),
  omrade AS geom,
  lokalid,
  kommunenummer,
  navnerom,
  planidentifikasjon,
  CAST('Reguleringsplan' AS varchar(20)) AS plankategori,
  CAST(arealformal AS integer),
  CAST(NULL AS integer) AS arealbruksstatus,
  oppdateringsdato,
  versjonid,
  vedtakendeligplandato,
  ikrafttredelsesdato,
  CAST(NULL AS timestamp with time zone) AS kopidato,
  forstedigitaliseringsdato,
  prosesshistorie,
  CAST(NULL AS text) AS objekttypenavn,
  CAST(NULL AS text) AS omradenavn,
  beskrivelse,
  CAST(originaldatavert AS text),
  plannavn,
  CAST(plantype AS integer),
  CAST(planstatus AS integer),
  CAST(NULL AS integer) AS planbestemmelse,
  opprinneligplanid,
  opprinneligadministrativenhet,
  CAST(lovreferanse AS integer),
  vertikalniva,
  link,
  utnyttingstype,
  utnyttingstall,
  utnyttingstall_minimum,
  informasjon
FROM
  "{ognp.active_schema}".reguleringsplaner_rpomrader_samlet WHERE niva = 2) AS b;"""
with ognp.connection.cursor() as cur:
    cur.execute(merge_sql)


with ognp.connection.cursor() as cur:
    cur.execute(f"""DROP TABLE IF EXISTS "{ognp.active_schema}".plandata_stats;
CREATE TABLE "{ognp.active_schema}".plandata_stats AS
SELECT x.kommunenummer,
  CASE
    WHEN plantype IS NULL THEN 'Ingen plandata'
    ELSE plantype
  END AS plantype
FROM
  tomtereserver_fritidsbolig.kommuner AS x LEFT JOIN
  (
  SELECT kommunenummer, array_to_string(array_agg(plantype ORDER BY plantype), ',') AS plantype FROM
  (SELECT * FROM (SELECT DISTINCT ON (a.kommunenummer) a.kommunenummer, 'Kommuneplan'::text AS plantype FROM
    tomtereserver_fritidsbolig.kommuner AS a,
    tomtereserver_fritidsbolig.kommuneplaner_samlet AS b WHERE ST_Within(b.geom, a.geom)) AS a1
  UNION ALL
  SELECT * FROM (SELECT DISTINCT ON (c.kommunenummer) c.kommunenummer, 'Reguleringsplan'::text AS plantype FROM
    tomtereserver_fritidsbolig.kommuner AS c,
    (SELECT omrade FROM "tomtereserver_fritidsbolig"."reguleringsplaner_rparealformalomrade_omrade_vn1" UNION ALL
     SELECT omrade FROM "tomtereserver_fritidsbolig"."reguleringsplaner_rparealformalomrade_omrade_vn2" UNION ALL
     SELECT omrade FROM "tomtereserver_fritidsbolig"."reguleringsplaner_rparealformalomrade_omrade_vn3" UNION ALL
     SELECT omrade FROM "tomtereserver_fritidsbolig"."reguleringsplaner_rparealformalomrade_omrade_vn4" UNION ALL
     SELECT omrade FROM "tomtereserver_fritidsbolig"."reguleringsplaner_rparealformalomrade_omrade_vn5") AS d
  WHERE ST_Within(d.omrade, c.geom)) AS a2
   ) AS y GROUP BY kommunenummer) AS z
  USING (kommunenummer)
  ;""")


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Topologically clean municipal planning data
kp_samlet = "kommuneplaner_samlet"
subprocess.run(["python3", "clean_planning_data.py"])


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Spatialy join existing cabins and municipal plans
fritidsbygg = "fritidsbygg"
vector_map = "kommunale_planer_samlet_cleaned"
planformalomrader = "kommunale_planer_samlet_cleaned_pg"
fritidsboligformal = "fritidsboligformal"
relevante_formal = ["140", "1120", "1121", "1122", "1123"]

with ognp.connection.cursor() as cur:
     cur.execute(f"""ALTER TABLE "{ognp.active_schema}"."{vector_map}" ADD COLUMN areal_m2 double precision;
ALTER TABLE "{ognp.active_schema}"."{vector_map}" ADD COLUMN perimeter double precision;
ALTER TABLE "{ognp.active_schema}"."{vector_map}" ADD COLUMN fractal_dimension_pg double precision;
ALTER TABLE "{ognp.active_schema}"."{vector_map}" ADD COLUMN compactness_pg double precision;
ALTER TABLE "{ognp.active_schema}"."{vector_map}" ADD COLUMN geom_isvalid smallint;
ALTER TABLE "{ognp.active_schema}"."{vector_map}" ADD COLUMN geom geometry(Polygon, 25833);
UPDATE "{ognp.active_schema}"."{vector_map}" SET areal_m2 = ST_Area(wkb_geometry),
perimeter = ST_Perimeter(wkb_geometry),
compactness_pg = ST_Perimeter(wkb_geometry) / sqrt(pi() * ST_Area(wkb_geometry)),
fractal_dimension_pg = 2 * (log(ST_Perimeter(wkb_geometry)) / log(ST_Area(wkb_geometry))),
geom_isvalid = CASE WHEN ST_IsValid(wkb_geometry) THEN 1 else 0 END,
geom = CASE WHEN ST_IsValid(wkb_geometry) THEN wkb_geometry ELSE ST_MakeValid(wkb_geometry) END;""")

ognp.run_maintenance(vector_map, [("geom", "gist", True), ("geom_isvalid", "btree", False), ("fractal_dimension_pg", "btree", False), ("compactness_pg", "btree", False)])


with ognp.connection.cursor() as cur:
    cur.execute(f"""SELECT sum(areal_m2), kommunenummer FROM "{ognp.active_schema}"."{planformalomrader}" GROUP BY kommunenummer;""")

with ognp.connection.cursor() as cur:
    cur.execute(f"""DROP TABLE IF EXISTS "{ognp.active_schema}"."{planformalomrader}" CASCADE;
CREATE TABLE "{ognp.active_schema}"."{planformalomrader}" AS SELECT
CAST(row_number() OVER () AS integer) AS poid,
*
FROM
(SELECT
ST_Collect(geom) AS geom,
sum(ST_Area(geom)) AS areal_m2,
lokalid,
kommunenummer,
planidentifikasjon,
plankategori,
arealformal,
arealbruksstatus,
oppdateringsdato,
versjonid,
vedtakendeligplandato,
ikrafttredelsesdato,
kopidato,
forstedigitaliseringsdato,
prosesshistorie,
objekttypenavn,
omradenavn,
beskrivelse,
originaldatavert,
plannavn,
plantype,
planstatus,
planbestemmelse,
opprinneligplanid,
opprinneligadministrativenhet,
lovreferansetype,
vertikalniva,
link,
utnyttingstype,
utnyttingstall,
utnyttingstall_minimum,
informasjon
FROM
"{ognp.active_schema}"."{vector_map}"
WHERE fractal_dimension_pg < 1.86 OR arealformal NOT IN ({", ".join(relevante_formal)})
GROUP BY lokalid,
kommunenummer,
planidentifikasjon,
plankategori,
arealformal,
arealbruksstatus,
oppdateringsdato,
versjonid,
vedtakendeligplandato,
ikrafttredelsesdato,
kopidato,
forstedigitaliseringsdato,
prosesshistorie,
objekttypenavn,
omradenavn,
beskrivelse,
originaldatavert,
plannavn,
plantype,
planstatus,
planbestemmelse,
opprinneligplanid,
opprinneligadministrativenhet,
lovreferansetype,
vertikalniva,
link,
utnyttingstype,
utnyttingstall,
utnyttingstall_minimum,
informasjon
) AS x;""")

ognp.run_maintenance(planformalomrader, [("geom", "gist", True), ("poid", "btree", False), ("kommunenummer", "btree", False), ("arealformal", "btree", False)])

columns = ", ".join(["y.{}".format(c) for c in ognp.get_column_names(planformalomrader)])

with ognp.connection.cursor() as cur:
    cur.execute(f"""ALTER TABLE "{ognp.active_schema}".kommuner ADD COLUMN IF NOT EXISTS dekkning_plandata_andel smallint;
    UPDATE "{ognp.active_schema}".kommuner SET dekkning_plandata_andel = CAST(dekkning_plandata_perc AS smallint) FROM (SELECT a.kommunenummer AS kid, round((((b.areal_m2 / 1000000.0) / a.landareal_km2)*100.0)::numeric, 0) AS dekkning_plandata_perc FROM
    (SELECT kommunenummer, sum(landareal_km2) AS landareal_km2 FROM "{ognp.active_schema}".kommuner GROUP BY kommunenummer) AS a LEFT JOIN
    (SELECT sum(areal_m2) AS areal_m2, kommunenummer FROM "{ognp.active_schema}"."{planformalomrader}" GROUP BY kommunenummer) AS b USING (kommunenummer)) AS x WHERE x.kid = kommunenummer;""")


# By planomrade
with ognp.connection.cursor() as cur:
    cur.execute(f"""DROP TABLE IF EXISTS "{ognp.active_schema}"."plan_og_fritidsbygg";
CREATE TABLE "{ognp.active_schema}"."plan_og_fritidsbygg" AS SELECT
  {columns}
  , ST_X(ST_Centroid(y.geom)) AS utm_33_x
  , ST_Y(ST_Centroid(y.geom)) AS utm_33_y
  , count(x.gid) / ST_Area(y.geom) AS antall_hytter_per_m2
  , count(x.gid) AS antall_bygninger
  , array_agg(DISTINCT x.bygningstype) AS bygningstype_uniq
  , array_agg(DISTINCT x.bygningsstatus) AS bygningsstatus_uniq
  -- , array_agg(DISTINCT x.innmalingsstatus) AS innmalingsstatus_uniq
  , ST_Collect(x.geom) AS bygningcentroid
  , ST_Centroid(y.geom) AS geom_centroid
  , max(x.oppdateringsdato) AS bygningoppdateringsdato_max
  , to_timestamp(avg(extract(epoch from x.oppdateringsdato))) AS bygningoppdateringsdato_avg
  , min(x.oppdateringsdato) AS bygningoppdateringsdato_min
  , array_agg(x.oppdateringsdato) AS bygningoppdateringsdato
FROM
  "{ognp.active_schema}"."{planformalomrader}" AS y LEFT JOIN
  (SELECT * FROM "{ognp.active_schema}"."matrikkelenbygning" WHERE bygningstype IN (161, 162, 163)) AS x
  ON ST_DWithin(y.geom, x.geom, 0)
GROUP BY {columns}
;""")

indices = [
    ("geom", "GIST", True),
    ("poid", "btree", False),
    ("geom_centroid", "GIST", False),
    ("bygningcentroid", "GIST", False),
    ("utm_33_x", "btree", False),
    ("utm_33_y", "btree", False),
    ("antall_bygninger", "btree", False),
    ("antall_hytter_per_m2", "btree", False),
    ("bygningoppdateringsdato_max", "btree", False),
    ("bygningoppdateringsdato_avg", "btree", False),
    ("kommunenummer", "btree", False),
    ("arealformal", "btree", False),
    ("plankategori", "btree", False),
    ("planstatus", "btree", False),
    ("plantype", "btree", False),
    ]

ognp.run_maintenance("plan_og_fritidsbygg", indices)

with ognp.connection.cursor() as cur:
    cur.execute(f"""DROP TABLE IF EXISTS "{ognp.active_schema}"."{fritidsboligformal}" CASCADE;
CREATE TABLE "{ognp.active_schema}"."{fritidsboligformal}" AS
SELECT * FROM "{ognp.active_schema}"."plan_og_fritidsbygg"
WHERE arealformal IN ({",".join(relevante_formal)});""")
ognp.run_maintenance(fritidsboligformal, indices)


# Add municipality attributes
with ognp.connection.cursor() as cur:
    cur.execute(f"""ALTER TABLE "{ognp.active_schema}"."{fritidsboligformal}" ADD COLUMN IF NOT EXISTS kommunenummer_aktuell integer;""")
    cur.execute(f"""UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET kommunenummer_aktuell = kna FROM
                 (SELECT DISTINCT ON (a.poid, b.kommunenummer) a.poid AS id,
                 CAST(b.kommunenummer AS integer) AS kna FROM
"{ognp.active_schema}"."{fritidsboligformal}" AS a,
"{ognp.active_schema}"."kommuner" AS b WHERE ST_Intersects(a.geom, b.geom) ORDER BY a.poid, b.kommunenummer, ST_Area(ST_Intersection(a.geom, b.geom)) DESC) AS x WHERE poid = x.id""")


# Add municipality attributes
with ognp.connection.cursor() as cur:
    cur.execute(f"""ALTER TABLE "{ognp.active_schema}"."{fritidsboligformal}" ADD COLUMN IF NOT EXISTS kommune_landareal_km2 double precision;""")
    cur.execute(f"""UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET kommune_landareal_km2 = landareal_km2 FROM
                 (SELECT CAST(kommunenummer AS integer) AS kna, first(landareal_km2) AS landareal_km2 FROM "{ognp.active_schema}"."kommuner"
                 GROUP BY kna) AS x WHERE kommunenummer_aktuell = kna""")


# Get area uavailable for building
for comb in [("kommuneplaner_faresone", "fare"), ("kommuneplaner_sikringsone", "sikring"), ("kommuneplaner_stoysone", "støy"), ("kommuneplaner_angitthensynsone", "angitthensyn"), ("kommuneplaner_bandleggingsone", "båndlegging"), ("kommuneplaner_restriksjonomrade", "arealbruksrestriksjoner"), ("mdir_naturvernområde", "verneform")]:
    ognp.run_maintenance(comb[0], [("geom", "gist", True), (comb[1], "btree", False)])

with ognp.connection.cursor() as cur:
    column = "geom"
    cur_table = fritidsboligformal
    cur.execute(f"""ALTER TABLE "{ognp.active_schema}"."{cur_table}" RENAME COLUMN {column} TO {column}_old;
    SELECT AddGeometryColumn('{ognp.active_schema}'::varchar, '{cur_table}'::varchar, '{column}'::varchar, 25833, 'MULTIPOLYGON'::varchar, 2);""")
    cur.execute(f"""UPDATE "{ognp.active_schema}"."{cur_table}" SET {column} = CASE WHEN ST_IsValid({column}_old) THEN {column}_old ELSE ST_CollectionExtract(ST_MakeValid({column}_old), 3) END;""")

with ognp.connection.cursor() as cur:
    cur.execute(f"""DROP INDEX IF EXISTS "{ognp.active_schema}_{cur_table}_{column}";""")
ognp.run_maintenance(cur_table,
        [
            ("geom", "gist", True),
        ]
    )

with ognp.connection.cursor() as cur:
     cur.execute(f"""DROP TABLE IF EXISTS "{ognp.active_schema}"."ubebyggbar_areal";
CREATE TABLE "{ognp.active_schema}"."ubebyggbar_areal" AS
SELECT poid, ubebyggbar_geom FROM
(SELECT poid, ST_CollectionExtract(ST_MakeValid(ST_Union(ST_Multi(geom))),3) AS ubebyggbar_geom FROM (
SELECT poid, geom FROM (SELECT b1.poid, ST_Buffer(ST_Intersection(a1.geom_valid, b1.geom), 0) AS geom FROM "{ognp.active_schema}".arealbruk_ssb_ssbarealbrukflate AS a1, "{ognp.active_schema}"."{fritidsboligformal}" AS b1 WHERE ST_Intersects(a1.geom, b1.geom)) AS x1 UNION ALL
SELECT poid, geom FROM (SELECT b2.poid, ST_Buffer(ST_Intersection(a2.geom, b2.geom), 0) AS geom FROM "{ognp.active_schema}"."mdir_naturvernområde" AS a2, "{ognp.active_schema}"."{fritidsboligformal}" AS b2 WHERE ST_Intersects(a2.geom, b2.geom) AND verneform IN ('nasjonalpark', 'naturreservat')) AS x2 UNION ALL
SELECT poid, geom FROM (SELECT b3.poid, ST_Buffer(ST_Intersection(a3.geom, b3.geom), 0) AS geom FROM "{ognp.active_schema}"."n50_arealdekke_omrade" AS a3, "{ognp.active_schema}"."{fritidsboligformal}" AS b3 WHERE ST_Intersects(a3.geom, b3.geom) AND objtype = 'Myr') AS x3 UNION ALL
SELECT poid, geom FROM (SELECT b4.poid, ST_Buffer(ST_Intersection(a4.geom, b4.geom), 0) AS geom FROM "{ognp.active_schema}"."flomsoner_flomareal" AS a4, "{ognp.active_schema}"."{fritidsboligformal}" AS b4 WHERE ST_Intersects(a4.geom, b4.geom) AND "gjentaksinterval" <= 200) AS x4 UNION ALL
SELECT poid, geom FROM (SELECT b5.poid, ST_Buffer(ST_Intersection(a5.geom, b5.geom), 0) AS geom FROM "{ognp.active_schema}"."skredfaresone" AS a5, "{ognp.active_schema}"."{fritidsboligformal}" AS b5  WHERE ST_Intersects(a5.geom, b5.geom) AND "skredstatistikksannsynlighet" IN ('100', '1000')) AS x5 UNION ALL
SELECT poid, geom FROM (SELECT b6.poid, ST_Buffer(ST_Intersection(a6.geom, b6.geom), 0) AS geom FROM "{ognp.active_schema}"."kvikkleire_utlopomr" AS a6, "{ognp.active_schema}"."{fritidsboligformal}" AS b6 WHERE ST_Intersects(a6.geom, b6.geom)) AS x6 UNION ALL
SELECT poid, geom FROM (SELECT b7.poid, ST_Buffer(ST_Intersection(a7.geom, b7.geom), 0) AS geom FROM "{ognp.active_schema}"."kvikkleire_utlosningomr" AS a7, "{ognp.active_schema}"."{fritidsboligformal}" AS b7 WHERE ST_Intersects(a7.geom, b7.geom)) AS x7
) AS a
GROUP BY poid) AS x
WHERE ST_Area(ubebyggbar_geom) IS NOT NULL AND ST_Area(ubebyggbar_geom) > 0;""")
ognp.run_maintenance("ubebyggbar_areal", [("ubebyggbar_geom", "gist", True), ("poid", "btree", True)])


with ognp.connection.cursor() as cur:
     cur.execute(f"""DROP TABLE IF EXISTS "{ognp.active_schema}"."ubebyggbar_og_hensyn_areal";
CREATE TABLE "{ognp.active_schema}"."ubebyggbar_og_hensyn_areal" AS
SELECT poid, ubebyggbar_og_hensyn_geom FROM
(SELECT poid, ST_CollectionExtract(ST_MakeValid(ST_Union(ST_Multi( ST_CollectionExtract(ST_MakeValid(geom), 3)))),3) AS ubebyggbar_og_hensyn_geom FROM (
SELECT poid, geom FROM (SELECT b1.poid, ST_Buffer(ST_Intersection(a1.geom_valid, b1.geom), 0) AS geom FROM "{ognp.active_schema}".arealbruk_ssb_ssbarealbrukflate AS a1, "{ognp.active_schema}"."{fritidsboligformal}" AS b1 WHERE ST_Intersects(a1.geom, b1.geom)) AS x1 UNION ALL
SELECT poid, geom FROM (SELECT b2.poid, ST_Buffer(ST_Intersection(a2.geom, b2.geom), 0) AS geom FROM "{ognp.active_schema}"."mdir_naturvernområde" AS a2, "{ognp.active_schema}"."{fritidsboligformal}" AS b2 WHERE ST_Intersects(a2.geom, b2.geom) AND verneform IN ('nasjonalpark', 'naturreservat')) AS x2 UNION ALL
SELECT poid, geom FROM (SELECT b3.poid, ST_Buffer(ST_Intersection(a3.geom, b3.geom), 0) AS geom FROM "{ognp.active_schema}"."n50_arealdekke_omrade" AS a3, "{ognp.active_schema}"."{fritidsboligformal}" AS b3 WHERE ST_Intersects(a3.geom, b3.geom) AND objtype = 'Myr') AS x3 UNION ALL
SELECT poid, geom FROM (SELECT b4.poid, ST_Buffer(ST_Intersection(a4.geom, b4.geom), 0) AS geom FROM "{ognp.active_schema}"."flomsoner_flomareal" AS a4, "{ognp.active_schema}"."{fritidsboligformal}" AS b4 WHERE ST_Intersects(a4.geom, b4.geom) AND "gjentaksinterval" <= 200) AS x4 UNION ALL
SELECT poid, geom FROM (SELECT b5.poid, ST_Buffer(ST_Intersection(a5.geom, b5.geom), 0) AS geom FROM "{ognp.active_schema}"."skredfaresone" AS a5, "{ognp.active_schema}"."{fritidsboligformal}" AS b5  WHERE ST_Intersects(a5.geom, b5.geom) AND "skredstatistikksannsynlighet" IN ('100', '1000')) AS x5 UNION ALL
SELECT poid, geom FROM (SELECT b6.poid, ST_Buffer(ST_Intersection(a6.geom, b6.geom), 0) AS geom FROM "{ognp.active_schema}"."kvikkleire_utlopomr" AS a6, "{ognp.active_schema}"."{fritidsboligformal}" AS b6 WHERE ST_Intersects(a6.geom, b6.geom)) AS x6 UNION ALL
SELECT poid, geom FROM (SELECT b7.poid, ST_Buffer(ST_Intersection(a7.geom, b7.geom), 0) AS geom FROM "{ognp.active_schema}"."kvikkleire_utlosningomr" AS a7, "{ognp.active_schema}"."{fritidsboligformal}" AS b7 WHERE ST_Intersects(a7.geom, b7.geom)) AS x7 UNION ALL
SELECT poid, geom FROM (SELECT b8.poid, ST_Buffer(ST_Intersection(ST_CollectionExtract(ST_MakeValid(a8.geom), 3), b8.geom), 0) AS geom FROM "{ognp.active_schema}".kommuneplaner_faresone AS a8, "{ognp.active_schema}"."{fritidsboligformal}" AS b8 WHERE ST_Intersects(a8.geom, b8.geom) AND fare IN (310, 320, 350, 360, 380, 390)) AS x8 UNION ALL
SELECT poid, geom FROM (SELECT b9.poid, ST_Buffer(ST_Intersection(ST_CollectionExtract(ST_MakeValid(a9.geom), 3), b9.geom), 0) AS geom FROM "{ognp.active_schema}".kommuneplaner_sikringsone AS a9, "{ognp.active_schema}"."{fritidsboligformal}" AS b9 WHERE ST_Intersects(a9.geom, b9.geom) AND  sikring IN (110, 120, 130, 190)) AS x9 UNION ALL
SELECT poid, geom FROM (SELECT b10.poid, ST_Buffer(ST_Intersection(ST_CollectionExtract(ST_MakeValid(a10.geom), 3), b10.geom), 0) AS geom FROM "{ognp.active_schema}".kommuneplaner_stoysone AS a10, "{ognp.active_schema}"."{fritidsboligformal}" AS b10 WHERE ST_Intersects(a10.geom, b10.geom) AND "støy" = 210) AS x10 UNION ALL
SELECT poid, geom FROM (SELECT b11.poid, ST_Buffer(ST_Intersection(ST_CollectionExtract(ST_MakeValid(a11.geom), 3), b11.geom), 0) AS geom FROM "{ognp.active_schema}".kommuneplaner_angitthensynsone AS a11, "{ognp.active_schema}"."{fritidsboligformal}" AS b11 WHERE ST_Intersects(a11.geom, b11.geom) AND angitthensyn IN (510, 520, 530, 540, 550, 560, 570, 580)) AS x11 UNION ALL
SELECT poid, geom FROM (SELECT b12.poid, ST_Buffer(ST_Intersection(ST_CollectionExtract(ST_MakeValid(a12.geom), 3), b12.geom), 0) AS geom FROM "{ognp.active_schema}".kommuneplaner_bandleggingsone AS a12, "{ognp.active_schema}"."{fritidsboligformal}" AS b12 WHERE ST_Intersects(a12.geom, b12.geom) AND "båndlegging" IN (720, 735, 730, 740, 750)) AS x12 UNION ALL
SELECT poid, geom FROM (SELECT b13.poid, ST_Buffer(ST_Intersection(ST_CollectionExtract(ST_MakeValid(a13.geom), 3), b13.geom), 0) AS geom FROM "{ognp.active_schema}".kommuneplaner_restriksjonomrade AS a13, "{ognp.active_schema}"."{fritidsboligformal}" AS b13 WHERE ST_Intersects(a13.geom, b13.geom) AND arealbruksrestriksjoner IN (135, 136, 410, 411, 420)) AS x13
) AS a
GROUP BY poid) AS x
WHERE ST_Area(ubebyggbar_og_hensyn_geom) IS NOT NULL AND ST_Area(ubebyggbar_og_hensyn_geom) > 0;""")
ognp.run_maintenance("ubebyggbar_og_hensyn_areal", [("ubebyggbar_og_hensyn_geom", "gist", True), ("poid", "btree", True)])

with ognp.connection.cursor() as cur:
    column = "ubebyggbar_areal_m2"
    if column not in ognp.get_column_names(fritidsboligformal):
        cur.execute(f"""ALTER TABLE "{ognp.active_schema}"."{fritidsboligformal}" ADD COLUMN IF NOT EXISTS {column} double precision;""")
    cur.execute(f"""UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET {column} = 0;
UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET {column} = areal FROM
                 (SELECT poid AS id, ST_Area(ubebyggbar_geom) AS areal FROM "{ognp.active_schema}"."ubebyggbar_areal") AS x WHERE poid = x.id""")
    column = "ubebyggbar_og_hensyn_areal_m2"
    if column not in ognp.get_column_names(fritidsboligformal):
        cur.execute(f"""ALTER TABLE "{ognp.active_schema}"."{fritidsboligformal}" ADD COLUMN IF NOT EXISTS {column} double precision;""")
    cur.execute(f"""UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET {column} = 0;
UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET {column} = areal FROM
                 (SELECT poid AS id, ST_Area(ubebyggbar_og_hensyn_geom) AS areal FROM "{ognp.active_schema}"."ubebyggbar_og_hensyn_areal") AS x WHERE poid = x.id""")


# Analyse already build up area
with ognp.connection.cursor() as cur:
    cur.execute(f"""DROP TABLE IF EXISTS "{ognp.active_schema}"."bebygd_areal";
CREATE TABLE "{ognp.active_schema}"."bebygd_areal" AS
SELECT a.poid, ST_CollectionExtract(ST_Intersection(a.geom, ST_Buffer(ST_Collect(b.geom), 10)), 3) AS bebygd_geom
FROM
"{ognp.active_schema}"."{fritidsboligformal}" AS a,
"{ognp.active_schema}"."matrikkelenbygning" AS b
WHERE ST_DWithin(a.geom, b.geom, 10)
GROUP BY a.poid, a.geom;""")
ognp.run_maintenance("bebygd_areal", [("bebygd_geom", "gist", True), ("poid", "btree", True)])

with ognp.connection.cursor() as cur:
    column = "bebygd_areal_m2"
    if column not in ognp.get_column_names(fritidsboligformal):
        cur.execute(f"""ALTER TABLE "{ognp.active_schema}"."{fritidsboligformal}" ADD COLUMN IF NOT EXISTS {column} double precision;""")
    cur.execute(f"""UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET {column} = 0;
UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET {column} = areal FROM
                 (SELECT poid AS id, ST_Area(bebygd_geom) AS areal FROM "{ognp.active_schema}"."bebygd_areal") AS x WHERE poid = x.id""")

with ognp.connection.cursor() as cur:
    cur.execute(f"""DROP TABLE IF EXISTS "{ognp.active_schema}"."utilgjengelig_areal";
CREATE TABLE "{ognp.active_schema}"."utilgjengelig_areal" AS SELECT * FROM (SELECT
poid,
CASE
WHEN (ubebyggbar_geom IS NULL OR ST_IsEmpty(ubebyggbar_geom)) AND bebygd_geom IS NOT NULL THEN bebygd_geom
WHEN ubebyggbar_geom IS NOT NULL AND ST_IsEmpty(ubebyggbar_geom) = FALSE AND (bebygd_geom IS NULL OR ST_IsEmpty(bebygd_geom)) THEN ubebyggbar_geom
WHEN ubebyggbar_geom IS NOT NULL AND ST_IsEmpty(ubebyggbar_geom) = FALSE AND bebygd_geom IS NOT NULL AND ST_IsEmpty(bebygd_geom) = FALSE THEN ST_CollectionExtract(ST_Union(ubebyggbar_geom, bebygd_geom), 3)
ELSE NULL
END AS "utilgjengelig_geom"
FROM
"{ognp.active_schema}"."ubebyggbar_areal" FULL OUTER JOIN
"{ognp.active_schema}"."bebygd_areal"
USING (poid)) AS x WHERE "utilgjengelig_geom" IS NOT NULL AND ST_IsEmpty("utilgjengelig_geom") = FALSE;""")
ognp.run_maintenance("utilgjengelig_areal", [("utilgjengelig_geom", "gist", True), ("poid", "btree", True)])

with ognp.connection.cursor() as cur:
    cur.execute(f"""DROP TABLE IF EXISTS "{ognp.active_schema}"."tilgjengelig_areal";
CREATE TABLE "{ognp.active_schema}"."tilgjengelig_areal" AS SELECT
poid,
CASE
WHEN b."utilgjengelig_geom" IS NULL OR ST_IsEmpty(b."utilgjengelig_geom") THEN geom
WHEN b."utilgjengelig_geom" IS NOT NULL AND ST_IsEmpty(b."utilgjengelig_geom") = FALSE THEN
    ST_Difference(ST_Buffer(a.geom, 0), ST_CollectionExtract(ST_MakeValid(ST_SnaptoGrid(b."utilgjengelig_geom", 0.001)), 3))
END AS "tilgjengelig_geom"
FROM
"{ognp.active_schema}"."{fritidsboligformal}" AS a LEFT JOIN
"{ognp.active_schema}"."utilgjengelig_areal" AS b USING (poid);""")
ognp.run_maintenance("tilgjengelig_areal", [("tilgjengelig_geom", "gist", True), ("poid", "btree", True)])

with ognp.connection.cursor() as cur:
    cur.execute(f"""DROP TABLE IF EXISTS "{ognp.active_schema}"."tilgjengelig_stor_areal";
CREATE TABLE "{ognp.active_schema}"."tilgjengelig_stor_areal" AS SELECT
poid, ST_Buffer(tilgjengelig_geom_neg_buf, 5) AS geom_tilgjengelig_stor FROM
(SELECT poid, ST_Buffer("tilgjengelig_geom", -5) AS tilgjengelig_geom_neg_buf FROM "{ognp.active_schema}"."tilgjengelig_areal") AS x
WHERE NOT ST_IsEmpty(tilgjengelig_geom_neg_buf);""")
ognp.run_maintenance("tilgjengelig_stor_areal", [("geom_tilgjengelig_stor", "gist", True), ("poid", "btree", True)])

with ognp.connection.cursor() as cur:
    column = "tilgjengelig_stor_areal_m2"
    if column not in ognp.get_column_names(fritidsboligformal):
        cur.execute(f"""ALTER TABLE "{ognp.active_schema}"."{fritidsboligformal}" ADD COLUMN IF NOT EXISTS {column} double precision;""")
    cur.execute(f"""UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET {column} = 0;
UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET {column} = areal FROM
                 (SELECT poid AS id, ST_Area(geom_tilgjengelig_stor) AS areal FROM "{ognp.active_schema}"."tilgjengelig_stor_areal") AS x WHERE poid = x.id""")


# Get tomtereserve (areas that can be build upon or have not been built upon)
with ognp.connection.cursor() as cur:
    geom_tr_column = "geom_tomtereserve"
    if geom_tr_column not in ognp.get_column_names(fritidsboligformal):
        cur.execute(f"""SELECT
          AddGeometryColumn('{ognp.active_schema}'::varchar,
                            '{fritidsboligformal}'::varchar,
                            '{geom_tr_column}'::varchar,
                            25833,
                            'MULTIPOLYGON'::varchar,
                            2);""")
    cur.execute(f"""UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET {geom_tr_column} = NULL;
    UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET {geom_tr_column} = ST_Multi(geom_tr) FROM (
    SELECT poid AS id,
    CASE
      WHEN tilgjengelig_stor_areal_m2 = 0 AND antall_bygninger = 0 THEN geom
      WHEN tilgjengelig_stor_areal_m2 > 0 THEN geom_tilgjengelig_stor
    END AS geom_tr
    FROM
      (SELECT poid, geom, tilgjengelig_stor_areal_m2, antall_bygninger FROM "{ognp.active_schema}"."{fritidsboligformal}") AS a
      LEFT JOIN "{ognp.active_schema}"."tilgjengelig_stor_areal" AS b USING (poid)
    ) AS x WHERE poid = x.id;""")
ognp.run_maintenance(fritidsboligformal, [(geom_tr_column, "gist", True)])


with ognp.connection.cursor() as cur:
    column = "tomtereserve_m2"
    if column not in ognp.get_column_names(fritidsboligformal):
        cur.execute(f"""ALTER TABLE "{ognp.active_schema}"."{fritidsboligformal}" ADD COLUMN IF NOT EXISTS {column} integer;""")
    cur.execute(f"""UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET {column} = 0;
UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET {column} = round(ST_Area({geom_tr_column})::numeric) WHERE {geom_tr_column} IS NOT NULL;""")


# Analyse regulated tomtereserve
with ognp.connection.cursor() as cur:
    column = "tomtereserve_regulert_m2"
    cur.execute(f"""ALTER TABLE "{ognp.active_schema}"."{fritidsboligformal}" ADD COLUMN IF NOT EXISTS {column} integer;""")
    cur.execute(f"""UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET {column} = 0;
UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET {column} = round(x.areal::numeric, 0)::integer FROM
                 (SELECT a.poid AS id, ST_Area(ST_Union(ST_CollectionExtract(ST_MAkeValid(ST_SnaptoGrid(ST_Intersection(
                     ST_MAkeValid(ST_SnaptoGrid(a.{geom_tr_column}, 0.001)),
                     ST_MAkeValid((ST_SnaptoGrid(ST_CurveToLine(b.geom), 0.001)))
                 ), 0.001)), 3))) AS areal
                 FROM (SELECT poid, {geom_tr_column} FROM "{ognp.active_schema}"."{fritidsboligformal}" WHERE {geom_tr_column} IS NOT NULL) AS a,
                 (SELECT geom FROM "{ognp.active_schema}"."reguleringsplaner_omrade_guldig_vn2") AS b
                 WHERE ST_Intersects(a.{geom_tr_column}, b.geom)
                 GROUP BY a.poid) AS x
                 WHERE poid = x.id""")


with ognp.connection.cursor() as cur:
    column = "tomtereserve_antall_boliger"
    cur.execute(f"""ALTER TABLE "{ognp.active_schema}"."{fritidsboligformal}" ADD COLUMN IF NOT EXISTS {column} integer;""")
    cur.execute(f"""UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET {column} = 0;
UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET {column} = CASE
  WHEN areal_m2 <= 2000 AND CAST((tomtereserve_m2 * 1.0) / 1000.0 AS integer) = 0 AND
       (antall_bygninger::double precision / (areal_m2 / 1000.0)) <= 0.5 THEN CAST(round(((areal_m2 * (1.0 + antall_bygninger)) / 1000.0)::numeric, 0) AS integer)
  WHEN areal_m2 <= 2000 THEN CAST((tomtereserve_m2 * 1.0) / 1000.0 AS integer)
  WHEN areal_m2 <= 2000 THEN CAST((tomtereserve_m2 * 1.0) / 1000.0 AS integer)
  WHEN areal_m2 > 50000 THEN CAST((tomtereserve_m2 * 0.5) / 1000.0 AS integer)
  ELSE CAST((tomtereserve_m2 * 0.75) / 1000.0 AS integer)
END::integer
WHERE tomtereserve_m2 > 0;""")


# # Analyse overlap with regulation plans
# with ognp.connection.cursor() as cur:
#     cur.execute(f"""ALTER TABLE "{ognp.active_schema}"."{fritidsboligformal}" ADD COLUMN IF NOT EXISTS tomtereserve_regulert_m2 integer;""")
#     cur.execute(f"""ALTER TABLE "{ognp.active_schema}"."{fritidsboligformal}" ADD COLUMN IF NOT EXISTS tomtereserve_uregulert_m2 integer;""")
#     cur.execute(f"""UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET tomtereserve_regulert_m2 = 0, tomtereserve_uregulert_m2 = 0;
# UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET tomtereserve_regulert_m2 = regulert, tomtereserve_uregulert_m2 = uregulert
#   FROM
#   (SELECT
#     poid AS id,
#     CASE
#       WHEN tilgjengelig_stor_areal_m2 = 0 AND antall_bygninger = 0 THEN round(areal_2::numeric, 0)::integer
#       WHEN tilgjengelig_stor_areal_m2 > 0 THEN round(areal::numeric, 0)::integer
#     END AS regulert,
#     CASE
#       WHEN tilgjengelig_stor_areal_m2 = 0 AND antall_bygninger = 0 THEN round((areal_m2 - areal)::numeric, 0)::integer
#       WHEN tilgjengelig_stor_areal_m2 > 0 THEN round((tilgjengelig_stor_areal_m2-areal)::numeric, 0)::integer)
#     END AS uregulert
#     FROM
#       (SELECT
#           a.poid,
#           sum(ST_Area(ST_Intersection(a.geom, b."geom_tilgjengelig_stor"))) AS areal
#       FROM
#         "{ognp.active_schema}"."tilgjengelig_stor_areal" AS a,
#         (SELECT geom FROM "{ognp.active_schema}"."reguleringsplaner_omrade_guldig_vn2") AS b
#       WHERE ST_Intersects(a.geom, b."geom")
#       GROUP BY a.poid) AS ab
#     GROUP BY a.poid) AS x
#     WHERE poid = x.id AND plankategori != 'Reguleringsplan'
# WHERE tilgjengelig_stor_areal_m2 > 0 OR antall_bygninger = 0;""")
#

# Analyse overlap with NiN landskap
with ognp.connection.cursor() as cur:
    if "landskap" not in ognp.get_column_names(fritidsboligformal):
        cur.execute(f"""ALTER TABLE "{ognp.active_schema}"."{fritidsboligformal}" ADD COLUMN landskap text;""")
    cur.execute(f"""UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET landskap = "grunntypenavn" FROM
                 (SELECT DISTINCT ON (id) id, "grunntypenavn" FROM
                   (SELECT a.poid AS id, b."grunntypenavn", ST_Area(ST_Union(ST_Intersection(a.geom, b.geom))) AS areal
                   FROM
                   "{ognp.active_schema}"."{fritidsboligformal}" AS a,
                   "{ognp.active_schema}"."nin_landskap" AS b
                 WHERE ST_Intersects(a.geom, b.geom) GROUP BY a.poid, b."grunntypenavn"
                 ) AS y ORDER BY id, areal DESC) AS x WHERE poid = x.id""")
print("Done with landskap")

# Analyse overlap with mires
with ognp.connection.cursor() as cur:
    if "myr_areal_m2" not in ognp.get_column_names(fritidsboligformal):
        cur.execute(f"""ALTER TABLE "{ognp.active_schema}"."{fritidsboligformal}" ADD COLUMN myr_areal_m2 double precision;""")
    cur.execute(f"""UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET myr_areal_m2 = 0;
                 UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET myr_areal_m2 = x.areal FROM
                 (SELECT a.poid AS id, ST_Area(ST_Union(ST_Intersection(a.geom, b.geom))) AS areal FROM "{ognp.active_schema}"."{fritidsboligformal}" AS a, "{ognp.active_schema}"."n50_arealdekke_omrade" AS b WHERE ST_Intersects(a.geom, b.geom) AND b.objtype = 'Myr' GROUP BY a.poid) AS x WHERE poid = x.id""")
print("Done with myr")


# Analyse overlap with mountains
with ognp.connection.cursor() as cur:
    # cur.execute(f"""CREATE TABLE "{ognp.active_schema}"."naturindeks_2010_lavalpin_mellomalpin_hoeyalpin" AS TABLE "naturindeks_2010_utm33n"."naturindeks_2010_lavalpin_mellomalpin_hoeyalpin";""")
    if "fjell_areal_m2" not in ognp.get_column_names(fritidsboligformal):
        cur.execute(f"""ALTER TABLE "{ognp.active_schema}"."{fritidsboligformal}" ADD COLUMN fjell_areal_m2 double precision;""")
    cur.execute(f"""UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET fjell_areal_m2 = 0;
                 UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET fjell_areal_m2 = x.areal FROM
                 (SELECT a.poid AS id, ST_Area(ST_Union(ST_Intersection(a.geom, b.geom))) AS areal FROM "{ognp.active_schema}"."{fritidsboligformal}" AS a, "{ognp.active_schema}"."naturindeks_2010_lavalpin_mellomalpin_hoeyalpin" AS b WHERE ST_Intersects(a.geom, b.geom) GROUP BY a.poid) AS x WHERE poid = x.id""")
print("Done with mountains")


# Analyse overlap with costal regulations
with ognp.connection.cursor() as cur:
    for cat in [1,2,3]:
        if f"kystsone_{cat}_m2" not in ognp.get_column_names(fritidsboligformal):
            cur.execute(f"""ALTER TABLE "{ognp.active_schema}"."{fritidsboligformal}" ADD COLUMN kystsone_{cat}_m2 double precision;""")
        cur.execute(f"""UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET kystsone_{cat}_m2 = 0;
UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET kystsone_{cat}_m2 = x.areal FROM
                 (SELECT a.poid AS id, ST_Area(ST_Union(ST_Intersection(a.geom, b.geom))) AS areal FROM "{ognp.active_schema}"."{fritidsboligformal}" AS a, "{ognp.active_schema}"."planrettningslinjer_strandsonen_rpromrade" AS b WHERE ST_Intersects(a.geom, b.geom) AND b."planretningslinjerstrandsone" = {cat} GROUP BY a.poid) AS x WHERE poid = x.id""")
    if f"kystsone_m2" not in ognp.get_column_names(fritidsboligformal):
        cur.execute(f"""ALTER TABLE "{ognp.active_schema}"."{fritidsboligformal}" ADD COLUMN kystsone_m2 double precision;""")
    cur.execute(f"""UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET kystsone_m2 = 0;
UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET kystsone_m2 = kystsone_1_m2 + kystsone_2_m2 + kystsone_3_m2;""")
print("Done with kystsone")


# Analyse distance to coastline
with ognp.connection.cursor() as cur:
    column = "avstand_kystkontur_m"
    if column not in ognp.get_column_names(fritidsboligformal):
        cur.execute(f"""ALTER TABLE "{ognp.active_schema}"."{fritidsboligformal}" ADD COLUMN IF NOT EXISTS {column} integer;""")
    cur.execute(f"""UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET {column} = 10001;
 UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET {column} = distance FROM (SELECT DISTINCT ON (a.poid) a.poid AS fid, ST_Distance(a.geom, b.geom) AS distance FROM
 "{ognp.active_schema}"."{fritidsboligformal}" AS a,
 "{ognp.active_schema}"."n50_arealdekke_grense" AS b WHERE b.objtype = 'Kystkontur' AND ST_DWithin(a.geom, b.geom, 10000)
 ORDER BY a.poid, distance
 ) AS y WHERE poid = fid;""")
print("Done with kystlinje")


# Analyse overlap with existing protection
with ognp.connection.cursor() as cur:
    column = "naturvern_m2"
    if column not in ognp.get_column_names(fritidsboligformal):
        cur.execute(f"""ALTER TABLE "{ognp.active_schema}"."{fritidsboligformal}" ADD COLUMN IF NOT EXISTS {column} double precision;""")
    cur.execute(f"""UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET {column} = 0;
                 UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET {column} = x.areal FROM
                 (SELECT a.poid AS id, ST_Area(ST_Union(ST_Intersection(a.geom, b.geom))) AS areal FROM "{ognp.active_schema}"."{fritidsboligformal}" AS a, "{ognp.active_schema}"."mdir_naturvernområde" AS b WHERE ST_Intersects(a.geom, b.geom) GROUP BY a.poid) AS x WHERE poid = x.id""")
    column = "naturvern_streng_m2"
    if column not in ognp.get_column_names(fritidsboligformal):
        cur.execute(f"""ALTER TABLE "{ognp.active_schema}"."{fritidsboligformal}" ADD COLUMN IF NOT EXISTS {column} double precision;""")
    cur.execute(f"""UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET {column} = 0;
                 UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET {column} = x.areal FROM
                 (SELECT a.poid AS id, ST_Area(ST_Union(ST_Intersection(a.geom, b.geom))) AS areal FROM "{ognp.active_schema}"."{fritidsboligformal}" AS a, "{ognp.active_schema}"."mdir_naturvernområde" AS b WHERE ST_Intersects(a.geom, b.geom) AND verneform IN ('nasjonalpark', 'naturreservat') GROUP BY a.poid) AS x WHERE poid = x.id""")
print("Done with vern")


# Analyse overlap with proposed protection
with ognp.connection.cursor() as cur:
    column = "naturvern_foreslatt_m2"
    if column not in ognp.get_column_names(fritidsboligformal):
        cur.execute(f"""ALTER TABLE "{ognp.active_schema}"."{fritidsboligformal}" ADD COLUMN IF NOT EXISTS {column} double precision;""")
    cur.execute(f"""UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET {column} = 0;
                 UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET {column} = x.areal FROM
                 (SELECT a.poid AS id, ST_Area(ST_Union(ST_Intersection(a.geom, b.geom))) AS areal FROM "{ognp.active_schema}"."{fritidsboligformal}" AS a, "{ognp.active_schema}"."mdir_foreslattvern_norge_med_svalbard" AS b WHERE ST_Intersects(a.geom, b.geom) GROUP BY a.poid) AS x WHERE poid = x.id""")
    column = "naturvern_foreslatt_streng_m2"
    if column not in ognp.get_column_names(fritidsboligformal):
        cur.execute(f"""ALTER TABLE "{ognp.active_schema}"."{fritidsboligformal}" ADD COLUMN IF NOT EXISTS {column} double precision;""")
    cur.execute(f"""UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET {column} = 0;
                 UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET {column} = x.areal FROM
                 (SELECT a.poid AS id, ST_Area(ST_Union(ST_Intersection(a.geom, b.geom))) AS areal FROM "{ognp.active_schema}"."{fritidsboligformal}" AS a, "{ognp.active_schema}"."mdir_foreslattvern_norge_med_svalbard" AS b WHERE ST_Intersects(a.geom, b.geom) AND verneform IN ('nasjonalpark', 'naturreservat') GROUP BY a.poid) AS x WHERE poid = x.id""")
print("Done with foreslått vern")


# Analyse overlap with mapped habitats
with ognp.connection.cursor() as cur:
    column = "naturtyper_m2"
    if column not in ognp.get_column_names(fritidsboligformal):
        cur.execute(f"""ALTER TABLE "{ognp.active_schema}"."{fritidsboligformal}" ADD COLUMN IF NOT EXISTS {column} double precision;""")
    cur.execute(f"""UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET {column} = 0;
                 UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET {column} = x.areal FROM
                 (SELECT a.poid AS id, ST_Area(ST_Union(ST_Intersection(a.geom, b.geom))) AS areal FROM "{ognp.active_schema}"."{fritidsboligformal}" AS a,
(SELECT geom FROM "{ognp.active_schema}"."mdir_naturtyper_hb13_norge_med_svalbard" UNION ALL
SELECT geom FROM "{ognp.active_schema}"."mdir_naturtyper_nin_norge_med_svalbard"
) AS b WHERE ST_Intersects(a.geom, b.geom) GROUP BY a.poid) AS x WHERE poid = x.id""")
    column = "naturtyper_hoy_verdi_m2"
    if column not in ognp.get_column_names(fritidsboligformal):
        cur.execute(f"""ALTER TABLE "{ognp.active_schema}"."{fritidsboligformal}" ADD COLUMN IF NOT EXISTS {column} double precision;""")
    cur.execute(f"""UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET {column} = 0;
                 UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET {column} = x.areal FROM
                 (SELECT a.poid AS id, ST_Area(ST_Union(ST_Intersection(a.geom, b.geom))) AS areal FROM "{ognp.active_schema}"."{fritidsboligformal}" AS a,
(SELECT geom FROM "{ognp.active_schema}"."mdir_naturtyper_nin_norge_med_svalbard" WHERE lokalitetskvalitet IN ('Høy kvalitet', 'Svært høy kvalitet') UNION ALL
SELECT geom FROM "{ognp.active_schema}"."mdir_naturtyper_hb13_norge_med_svalbard" WHERE bmverdi = 'A' OR utvalgtnaturtype IS NOT NULL
) AS b WHERE ST_Intersects(a.geom, b.geom) GROUP BY a.poid) AS x WHERE poid = x.id""")
print("Done with naturtyper")


# Analyse overlap with domestic reindeer
with ognp.connection.cursor() as cur:
    # Funksjonsomrader
    column = "reindrift_funksjonsomrader_m2"
    if column not in ognp.get_column_names(fritidsboligformal):
        cur.execute(f"""ALTER TABLE "{ognp.active_schema}"."{fritidsboligformal}" ADD COLUMN IF NOT EXISTS {column} double precision;""")
    cur.execute(f"""UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET {column} = 0;
                 UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET {column} = x.areal FROM
                 (SELECT a.poid AS id, ST_Area(ST_Union(ST_Intersection(a.geom, b.geom))) AS areal FROM "{ognp.active_schema}"."{fritidsboligformal}" AS a, (SELECT geom FROM "{ognp.active_schema}"."reindrift_reindrift_beitehage_flate" UNION ALL
SELECT geom FROM "{ognp.active_schema}"."reindrift_reindrift_flyttlei_flate" UNION ALL
SELECT geom FROM "{ognp.active_schema}"."reindrift_reindrift_oppsamlingsomrade_flate" UNION ALL
SELECT ST_Buffer(geom, 500) AS geom FROM "{ognp.active_schema}"."reindrift_reindrift_reindriftsanlegg_punkt" UNION ALL
SELECT ST_Buffer(geom, 500) AS geom FROM "{ognp.active_schema}"."reindrift_reindrift_trekklei_linje") AS b WHERE ST_Intersects(a.geom, b.geom) GROUP BY a.poid) AS x WHERE poid = x.id""")
print("Funksjonsomrader reindrift done!")

with ognp.connection.cursor() as cur:
    # Adminomrader
    column = "reindrift_adminomrader_m2"
    if column not in ognp.get_column_names(fritidsboligformal):
        cur.execute(f"""ALTER TABLE "{ognp.active_schema}"."{fritidsboligformal}" ADD COLUMN IF NOT EXISTS {column} double precision;""")
    cur.execute(f"""UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET {column} = 0;
                 UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET {column} = x.areal FROM
                 (SELECT a.poid AS id, ST_Area(ST_Union(ST_Intersection(a.geom, b.geom))) AS areal FROM "{ognp.active_schema}"."{fritidsboligformal}" AS a, (SELECT geom FROM "{ognp.active_schema}"."reindrift_reindrift_reinbeitedistrikt_flate" UNION ALL
SELECT geom FROM "{ognp.active_schema}"."reindrift_reindrift_konsesjonsomrade_flate") AS b WHERE ST_Intersects(a.geom, b.geom) GROUP BY a.poid) AS x WHERE poid = x.id""")
print("Adminomrader reindrift done!")


# Analyse overlap with wild reindeer
with ognp.connection.cursor() as cur:
    column = "villrein_m2"
    if column not in ognp.get_column_names(fritidsboligformal):
        cur.execute(f"""ALTER TABLE "{ognp.active_schema}"."{fritidsboligformal}" ADD COLUMN IF NOT EXISTS {column} double precision;""")
    cur.execute(f"""UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET {column} = 0;
                 UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET {column} = x.areal FROM
                 (SELECT a.poid AS id, ST_Area(ST_Union(ST_Intersection(a.geom, b.geom))) AS areal FROM "{ognp.active_schema}"."{fritidsboligformal}" AS a, "{ognp.active_schema}"."mdir_villrein_norge_med_svalbard" AS b WHERE ST_Intersects(a.geom, b.geom) GROUP BY a.poid) AS x WHERE poid = x.id""")
    column = "villrein_helar_m2"
    if column not in ognp.get_column_names(fritidsboligformal):
        cur.execute(f"""ALTER TABLE "{ognp.active_schema}"."{fritidsboligformal}" ADD COLUMN IF NOT EXISTS {column} double precision;""")
    cur.execute(f"""UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET {column} = 0;
                 UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET {column} = x.areal FROM
                 (SELECT a.poid AS id, ST_Area(ST_Union(ST_Intersection(a.geom, b.geom))) AS areal FROM "{ognp.active_schema}"."{fritidsboligformal}" AS a, "{ognp.active_schema}"."mdir_villrein_norge_med_svalbard" AS b WHERE ST_Intersects(a.geom, b.geom) AND funksjonsperiode = 'heleÅret' GROUP BY a.poid) AS x WHERE poid = x.id""")
print("Done with villrein")


# Analyse accessibility

# Split road network at tettsted borders (adds new vertices)
with ognp.connection.cursor() as cur:
     cur.execute(f"""DROP TABLE IF EXISTS "{ognp.active_schema}"."tettsteder2021_exteriorring";
CREATE TABLE "{ognp.active_schema}"."tettsteder2021_exteriorring" AS SELECT
  CAST(tettstednummer AS smallint) AS gid
, tettstednavn
, totalbefolkning
, ST_Collect(geom) AS geom
  FROM (
  SELECT
    ST_ExteriorRing((ST_Dump(geom)).geom) AS geom
    , tettstednavn
    , tettstednummer
    , totalbefolkning
  FROM
    "{ognp.active_schema}".tettsteder2021_tettsted) AS x
  GROUP BY tettstednavn
    , tettstednummer
    , totalbefolkning;""")
ognp.run_maintenance("tettsteder2021_exteriorring", [("geom", "gist", True), ("gid", "btree", False),
])

# Split Network at road at tettsted boundary to get entry points
subprocess.run(["python3", "clean_NVDB.py"])

with ognp.connection.cursor() as cur:
     cur.execute(f"""DROP TABLE IF EXISTS "{ognp.active_schema}"."nvdb_nettverk_tettsteder";
CREATE TABLE "{ognp.active_schema}"."nvdb_nettverk_tettsteder" AS SELECT
  ogc_fid AS id
, ST_SetSRID(wkb_geometry, 25833) AS the_geom
, CAST(NULL AS integer) AS source
, CAST(NULL AS integer) AS target
, CASE
  WHEN meter_org - ST_Length(wkb_geometry) > 0 THEN (ST_Length(wkb_geometry) / meter_org) * tf_minutes
  ELSE tf_minutes
  END AS cost
, vegtype
FROM
 "{ognp.active_schema}"."nvdb_nettverk_fritidsbolig";""")

ognp.run_maintenance("nvdb_nettverk_tettsteder", [("the_geom", "gist", True), ("id", "btree", False), ("source", "btree", False), ("target", "btree", False), ("cost", "btree", False), ("vegtype", "btree", False),
])

with ognp.connection.cursor() as cur:
    cur.execute(f"""SELECT pgr_createTopology('{ognp.active_schema}.nvdb_nettverk_tettsteder'::text, 0.00001, the_geom:='the_geom'::text, clean:=TRUE);""")

# Create table with target vertices for distance analyses
with ognp.connection.cursor() as cur:
     cur.execute(f"""DROP TABLE IF EXISTS "{ognp.active_schema}"."nvdb_nettverk_tettsteder_innfart";
CREATE TABLE "{ognp.active_schema}"."nvdb_nettverk_tettsteder_innfart" AS SELECT
b.id
, a.gid AS tettstednummer
, a.tettstednavn
, a.totalbefolkning
FROM "{ognp.active_schema}"."tettsteder2021_exteriorring" AS a,
"{ognp.active_schema}"."nvdb_nettverk_tettsteder_vertices_pgr" AS b
WHERE ST_DWithin(a.geom, b.the_geom, 0.01);""")
ognp.run_maintenance("nvdb_nettverk_tettsteder_innfart", [("id", "btree", True), ("tettstednummer", "btree", False), ("totalbefolkning", "btree", False),
])

# with ognp.connection.cursor() as cur:
#     if "innfart_type" not in ognp.get_column_names("nvdb_nettverk_tettsteder_innfart"):
#         cur.execute(f"""ALTER TABLE "{ognp.active_schema}"."nvdb_nettverk_tettsteder_innfart" ADD COLUMN "innfart_type" smallint;""")
#     cur.execute(f"""UPDATE "{ognp.active_schema}"."nvdb_nettverk_tettsteder_innfart" AS a SET "innfart_type" = x.vegtype FROM
#   (SELECT CAST(CASE WHEN "vegtype" IN ('E', 'R', 'F') THEN 1 ELSE 0 END AS smallint) AS vegtype, target FROM "{ognp.active_schema}"."nvdb_nettverk_tettsteder") AS x WHERE x.target = a.id;""")
#
# # Remove minor entry points
# with ognp.connection.cursor() as cur:
#     cur.execute(f"""DELETE FROM "{ognp.active_schema}"."nvdb_nettverk_tettsteder_innfart" WHERE id IN (SELECT a.id FROM "{ognp.active_schema}"."nvdb_nettverk_tettsteder_innfart" AS a,  (SELECT "tettstednummer", max("innfart_type") AS max_innfart FROM "{ognp.active_schema}"."nvdb_nettverk_tettsteder_innfart" GROUP BY "tettstednummer") AS b WHERE a."tettstednummer" = b."tettstednummer" AND a."innfart_type" < max_innfart);""")
#     cur.execute(f"""VACUUM FULL ANALYZE "{ognp.active_schema}"."nvdb_nettverk_tettsteder_innfart";""")

# Get closest point on road network for area
with ognp.connection.cursor() as cur:
     cur.execute(f"""DROP TABLE IF EXISTS "{ognp.active_schema}"."nvdb_nettverk_fritidsbolig_connection";
CREATE TABLE "{ognp.active_schema}"."nvdb_nettverk_fritidsbolig_connection" AS SELECT
  a.poid
, x.id
, ST_MakeLine(a.geom_centroid, x.the_geom) AS connection_line
, x.distance
FROM
  "{ognp.active_schema}"."{fritidsboligformal}" AS a
CROSS JOIN LATERAL (
  SELECT
    id, b.the_geom, a.geom_centroid <-> b.the_geom AS distance
  FROM
    "{ognp.active_schema}"."nvdb_nettverk_tettsteder_vertices_pgr" AS b
  ORDER BY distance
  LIMIT 1
  )  AS x;""")
ognp.run_maintenance("nvdb_nettverk_fritidsbolig_connection", [("id", "btree", True), ("poid", "btree", False), ("connection_line", "gist", False), ("distance", "btree", False),
])


# Get closest point on road network for area
with ognp.connection.cursor() as cur:
    column = "avstand_vei_m"
    if column not in ognp.get_column_names(fritidsboligformal):
        cur.execute(f"""ALTER TABLE "{ognp.active_schema}"."{fritidsboligformal}" ADD COLUMN IF NOT EXISTS {column} double precision;""")
    cur.execute(f"""UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET {column} = 0;
                 UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET {column} = x.distance FROM
                   (SELECT a.poid AS id, a.geom_centroid, distance FROM "{ognp.active_schema}"."{fritidsboligformal}" AS a
                 CROSS JOIN LATERAL (
                   SELECT
                     a.poid AS id, a.geom_centroid <-> b.the_geom AS distance
                   FROM
                     "{ognp.active_schema}"."nvdb_nettverk_tettsteder_vertices_pgr" AS b
                   ORDER BY distance
                   LIMIT 1
                   ) AS y)  AS x WHERE x.id = poid;""")


with ognp.connection.cursor() as cur:
    cur.execute(f"""SELECT array_agg(DISTINCT id) FROM "{ognp.active_schema}"."nvdb_nettverk_fritidsbolig_connection";""")
    vids = cur.fetchone()[0]

with ognp.connection.cursor() as cur:
     cur.execute(f"""DROP TABLE IF EXISTS "{ognp.active_schema}"."nvdb_nettverk_kjoereavstand_all";
    CREATE TABLE "{ognp.active_schema}"."nvdb_nettverk_kjoereavstand_all"(poid integer, tettstednummer smallint, totalbefolkning integer, avstand smallint);""")

def get_distance_matrix(vids, max_cost=240, connection_string=None):
    with psycopg2.connect(connection_string) as con:
        con.set_session(autocommit=True)
        with con.cursor() as cur:
            for idx, vid in enumerate(vids):
                print(f"Processing {vid}, {round((idx / len(vids)) * 100.0, 0)}")
                cur.execute(f"""INSERT INTO "{ognp.active_schema}"."nvdb_nettverk_kjoereavstand_all"
    SELECT DISTINCT ON (b.poid, c.tettstednummer) b.poid, c.tettstednummer, c.totalbefolkning, a.avstand FROM (SELECT start_vid, end_vid, CAST(round(agg_cost) AS smallint) AS avstand
    FROM pgr_dijkstraCost(
      'select id, source, target, cost from "{ognp.active_schema}"."nvdb_nettverk_tettsteder"'
      , {vid}
      , (SELECT array_agg(DISTINCT id) FROM "{ognp.active_schema}"."nvdb_nettverk_tettsteder_innfart")
      , False)
    WHERE agg_cost <= {max_cost}) AS a INNER JOIN
      "{ognp.active_schema}"."nvdb_nettverk_fritidsbolig_connection" AS b ON a.start_vid = b.id INNER JOIN
      "{ognp.active_schema}"."nvdb_nettverk_tettsteder_innfart" AS c ON a.end_vid = c.id
    ORDER BY b.poid, c.tettstednummer, a.avstand;""")


#with ognp.connection.cursor() as cur:
#    cur.execute(f"""SELECT DISTINCT ON (b.poid, c.tettstednummer) b.poid, c.tettstednummer, c.totalbefolkning, a.avstand FROM (SELECT start_vid, end_vid, CAST(round(agg_cost) AS smallint) AS avstand
#    FROM pgr_dijkstraCost(
#      'select id, source, target, cost from "{ognp.active_schema}"."nvdb_nettverk_tettsteder"'
#      , {vid}
#      , (SELECT array_agg(DISTINCT id) FROM "{ognp.active_schema}"."nvdb_nettverk_tettsteder_innfart")
#      , False)
#    WHERE agg_cost <= {max_cost}) AS a LEFT JOIN
#      "{ognp.active_schema}"."nvdb_nettverk_fritidsbolig_connection" AS b ON a.start_vid = b.id LEFT JOIN
#      "{ognp.active_schema}"."nvdb_nettverk_tettsteder_innfart" AS c ON a.end_vid = c.id
#    ORDER BY b.poid, c.tettstednummer, a.avstand;""")
#    res = cur.fetchall()

get_distance_matrix_parallel = partial(get_distance_matrix, connection_string=ognp.connection_string)

cores = 20
with Pool(cores) as p:
    p.map(get_distance_matrix_parallel, np.array_split(vids, cores))

with ognp.connection.cursor() as cur:
    for hours in [2,3,4]:
        column = f"befolkning_{hours}h"
        if column not in ognp.get_column_names(fritidsboligformal):
            cur.execute(f"""ALTER TABLE "{ognp.active_schema}"."{fritidsboligformal}" ADD COLUMN IF NOT EXISTS {column} integer;""")
        cur.execute(f"""UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET {column} = 0;
        UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET {column} = befolkning
        FROM (SELECT poid AS fid, sum(totalbefolkning) AS befolkning
        FROM "{ognp.active_schema}"."nvdb_nettverk_kjoereavstand_all" WHERE avstand <= ({hours} * 60) GROUP BY poid) AS x WHERE poid = fid;""")
        column = f"kjoereavstand_avg_{hours}h"
        if column not in ognp.get_column_names(fritidsboligformal):
            cur.execute(f"""ALTER TABLE "{ognp.active_schema}"."{fritidsboligformal}" ADD COLUMN IF NOT EXISTS {column} double precision;""")
        cur.execute(f"""UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET {column} = avstand_avg FROM (SELECT poid AS fid, sum(avstand * totalbefolkning) / sum(totalbefolkning) AS avstand_avg FROM "{ognp.active_schema}"."nvdb_nettverk_kjoereavstand_all" WHERE avstand <= ({hours} * 60) GROUP BY poid) AS x WHERE poid = fid;""")
        column = "kjoereavstand_naermeste_tettsted_minutes"
        if column not in ognp.get_column_names(fritidsboligformal):
            cur.execute(f"""ALTER TABLE "{ognp.active_schema}"."{fritidsboligformal}" ADD COLUMN IF NOT EXISTS {column} double precision;""")
        cur.execute(f"""UPDATE "{ognp.active_schema}"."{fritidsboligformal}" SET {column} = avstand FROM (SELECT DISTINCT ON (poid) poid AS fid, avstand FROM "{ognp.active_schema}"."nvdb_nettverk_kjoereavstand_all" ORDER BY poid, avstand ASC) AS x WHERE poid = fid;""")



# By cabin
with ognp.connection.cursor() as cur:
    cur.execute(f"""DROP TABLE IF EXISTS "{ognp.active_schema}"."fritidsbygg_vs_plan_alle";
CREATE TABLE "{ognp.active_schema}"."fritidsbygg_vs_plan_alle" AS SELECT
y.*
, x.gid AS bygningsid
, x.bygningstype
, x.bygningsstatus
, x.centroid AS bygningcentroid
, x.datafangstdato AS bygningdatafangstdato
FROM
  "{ognp.active_schema}"."{fritidsbygg}" AS x LEFT JOIN
  "{ognp.active_schema}"."{planformalomrader}" AS y ON ST_DWithin(y.geom, x.centroid, 0);""")

ognp.run_maintenance("fritidsbygg_vs_plan_alle", [
    ("bygningcentroid", "GIST", False),
    ("kommunenummer", "btree", True),
    ("arealformal", "btree", False),
    ])


# Velg uregulerte områder med arealformål fritidsbolig som er uten bygg
# Velg områder som er regulert for fritidsbolig som er uten bygg


# Velg eldre, regulerte og uregulerte områder med arealformål fritidsbolig og ta univariat statistikk av tetthet
# Velg områder som har x ganger lavere tetthet




























p_cols = ", ".join([f"po.{col}" for col in columns_planomrade])

union_sql.append(f"""SELECT po.*, o.description AS arealformal_tekst, s.description AS planstatus_tekst FROM (SELECT x.*, p.omrade AS planomrade, {p_cols} FROM "{ognp.active_schema}"."rparealformalomrade_omrade" AS x,
(SELECT DISTINCT ON (omrade, kommunenummer, planidentifikasjon, vertikalniva, plantype) * FROM "{ognp.active_schema}"."kommuneplaner_omrade" WHERE planstatus NOT IN ({invalid_planstatus}) ORDER BY omrade, kommunenummer, planidentifikasjon, vertikalniva, plantype, ikrafttredelsesdato DESC, vedtakendeligplandato DESC) AS p
WHERE x.kommunenummer = p.kommunenummer
AND x.planidentifikasjon = p.planidentifikasjon
AND x.vertikalniva = p.vertikalniva
AND ST_Intersects(x.omrade, p.omrade)
) AS po""")
union_sql = " UNION ALL ".join(union_sql)



with psycopg2.connect(connection_string) as con:
    con.set_session(autocommit=True)
    with con.cursor() as cur:
        run_maintenance(con, ognp.active_schema, "kommuneplaner_arealformalomrade", [("geom", "GIST", True), ("kommunenummer", "btree", False), ("planidentifikasjon", "btree", False), ("arealformål", "btree", False)])
        run_maintenance(con, ognp.active_schema, "kommuneplaner_arealbrukomrade", [("geom", "GIST", True), ("kommunenummer", "btree", False), ("planidentifikasjon", "btree", False), ("arealbruk", "btree", False)])
        run_maintenance(con, ognp.active_schema, "kommuneplaner_omrade", [("geom", "GIST", True), ("kommunenummer", "btree", False), ("planidentifikasjon", "btree", False)])












planformalomrader = "reguleringsplaner_rpomrader_samlet"
kp_samlet = "kommuneplaner_samlet"

with psycopg2.connect(connection_string) as con:
    con.set_session(autocommit=True)
    with con.cursor() as cur:
        cur.execute(f"""DROP TABLE IF EXISTS "{ognp.active_schema}"."fritidsbygg_vs_plan_alle";
CREATE TABLE "{ognp.active_schema}"."fritidsbygg_vs_plan_alle" AS SELECT
y.kommunenummer
, x.gid AS bygningsid
, x.bygningstype
, x.bygningsstatus
, x.centroid AS bygningcentroid
, x.verifiseringsdato AS bygningverifiseringsdato
, y.kommunenummer || '_' || y.planidentifikasjon || '_' || y.vertikalniva AS reguleringsplan_id
, y.omrade AS reguleringsplan_omrade
, y.arealformal_tekst AS reguleringsplan_arealformal
, y.ikrafttredelsesdato AS reguleringsplan_ikrafttredelsesdato
, y.vedtakendeligplandato AS reguleringsplan_vedtakendeligplandato
, y.plannavn AS reguleringsplan_plannavn
, y.beskrivelse AS reguleringsplan_beskrivelse
, y.utnyttingstall AS reguleringsplan_utnyttingstall
, y.utnyttingstall_minimum AS reguleringsplan_utnyttingstall_minimum
, y.plantype AS reguleringsplan_plantype
, y.planstatus_tekst AS reguleringsplan_planststus
, y.vertikalniva AS reguleringsplan_vertikalniva
, y.lovreferanse AS reguleringsplan_lovreferanse
, k.gid AS kommuneplan_id
, k.arealformål AS kommuneplan_arealformal
, k.arealbruksstatus AS kommuneplan_arealbruksstatus
, k.objekttypenavn AS kommuneplan_objekttypenavn
, k.områdeid AS kommuneplan_omradeid
, k.områdenavn AS kommuneplan_omradenavn
, k.planidentifikasjon
, k.utnyttingstall
, k.utnyttingstype
, k.geom AS kommuneplan_omrade
, k.planomrade
, k.plannavn
, k.plantype
, k.planstatus
, k.planbestemmelse
, k.ikrafttredelsesdato AS kommuneplan_ikrafttredelsesdato
, k.vedtakendeligplandato
, k.kunngjoringsdato
, k.lovreferansebeskrivelse
, k.opprinneligadministrativenhet
FROM "{ognp.active_schema}"."{fritidsbygg}" AS x LEFT JOIN "{ognp.active_schema}"."{planformalomrader}" AS y ON ST_DWithin(y.omrade, x.centroid, 0) LEFT JOIN "{ognp.active_schema}"."{kp_samlet}" AS k ON ST_DWithin(k.geom, x.centroid, 0);""")
        # stats = cur.fetchall()
        ognp.run_maintenance(con, ognp.active_schema, "fritidsbygg_vs_plan_alle", [
    ("bygningcentroid", "GIST", False),
    ("reguleringsplan_omrade", "GIST", False),
    ("kommuneplan_omrade", "GIST", False),
    ("kommunenummer", "btree", True),
    ("reguleringsplan_arealformal", "btree", False),
    ("kommuneplan_arealformal", "btree", False),
    ])




import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
plt.switch_backend("WxAgg")





# Regulaeringsplaner
with psycopg2.connect(connection_string) as con:
    con.set_session(autocommit=True)
    with con.cursor() as cur:
        cur.execute(f"""SELECT
reguleringsplan_arealformal,
sum(antall) AS antall_fritidsbygg_totalt,
sum(antall) / (sum(ST_Area(reguleringsplan_omrade)) / 10000.0) AS tetthet_fritidsbygg_sum,
max(antall / (ST_Area(reguleringsplan_omrade) / 10000.0)) AS tetthet_fritidsbygg_max,
avg(antall / (ST_Area(reguleringsplan_omrade) / 10000.0)) AS tetthet_fritidsbygg_avg,
stddev(antall / (ST_Area(reguleringsplan_omrade) / 10000.0)) AS tetthet_fritidsbygg_stddev
FROM (SELECT
count(*) AS antall
, reguleringsplan_omrade
, reguleringsplan_arealformal
, reguleringsplan_ikrafttredelsesdato
, CAST(date_part('year', reguleringsplan_vedtakendeligplandato) / 10 AS integer) * 10 AS vedtakendeligplandato
 FROM "{ognp.active_schema}"."fritidsbygg_vs_plan_alle"
GROUP BY reguleringsplan_omrade
, reguleringsplan_arealformal
, reguleringsplan_ikrafttredelsesdato
, CAST(date_part('year', reguleringsplan_vedtakendeligplandato) / 10 AS integer) * 10) AS x
GROUP BY reguleringsplan_arealformal
;""")
        rp_fritidsbygg = cur.fetchall()

# Kommuneplaner
with psycopg2.connect(connection_string) as con:
    con.set_session(autocommit=True)
    with con.cursor() as cur:
        cur.execute(f"""SELECT
kommuneplan_arealformal,
sum(antall) AS antall_fritidsbygg_totalt,
sum(antall) / (sum(ST_Area(kommuneplan_omrade)) / 10000.0) AS tetthet_fritidsbygg_sum,
max(antall / (ST_Area(kommuneplan_omrade) / 10000.0)) AS tetthet_fritidsbygg_max,
avg(antall / (ST_Area(kommuneplan_omrade) / 10000.0)) AS tetthet_fritidsbygg_avg,
stddev(antall / (ST_Area(kommuneplan_omrade) / 10000.0)) AS tetthet_fritidsbygg_stddev
FROM (SELECT
count(*) AS antall
, kommuneplan_omrade
, kommuneplan_arealformal
 FROM "{ognp.active_schema}"."fritidsbygg_vs_plan_alle"
GROUP BY kommuneplan_omrade
, kommuneplan_arealformal) AS x
GROUP BY kommuneplan_arealformal
;""")
        kp_fritidsbygg = cur.fetchall()

with psycopg2.connect(connection_string) as con:
    con.set_session(autocommit=True)
    with con.cursor() as cur:
        cur.execute(f"""SELECT
kommunenummer
, count(*)
, bygningsid
, bygningstype
, bygningsstatus
, bygningverifiseringsdato
, reguleringsplan_id
, reguleringsplan_arealformal
, reguleringsplan_ikrafttredelsesdato
, reguleringsplan_vedtakendeligplandato
, reguleringsplan_utnyttingstallstats
, reguleringsplan_utnyttingstall_minimum
, reguleringsplan_plantype
, reguleringsplan_planststus
, reguleringsplan_vertikalniva
, reguleringsplan_lovreferanse
, kommuneplan_id
, kommuneplan_arealformal
 FROM "{ognp.active_schema}"."fritidsbygg_vs_plan_alle";""")
        stats = cur.fetchall()



with psycopg2.connect(connection_string) as con:
    con.set_session(autocommit=True)
    with con.cursor() as cur:
        cur.execute(f"""SELECT
CAST(kommunenummer AS integer)
, CAST(sum(antall) AS integer) AS antall
, reguleringsplan_arealformal
, CAST(reguleringsplan_ikrafttredelsesdato_aar AS integer)
, sum(ST_Area(reguleringsplan_omrade))/10000 AS areal_ha
, sum(antall) / sum(ST_Area(reguleringsplan_omrade)/10000) AS tetthet
FROM (
SELECT
kommunenummer
, reguleringsplan_arealformal
, reguleringsplan_omrade
, floor(date_part('year', reguleringsplan_ikrafttredelsesdato)/10.0) * 10 AS reguleringsplan_ikrafttredelsesdato_aar
, count(bygningsid) AS antall
FROM "{ognp.active_schema}"."fritidsbygg_vs_plan_alle"
GROUP BY kommunenummer
, reguleringsplan_arealformal
, reguleringsplan_omrade
, floor(date_part('year', reguleringsplan_ikrafttredelsesdato)/10.0) * 10
) AS x
GROUP BY
kommunenummer
, reguleringsplan_arealformal
, reguleringsplan_ikrafttredelsesdato_aar;""")
        stats = cur.fetchall()

stats




with psycopg2.connect(connection_string) as con:
    con.set_session(autocommit=True)
    with con.cursor() as cur:
        cur.execute(f"""SELECT
reguleringsplan_arealformal
, CAST(sum(antall) AS integer) AS antall
, CAST(reguleringsplan_ikrafttredelsesdato_aar AS integer)
, sum(ST_Area(reguleringsplan_omrade))/10000 AS areal_ha
, sum(antall) / sum(ST_Area(reguleringsplan_omrade)/10000) AS tetthet
FROM (
SELECT
reguleringsplan_arealformal
, reguleringsplan_omrade
, floor(date_part('year', reguleringsplan_ikrafttredelsesdato)/10.0) * 10 AS reguleringsplan_ikrafttredelsesdato_aar
, count(bygningsid) AS antall
FROM "{ognp.active_schema}"."fritidsbygg_vs_plan_alle"
GROUP BY
reguleringsplan_arealformal
, reguleringsplan_omrade
, floor(date_part('year', reguleringsplan_ikrafttredelsesdato)/10.0) * 10
) AS x
GROUP BY
reguleringsplan_arealformal
, reguleringsplan_ikrafttredelsesdato_aar;""")
        rp_stats = cur.fetchall()

rp_stats
npa = np.genfromtxt([";".join([str(x) if x else '' for x in s]) for s in rp_stats], dtype=None, names=["Arealformål", "Antall", "År", "Areal i ha", "Tetthet"], delimiter=";", encoding="UTF8")


with psycopg2.connect(connection_string) as con:
    con.set_session(autocommit=True)
    with con.cursor() as cur:
        cur.execute(f"""SELECT
reguleringsplan_arealformal
, CAST(sum(antall) AS integer) AS antall
, CAST(reguleringsplan_ikrafttredelsesdato_aar AS integer)
, sum(ST_Area(reguleringsplan_omrade))/10000 AS areal_ha
, sum(antall) / sum(ST_Area(reguleringsplan_omrade)/10000) AS tetthet
FROM (
SELECT
reguleringsplan_arealformal
, reguleringsplan_omrade
, floor(date_part('year', reguleringsplan_ikrafttredelsesdato)/10.0) * 10 AS reguleringsplan_ikrafttredelsesdato_aar
, count(bygningsid) AS antall
FROM "{ognp.active_schema}"."fritidsbygg_vs_plan_alle"
GROUP BY
reguleringsplan_arealformal
, reguleringsplan_omrade
, floor(date_part('year', reguleringsplan_ikrafttredelsesdato)/10.0) * 10
) AS x
GROUP BY
reguleringsplan_arealformal
, reguleringsplan_ikrafttredelsesdato_aar;""")
        rp_stats = cur.fetchall()

rp_stats
npa = np.genfromtxt([";".join([str(x) if x else '' for x in s]) for s in rp_stats], dtype=None, names=["Arealformål", "Antall", "År", "Areal i ha", "Tetthet"], delimiter=";", encoding="UTF8")

plt.switch_backend("WxAgg")

npa_2010 = npa[np.where((npa["År"] == 2010) &  (npa["Antall"] > 100))]
fig, (ax) = plt.subplots(nrows=1)

plt.xticks(rotation="vertical")
plot = plt.bar([x[0:25] for x in npa_2010["Arealformål"][np.argsort(npa_2010["Antall"])]], npa_2010["Antall"][np.argsort(npa_2010["Antall"])])
plt.tight_layout()
plt.show()





with psycopg2.connect(connection_string) as con:
    con.set_session(autocommit=True)
    with con.cursor() as cur:
        cur.execute(f"""SELECT
kommuneplan_arealformal
, CAST(sum(antall) AS integer) AS antall
, CAST(kommuneplan_ikrafttredelsesdato_aar AS integer)
, sum(ST_Area(kommuneplan_omrade))/10000 AS areal_ha
, sum(antall) / sum(ST_Area(kommuneplan_omrade)/10000) AS tetthet
FROM (
SELECT
kommuneplan_arealformal
, kommuneplan_omrade
, floor(date_part('year', kommuneplan_ikrafttredelsesdato)/10.0) * 10 AS kommuneplan_ikrafttredelsesdato_aar
, count(bygningsid) AS antall
FROM "{ognp.active_schema}"."fritidsbygg_vs_plan_alle"
GROUP BY
kommuneplan_arealformal
, kommuneplan_omrade
, floor(date_part('year', kommuneplan_ikrafttredelsesdato)/10.0) * 10
) AS x
GROUP BY
kommuneplan_arealformal
, kommuneplan_ikrafttredelsesdato_aar;""")
        kp_stats = cur.fetchall()

kp_stats

npk = np.genfromtxt([";".join([str(x) if x else '' for x in s]) for s in kp_stats], dtype=None, names=["Arealformål", "Antall", "År", "Areal i ha", "Tetthet"], delimiter=";", encoding="UTF8")


npk_2010 = npk[np.where((npk["År"] == 2010) &  (npk["Antall"] > 100))]

plt.switch_backend("WxAgg")

fig, (ax) = plt.subplots(nrows=1)

plt.xticks(rotation="vertical")
plot = plt.bar(npk_2010["Arealformål"][np.argsort(npk_2010["Antall"])].astype(np.str), npk_2010["Antall"][np.argsort(npk_2010["Antall"])])
plt.tight_layout()
plt.show()


plt.switch_backend("WxAgg")
fig, (ax) = plt.subplots(nrows=1)
plt.xticks(rotation="vertical")
plot = plt.bar(npk_2010["Arealformål"][np.argsort(npk_2010["Tetthet"])].astype(np.str), npk_2010["Tetthet"][np.argsort(npk_2010["Tetthet"])])
plt.tight_layout()
plt.show()

plot = plt.boxplot(npk_2010["Tetthet"])


with psycopg2.connect(connection_string) as con:
    con.set_session(autocommit=True)
    with con.cursor() as cur:
        cur.execute(f"""SELECT
kommunenummer
, bygningsid
, reguleringsplan_arealformal
, reguleringsplan_ikrafttredelsesdato
, kommuneplan_arealformal
, kommuneplan_arealbruksstatus
, planstatus
, planbestemmelse
, kommuneplan_ikrafttredelsesdato
 FROM "{ognp.active_schema}"."fritidsbygg_vs_plan_alle";""")
        kp_stats = cur.fetchall()

df = pd.DataFrame(kp_stats, columns=["kommunenummer", "bygningsid", "reguleringsplan_arealformal", "reguleringsplan_ikrafttredelsesdato", "kommuneplan_arealformal", "kommuneplan_arealbruksstatus", "planstatus", "planbestemmelse", "kommuneplan_ikrafttredelsesdato"])
df["reguleringsplan_arealformal_kort"] = [x[:19] if x else None for x in df["reguleringsplan_arealformal"]]

rpf =  df[df["reguleringsplan_arealformal"].map(df["reguleringsplan_arealformal"].value_counts()) > 100 ]
rpf = rpf.groupby("reguleringsplan_arealformal_kort", sort=False)["reguleringsplan_arealformal_kort"].count()
rpf.sort_index(ascending=False)

kpf =  df[df["kommuneplan_arealformal"].map(df["kommuneplan_arealformal"].value_counts()) > 100 ]
kpf = kpf.groupby("kommuneplan_arealformal", sort=False)["kommuneplan_arealformal"].count()
kpf.sort_index(ascending=False)


plt.switch_backend("WxAgg")
fig, (ax) = plt.subplots(nrows=1)
plt.xticks(rotation="vertical")
rpf.plot(kind="bar")
kpf.plot(kind="bar")
plt.tight_layout()
plt.show()
