from pathlib import Path
# Set connection parameters
pg_host = "gisdata-db.nina.no"
pg_db = "gisdata"
#pg_user = Sys.getenv("LOGNAME")

workdir = Path(
    "/data/P-Prosjekter/15227000_kartlegging_av_tomtereserve_for_fritidsbebyggels/"
)
plot_dir = workdir.joinpath("plots")

schema = "tomtereserver_fritidsbolig"
# table = "fritidsbygg_vs_plan_alle"
table_plans = "plan_og_fritidsbygg"
table_reserve = "fritidsbygg_formal"

# columns = dbListFields(con, name=Id(schema=schema, table = table_reserve))
# columns = columns[!columns %in% {"geom", "bygningcentroid")]
fritidsboligformal = 'fritidsboligformal'
columns = {
    "poid": "",
    "areal_m2": "",
    "lokalid": "",
    "planidentifikasjon": "",
    "plankategori": "",
    "arealformal": "",
    "arealbruksstatus": "",
    "vedtakendeligplandato": "",
    "ikrafttredelsesdato": "",
    "omradenavn": "",
    "beskrivelse": "",
    "plannavn": "",
    "plantype": "",
    "planstatus": "",
    "planbestemmelse": "",
    "lovreferansetype": "",
    "vertikalniva": "",
    "link": "",
    "utnyttingstype": "",
    "utnyttingstall": "",
    "utnyttingstall_minimum": "",
    "informasjon": "",
    "utm_33_x": "",
    "utm_33_y": "",
    "geom_centroid": "",
    "antall_hytter_per_m2": "",
    "antall_bygninger": "",
    "ubebyggbar_areal_m2": "",
    "bebyggbar_areal_m2": "",
    "reguleringsplan_percent": "",
    "bebygd_areal_m2": "",
    "tomtereserve_m2": "",
    "tomtereserve_antall_boliger": "",
    "tomtereserve_regulert_m2": "",
    "landskap": "",
    "myr_areal_m2": "",
    "fjell_areal_m2": "",
    "kystsone_1_m2": "",
    "kystsone_2_m2": "",
    "kystsone_3_m2": "",
    "avstand_kystkontur_m": "",
    "naturvern_m2": "",
    "naturvern_streng_m2": "",
    "naturvern_foreslatt_m2": "",
    "naturvern_foreslatt_streng_m2": "",
    "naturtyper_m2": "",
    "naturtyper_hoy_verdi_m2": "",
    "reindrift_funksjonsomrader_m2": "",
    "reindrift_adminomrader_m2": "",
    "villrein_m2": "",
    "villrein_helar_m2": "",
    "kommune_landareal_km2": "",
    "kommunenummer_aktuell": "",
    "tilgjengelig_stor_areal_m2": "",
    "bebygd_areal_m2": "",
    "avstand_vei_m": "",
    "befolkning_2h": "",
    "kjoereavstand_avg_2h": "",
    "kjoereavstand_naermeste_tettsted_minutes": "",
    "befolkning_3h": "",
    "kjoereavstand_avg_3h": "",
    "befolkning_4h": "",
    "kjoereavstand_avg_4h": "",
    "kystsone_m2": "",
}

def to_daa(variabel):
    """"""
    if "m2" in variabel:
        if "_per_m2" in variabel:
            return f"{variabel} * 1000.0 AS {variabel.replace('m2', 'daa')},\n"
        else:
            return f"{variabel} / 1000.0 AS {variabel.replace('m2', 'daa')},\n"
    else:
        return f"{variabel},\n"

def sum_if(variabel):
    """"""
    if "m2" in variabel:
        return f"sum({variabel})::double precision / 1000.0 AS {variabel.replace('m2', 'daa')}_sum,\n"
    else:
        return f"sum({variabel})::double precision AS {variabel}_sum,\n"

def mean_if(variabel, reference="areal_m2"):
    """"""
    if "m2" in variabel:
        if "per_m2" in variabel:
            return f"(sum({variabel} * {reference}) / sum({reference}))::double precision * 1000.0 AS {variabel.replace('m2', 'daa')}_avg,\n"
        else:
            return f"(sum({variabel} * {reference}) / sum({reference}))::double precision / 1000.0 AS {variabel.replace('m2', 'daa')}_avg,\n"
    else:
        return f"sum({variabel} * {reference}) / sum({reference})::double precision AS {variabel}_avg,\n"

def mode_if(variabel):
    """"""
    return f"first({variabel}) AS {variabel}_mode,\n"

def percent_of(variabel, reference="areal_m2"):
    """"""
    alias_name = variabel.replace("_m2", "_perc") if "_m2" in variabel else f"{variabel}_perc"
    return f"round(avg(({variabel} / {reference} * 100.0))::numeric, 2) AS {alias_name},\n"

analysis_variables = {
    "descriptiv_vars": {
        "antall_hytter_per_m2": {
            "title": "Antall hytter per m2",
            "aggregations": [mean_if],
        },
        "antall_bygninger": {
            "title": "Antall hytter på planområde",
            "aggregations": [sum_if],
        },
        "ubebyggbar_areal_m2": {
            "title": "Areal utilgjengelig for bebyggelse i m2",
            "aggregations": [sum_if],
        },
        "tilgjengelig_stor_areal_m2": {
            "title": "Støre sammenhengende areal tilgjengelig for bebyggelse i m2",
            "aggregations": [sum_if],
        },
        "reguleringsplan_percent": {
            "title": "Andel regulert areal i planformålsområde",
            "aggregations": [mean_if],
        },
        "tomtereserve_m2": {
            "title": "Tomtereserve i planformålsområde i m2",
            "aggregations": [sum_if],
        },
        "tomtereserve_antall_boliger": {
            "title": "Tomtereserve i estimert antall fritidsboliger som kan bygges",
            "aggregations": [sum_if],
        },
        "tomtereserve_regulert_m2": {
            "title": "Regulert tomtereserve i planformålsområde i m2",
            "aggregations": [sum_if],
        },
        "avstand_kystkontur_m": {
            "title": "Avstand til kystlinje i m",
            "aggregations": [mean_if],
        },

        #"landskap": {
        #    "title": "Hoved landskapstype i planområde",
        #    "aggregations": [mode_if],
        #},
    },
    "conflict_vars": {
        "myr_areal_m2": {"title": "Overlapp med myr i dekar", "aggregations": [sum_if, percent_of]},
        "fjell_areal_m2": {
            "title": "Areal over skoggrensen i dekar",
            "aggregations": [sum_if, percent_of],
        },
        "kystsone_m2": {"title": "Areal i kystsonen i dekar", "aggregations": [sum_if, percent_of]},
        "kystsone_1_m2": {
            "title": "Areal i kystsone 1 i dekar",
            "aggregations": [sum_if, percent_of],
        },
        "kystsone_2_m2": {
            "title": "Areal i kystsone 2 i dekar",
            "aggregations": [sum_if, percent_of],
        },
        "kystsone_3_m2": {
            "title": "Areal i kystsone 3 i dekar",
            "aggregations": [sum_if, percent_of],
        },
        "naturvern_m2": {
            "title": "Overlapp med verneområder i dekar",
            "aggregations": [sum_if, percent_of],
        },
        "naturvern_streng_m2": {
            "title": "Overlapp med strenge verneområder i dekar",
            "aggregations": [sum_if, percent_of],
        },
        "naturvern_foreslatt_m2": {
            "title": "Overlapp med foreslåtte verneområder i dekar",
            "aggregations": [sum_if, percent_of],
        },
        "naturvern_foreslatt_streng_m2": {
            "title": "Overlapp med foreslåtte strenge verneområder i dekar",
            "aggregations": [sum_if, percent_of],
        },
        "naturtyper_m2": {
            "title": "Overlapp med kjene verdifulle naturtyper i dekar",
            "aggregations": [sum_if, percent_of],
        },
        "naturtyper_hoy_verdi_m2": {
            "title": "Overlapp med kjene naturtyper av høy verdi i dekar",
            "aggregations": [sum_if, percent_of],
        },
        "reindrift_funksjonsomrader_m2": {
            "title": "Overlapp funksjonsområder for reindrift i dekar",
            "aggregations": [sum_if, percent_of],
        },
        "reindrift_adminomrader_m2": {
            "title": "Overlapp administrative områder for reindrift i dekar",
            "aggregations": [sum_if, percent_of],
        },
        "villrein_m2": {
            "title": "Overlapp med villreinområder i dekar",
            "aggregations": [sum_if, percent_of],
        },
        "villrein_helar_m2": {
            "title": "Overlapp med villreinområder for helårsbruk i dekar",
            "aggregations": [sum_if, percent_of],
        },
    },
    "use_vars": {
        "avstand_vei_m": {
            "title": "Avstand til nærmeste kjørbar vei i NVDB",
            "aggregations": [mean_if],
        },
        "kjoereavstand_naermeste_tettsted_minutes": {
            "title": "Kjøreavstand til nærmeste tettsted",
            "aggregations": [mean_if],
        },
        "befolkning_2h": {
            "title": "Befolkning innen 2 timers kjøretid",
            "aggregations": [mean_if],
        },
        "befolkning_3h": {
            "title": "Befolkning innen 3 timers kjøretid",
            "aggregations": [mean_if],
        },
        "befolkning_4h": {
            "title": "Befolkning innen 4 timers kjøretid",
            "aggregations": [mean_if],
        },
    },
}

sql = f"""SELECT
percentile_cont(0.5) WITHIN GROUP (ORDER BY andel_fritidsboligomrader),
percentile_cont(0.5) WITHIN GROUP (ORDER BY andel_tomtereserve)
FROM
(SELECT
kommunenummer_aktuell
, areal_km2 / kommune_landareal_km2 AS andel_fritidsboligomrader
, tomtereserve_km2 / kommune_landareal_km2 AS andel_tomtereserve
FROM (SELECT
kommunenummer_aktuell
, array_to_string(array_agg(DISTINCT plankategori), '/') AS plankategorier
, sum(areal_km2) AS areal_km2
, min(kommune_landareal_km2) AS kommune_landareal_km2
, sum(tomtereserve_km2) AS tomtereserve_km2
FROM
(SELECT
kommunenummer_aktuell
, plankategori
, areal_m2 / 1000000.0 AS areal_km2
, kommune_landareal_km2
, tomtereserve_m2::double precision / 1000000.0 AS tomtereserve_km2
FROM {ognp.active_schema}."fritidsboligformal"
) AS a
GROUP BY kommunenummer_aktuell) AS b
WHERE plankategorier LIKE '%Kommuneplan%') AS c;"""
with ognp.connection.cursor() as cur:
    cur.execute(sql)
    medians = cur.fetchone()


rescale_sql_list = []
agg_sql_list = []
for v in analysis_variables:
    for k, n in analysis_variables[v].items():
        rescale_sql_list.append(to_daa(k))
        for x in n["aggregations"]:
            agg_sql_list.append(x(k))

agg_sql = "".join(agg_sql_list).rstrip(",\n")
agg_sql = f"""DROP TABLE IF EXISTS {ognp.active_schema}."tomtereserve_kommuner";
CREATE TABLE {ognp.active_schema}."tomtereserve_kommuner" AS SELECT *,
 (areal_fritidsbolig_formal_daa::double precision /1000.0) / landareal_km2 AS andel_fritidsboligomrader
 FROM
(SELECT
  CAST(kommunenummer AS smallint),
  navn,
  ST_CollectionExtract(ST_Collect(geom), 3) AS geom,
  max(samiskforvaltningsomrade) AS samiskforvaltningsomrade,
  sum(landareal_km2) AS landareal_km2,
  sum(antall_fritidsbolig)::integer AS antall_fritidsbolig,
  max(dekkning_plandata_andel) AS dekkning_plandata_andel
FROM {ognp.active_schema}."kommuner"
GROUP BY kommunenummer, navn) AS a LEFT JOIN
(SELECT
  kommunenummer_aktuell AS kommunenummer
  , array_to_string(array_agg(DISTINCT plankategori), '/') AS plankategorier
  , sum(areal_m2) / 1000.0 AS areal_fritidsbolig_formal_daa
  , {agg_sql}
FROM {ognp.active_schema}."{fritidsboligformal}"
GROUP BY kommunenummer_aktuell) AS b USING (kommunenummer);"""
with ognp.connection.cursor() as cur:
    cur.execute(agg_sql)

sql = f"""ALTER TABLE {ognp.active_schema}."tomtereserve_kommuner" ADD COLUMN IF NOT EXISTS tomtereserve_estimert_daa_sum double precision;
UPDATE {ognp.active_schema}."tomtereserve_kommuner_shiny" SET tomtereserve_estimert_daa_sum = 0;
UPDATE {ognp.active_schema}."tomtereserve_kommuner" SET tomtereserve_estimert_daa_sum = CASE
  WHEN plankategorier IS NULL THEN landareal_km2 * {medians[1]}
  WHEN plankategorier NOT LIKE '%Kommuneplan%' THEN (landareal_km2 * {medians[1]}) - (tomtereserve_regulert_daa_sum / 1000.0)
END * 1000.0
WHERE plankategorier IS NULL OR plankategorier NOT LIKE '%Kommuneplan%';"""
with ognp.connection.cursor() as cur:
    cur.execute(sql)

ognp.run_maintenance("tomtereserve_kommuner", [("geom", "gist", True), ("kommunenummer", "btree", True)])

agg_sql = "".join(agg_sql_list).rstrip(",\n")
agg_sql = f"""DROP TABLE IF EXISTS {ognp.active_schema}."tomtereserve_fylker";
CREATE TABLE {ognp.active_schema}."tomtereserve_fylker" AS SELECT * FROM
(SELECT fylkesnummer, navn, geom, sum(y.landareal_km2) AS landareal_km2,
max(y.dekkning_plandata_andel) AS dekkning_plandata_andel,
sum(y.antall_fritidsbolig)::integer AS antall_fritidsbolig
FROM
(SELECT CAST(fylkesnummer AS smallint), navn, geom, samiskforvaltningsomrade FROM {ognp.active_schema}."fylker") AS x LEFT JOIN
(SELECT CAST(kommunenummer::smallint / 100 AS smallint) AS fylkesnummer,
CASE WHEN plankategorier LIKE '%Kommuneplan%' THEN landareal_km2 ELSE 0::integer END AS landareal_km2,
antall_fritidsbolig,
dekkning_plandata_andel FROM {ognp.active_schema}."tomtereserve_kommuner") AS y
USING (fylkesnummer) GROUP BY fylkesnummer, geom, navn
) AS a LEFT JOIN
(SELECT CAST(kommunenummer_aktuell / 100 AS smallint) AS fylkesnummer,
sum(areal_m2) / 1000.0 AS areal_fritidsbolig_formal_daa,
{agg_sql} FROM {ognp.active_schema}."{fritidsboligformal}" GROUP BY fylkesnummer) AS b
USING (fylkesnummer);"""

with ognp.connection.cursor() as cur:
    cur.execute(agg_sql)
ognp.run_maintenance("tomtereserve_fylker", [("geom", "gist", True), ("fylkesnummer", "btree", True)])

rescale_sql = "".join(rescale_sql_list).rstrip(",\n")
rescale_sql = f"""DROP TABLE IF EXISTS {ognp.active_schema}."tomtereserver_formalsomrader";
DROP TABLE IF EXISTS {ognp.active_schema}."tomtereserve_formalsomrader";
CREATE TABLE {ognp.active_schema}."tomtereserve_formalsomrader" AS SELECT DISTINCT ON (a.poid)
    a."poid",
    b.kommune,
    c.fylke,
    a."areal_m2",
    a."lokalid",
    a."planidentifikasjon",
    a."plankategori",
    a."arealformal",
    a."arealbruksstatus",
    a."vedtakendeligplandato",
    a."ikrafttredelsesdato",
    a."omradenavn" AS navn,
    a."beskrivelse",
    a."plannavn",
    a."plantype",
    a."planstatus",
    a."planbestemmelse",
    a."lovreferansetype",
    a."vertikalniva",
    a."link",
    a."utnyttingstype",
    a."utnyttingstall",
    a."utnyttingstall_minimum",
    a."informasjon",
    a."utm_33_x",
    a."utm_33_y",
    a."geom_centroid",
    a."kommunenummer_aktuell" AS kommunenummer,
{rescale_sql} FROM {ognp.active_schema}."{fritidsboligformal}" AS a LEFT JOIN
(SELECT DISTINCT ON (kommunenummer) kommunenummer, navn AS kommune FROM {ognp.active_schema}."kommuner") AS b ON (CAST(a."kommunenummer_aktuell" AS smallint) = CAST(b."kommunenummer" AS smallint)) LEFT JOIN
(SELECT DISTINCT ON (fylkesnummer) fylkesnummer, navn AS fylke FROM {ognp.active_schema}."fylker") AS c ON (CAST(CAST(a."kommunenummer_aktuell" AS smallint)/100 AS smallint) = CAST(c."fylkesnummer" AS smallint));"""

with ognp.connection.cursor() as cur:
    cur.execute(rescale_sql)

agg_sql = "".join(agg_sql_list).rstrip(",\n")
agg_sql = f"""DROP TABLE IF EXISTS {ognp.active_schema}."tomtereserve_kommuner_shiny";
CREATE TABLE {ognp.active_schema}."tomtereserve_kommuner_shiny" AS SELECT *,
 (areal_fritidsbolig_formal_daa::double precision / 1000.0) / landareal_km2 AS andel_fritidsboligomrader
 FROM
(SELECT CAST(kommunenummer AS smallint) AS fid, navn,
  CAST(kommunenummer AS smallint), ST_CollectionExtract(ST_Transform(ST_Collect(geom), 4326),3) AS geom,
  ST_X(ST_Transform(ST_Centroid(ST_Collect(geom)), 4326)) AS longitude,
  ST_Y(ST_Transform(ST_Centroid(ST_Collect(geom)), 4326)) AS latitude,
  max(samiskforvaltningsomrade) AS samiskforvaltningsomrade,
  sum(landareal_km2) AS landareal_km2,
  max(dekkning_plandata_andel) AS dekkning_plandata_andel,
  sum(antall_fritidsbolig)::integer AS antall_fritidsbolig
FROM {ognp.active_schema}."kommuner"
GROUP BY kommunenummer, navn) AS a LEFT JOIN
(SELECT kommunenummer_aktuell AS kommunenummer
, array_to_string(array_agg(DISTINCT plankategori), '/') AS plankategorier
, sum(areal_m2) / 1000.0 AS areal_fritidsbolig_formal_daa
, {agg_sql}
FROM {ognp.active_schema}."{fritidsboligformal}"
GROUP BY kommunenummer_aktuell) AS b USING (kommunenummer) LEFT JOIN
(SELECT CAST("fylkesnummer" AS smallint), navn AS fylke FROM {ognp.active_schema}."fylker") AS c ON (CAST(CAST(a."kommunenummer" AS smallint)/100 AS smallint) = fylkesnummer);"""

with ognp.connection.cursor() as cur:
    cur.execute(agg_sql)

sql = f"""ALTER TABLE {ognp.active_schema}."tomtereserve_kommuner_shiny" ADD COLUMN IF NOT EXISTS tomtereserve_estimert_daa_sum double precision;
UPDATE {ognp.active_schema}."tomtereserve_kommuner_shiny" SET tomtereserve_estimert_daa_sum = 0;
UPDATE {ognp.active_schema}."tomtereserve_kommuner_shiny" SET tomtereserve_estimert_daa_sum = CASE
  WHEN plankategorier IS NULL THEN landareal_km2 * {medians[1]}
  WHEN plankategorier NOT LIKE '%Kommuneplan%' THEN (landareal_km2 * {medians[1]}) - (tomtereserve_regulert_daa_sum / 1000.0)
END * 1000.0
WHERE plankategorier IS NULL OR plankategorier NOT LIKE '%Kommuneplan%';"""
with ognp.connection.cursor() as cur:
    cur.execute(sql)
ognp.run_maintenance("tomtereserve_kommuner_shiny", [("geom", "gist", True), ("kommunenummer", "btree", True)])

agg_sql = "".join(agg_sql_list).rstrip(",\n")
agg_sql = f"""DROP TABLE IF EXISTS {ognp.active_schema}."tomtereserve_fylker_shiny";
CREATE TABLE {ognp.active_schema}."tomtereserve_fylker_shiny" AS SELECT * FROM
(SELECT fylkesnummer AS fid, navn,
  fylkesnummer, ST_Transform(geom, 4326) AS geom,
    ST_X(ST_Transform(ST_Centroid(geom), 4326)) AS longitude,
    ST_Y(ST_Transform(ST_Centroid(geom), 4326)) AS latitude,
sum(y.landareal_km2) AS landareal_km2,
max(y.dekkning_plandata_andel) AS dekkning_plandata_andel,
sum(y.antall_fritidsbolig)::integer AS antall_fritidsbolig,
avg(andel_fritidsboligomrader) AS andel_fritidsboligomrader_avg
FROM
(SELECT CAST(fylkesnummer AS smallint), geom, samiskforvaltningsomrade, navn FROM {ognp.active_schema}."fylker") AS x LEFT JOIN
(SELECT CAST(kommunenummer::smallint / 100 AS smallint) AS fylkesnummer,
andel_fritidsboligomrader,
CASE
  WHEN plankategorier LIKE '%Kommuneplan%' THEN landareal_km2 ELSE 0::integer
END AS landareal_km2,
dekkning_plandata_andel, antall_fritidsbolig FROM {ognp.active_schema}."tomtereserve_kommuner") AS y
USING (fylkesnummer) GROUP BY fylkesnummer, geom, navn
) AS a LEFT JOIN
(SELECT CAST(kommunenummer_aktuell / 100 AS smallint) AS fylkesnummer,
sum(areal_m2) / 1000.0 AS areal_fritidsbolig_formal_daa,
{agg_sql} FROM {ognp.active_schema}."{fritidsboligformal}" GROUP BY fylkesnummer) AS b
USING (fylkesnummer);"""

with ognp.connection.cursor() as cur:
    cur.execute(agg_sql)
ognp.run_maintenance("tomtereserve_fylker_shiny", [("geom", "gist", True), ("fylkesnummer", "btree", True)])

rescale_sql = "".join(rescale_sql_list).rstrip(",\n")
rescale_sql = f"""DROP TABLE IF EXISTS {ognp.active_schema}."tomtereserver_formalsomrader_shiny";
DROP TABLE IF EXISTS {ognp.active_schema}."tomtereserve_formalsomrader_shiny";
CREATE TABLE {ognp.active_schema}."tomtereserve_formalsomrader_shiny" AS SELECT DISTINCT ON (a.poid)
    a."poid" AS fid,
    b.kommune,
    c.fylke,
    a."areal_m2",
    a."lokalid",
    a."planidentifikasjon",
    a."plankategori",
    a."arealformal",
    a."arealbruksstatus",
    a."vedtakendeligplandato",
    a."ikrafttredelsesdato",
    a."omradenavn" AS navn,
    a."beskrivelse",
    a."plannavn",
    a."plantype",
    a."planstatus",
    a."planbestemmelse",
    a."lovreferansetype",
    a."vertikalniva",
    a."link",
    a."utnyttingstype",
    a."utnyttingstall",
    a."utnyttingstall_minimum",
    a."informasjon",
    ST_X(ST_Transform("geom_centroid", 4326)) AS longitude,
    ST_Y(ST_Transform("geom_centroid", 4326)) AS latitude,
    ST_Transform("geom_centroid", 4326) AS geom,
{rescale_sql} FROM {ognp.active_schema}."{fritidsboligformal}" AS a LEFT JOIN
(SELECT DISTINCT ON (kommunenummer) kommunenummer, navn AS kommune FROM {ognp.active_schema}."kommuner") AS b ON (CAST(a."kommunenummer_aktuell" AS smallint) = CAST(b."kommunenummer" AS smallint)) LEFT JOIN
(SELECT DISTINCT ON (fylkesnummer) fylkesnummer, navn AS fylke FROM {ognp.active_schema}."fylker") AS c ON (CAST(CAST(a."kommunenummer_aktuell" AS smallint)/100 AS smallint) = CAST(c."fylkesnummer" AS smallint));"""
with ognp.connection.cursor() as cur:
    cur.execute(rescale_sql)


area_breaks = [
    1000,
    2000,
    5000,
    25000,
    50000,
    100000,
    500000,
    1000000,
    5000000,
    10000000,
]


reserve_breaks = [0.0001, 0.00045, 0.0006]

tetthet_ref = 0.6

"""
kodeliste_arealbruk = read.csv(file.path(workdir, "kodeliste_arealbruk.csv"), header = FALSE, sep = ";")
names(kodeliste_arealbruk) = {"kommuneplan_arealformal_tekst", "kommuneplan_arealformal")

kodeliste_arealformal = read.csv(file.path(workdir, "kodeliste_arealformal.csv"), header = FALSE, sep = ";")
names(kodeliste_arealformal) = {"kommuneplan_arealformal_tekst", "kommuneplan_arealformal")

kommuneplan_arealformal_koder = rbind(kodeliste_arealbruk, kodeliste_arealformal)
"""
