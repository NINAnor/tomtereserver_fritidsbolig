#!/usr/bin/env python3

# Topology cleaning in GRASS GIS

import sqlite3
import grass.script as gscript
from grass.pygrass.vector.table import Table, get_path
from grass.pygrass.vector import VectorTopo
from datetime import datetime, timedelta


def topo_clean_sort(
    vector_map,
    mask=None,
    min_area=30,
    order_string="date(versjonid) DESC, date(oppdateringsdato) DESC, date(ikrafttredelsesdato) DESC",
):
    input_layer, vmap, attr_key, path, driver = (
        gscript.read_command(
            "v.db.connect",
            overwrite=True,
            verbose=True,
            map=vector_map,
            layer=1,
            flags="g",
        )
        .strip()
        .split("\n")[0]
        .split("|")
    )


    if gscript.find_file(f"{vector_map}_cleaned", element="vector", mapset=".")["file"]:
        # Delete layer (so table is not deleted when map is removed)
        gscript.run_command(
            "v.db.connect",
            overwrite=True,
            verbose=True,
            flags="d",
            map=f"{vector_map}_cleaned",
            layer=3,
        )

    if gscript.db.db_table_exist(f"{vector_map}_cleaned"):
        gscript.run_command(
            "db.droptable",
            overwrite=True,
            verbose=True,
            table=f"{vector_map}_cleaned",
            flags="f",
            )

    gscript.run_command(
            "db.copy",
            overwrite=True,
            verbose=True,
            from_table=vector_map,
            to_table=f"{vector_map}_cleaned",
        )

    gscript.run_command(
        "v.category",
        overwrite=True,
        verbose=True,
        input=vector_map,
        layer=3,
        type="area,centroid",
        output="tmp",
        option="add",
    )

    con = sqlite3.connect(get_path("$GISDBASE/$LOCATION_NAME/$MAPSET/sqlite/sqlite.db"))
    cur = con.cursor()
    try:
        cur.execute("DROP TABLE l1_l3;")
        cur.connection.commit()
    except Exception:
        pass

    cur.execute("CREATE TABLE l1_l3 (l1_cat int, l3_cat int);")
    cur.connection.commit()

    insert_sql = "INSERT INTO l1_l3(l1_cat, l3_cat) VALUES(?,?);"
    cur.executemany(
        insert_sql,
        [
            (
                int(c) if c else None,
                int(r.split("|")[1])
                if len(r.split("|")) == 2
                else int(r.split("|")[0]),
            )
            for r in gscript.read_command(
                "v.category", input="tmp", layer="1,3", option="print"
            )
            .strip()
            .split("\n")
            for c in r.split("|")[0].split("/")
            if len(r.split("|")) == 2
        ],
    )
    cur.connection.commit()

    cur.execute("CREATE INDEX l1_l3_l1_cat ON l1_l3(l1_cat);")
    cur.execute("CREATE INDEX l1_l3_l3_cat ON l1_l3(l3_cat);")
    con.commit()

    now = datetime.today() + timedelta(days=1)

    input_table = input_layer.split("/")[
        1
    ]  # "tomtereserver_fritidsbolig_kommuneplaner_samlet"

    try:
        cur.execute("DROP TABLE tmp_match;")
        cur.connection.commit()
    except Exception:
        pass

    # Consider using HAVING conditions (e.g. HAVING ikrafttredelsesdato = max(ikrafttredelsesdato)
    create_sql = f"""CREATE TABLE tmp_match AS SELECT
  l3_cat AS cat_new,
  CAST(CASE
    WHEN count(l1_cat) = 1 THEN max(l1_cat)
    ELSE substr(group_concat(l1_cat), 0, instr(group_concat(l1_cat),','))
  END AS integer) AS cat_org,
  group_concat(l1_cat) AS cats
  FROM (SELECT
    l3_cat, l1_cat,
    plankategori,
    versjonid,
    ikrafttredelsesdato,
    vedtakendeligplandato
    FROM
      (SELECT * FROM "l1_l3" WHERE l1_cat IS NOT NULL) AS a LEFT JOIN
      (SELECT
          cat,
          plankategori,
          versjonid,
          oppdateringsdato,
          CASE
            WHEN ikrafttredelsesdato IS NULL THEN '{now.isoformat()}'
            ELSE ikrafttredelsesdato
          END AS ikrafttredelsesdato,
          CASE
            WHEN vedtakendeligplandato IS NULL THEN '{now.isoformat()}'
            ELSE vedtakendeligplandato
          END AS vedtakendeligplandato
          FROM
        tmp) AS b
      ON a.l1_cat = b.cat
    ORDER BY
      l3_cat,  --original category (ID)
      plankategori DESC, -- Reguleringsplaner allways on top
      date(versjonid) DESC, -- latest version (of the respecive plan) on top
      date(ikrafttredelsesdato) DESC, -- latest (of the respecive plan) effecive date on top (breaks ties sorting above)
      date(vedtakendeligplandato) DESC -- latest confirmation (of the respecive plan) on top (breaks ties sorting above)
      ) AS x
    GROUP BY l3_cat
    ORDER BY
      l3_cat,  --original category (ID)
      plankategori DESC, -- Reguleringsplaner allways on top
      date(versjonid) DESC, -- latest version (of the respecive plan) on top
      date(ikrafttredelsesdato) DESC, -- latest (of the respecive plan) effecive date on top (breaks ties sorting above)
      date(vedtakendeligplandato) DESC -- latest confirmation (of the respecive plan) on top (breaks ties sorting above)
      ;"""
    cur.execute(create_sql)
    cur.connection.commit()

    cur.execute("CREATE INDEX tmp_match_cat_org ON tmp_match(cat_org);")
    cur.execute("CREATE UNIQUE INDEX tmp_match_cat_new ON tmp_match(cat_new);")
    cur.execute("DROP TABLE l1_l3;")
    cur.connection.commit()
    con.close()

    gscript.run_command(
        "v.db.connect",
        overwrite=True,
        verbose=True,
        flags="o",
        map="tmp",
        table="tmp_match",
        layer=3,
        key="cat_new",
    )

    gscript.run_command(
        "v.dissolve",
        overwrite=True,
        verbose=True,
        input="tmp",
        layer=3,
        column="cat_org",
        output=f"{vector_map}_dissolved",
    )
    vect_clean_input = f"{vector_map}_dissolved"
    if mask:
        gscript.run_command(
            "v.overlay",
            overwrite=True,
            verbose=True,
            flags="t",
            ainput=f"{vector_map}_dissolved",
            binput=mask,
            alayer=3,
            blayer=1,
            olayer="0,3,0",
            operator="not",
            output=f"{vector_map}_masked",
            snap=0.1,
        )
        vect_clean_input = f"{vector_map}_masked"

    gscript.run_command(
        "v.clean",
        overwrite=True,
        verbose=True,
        flags="c",
        input=vect_clean_input,
        layer=3,
        tool="rmdac,snap,break,bpol,rmdupl,rmline,rmdangle,rmbridge,rmdac,rmarea,rmdac",
        threshold=f"0,0.01,0,0,0,0,-1,0,0,{min_area},0",
        output=f"{vector_map}_cleaned",
    )

    gscript.run_command(
        "v.db.connect",
        overwrite=True,
        verbose=True,
        flags="o",
        map=f"{vector_map}_cleaned",
        table=f"{vector_map}_cleaned",
        layer=3,
    )

    gscript.run_command(
        "db.execute",
        overwrite=True,
        verbose=True,
        sql=f"CREATE UNIQUE INDEX IF NOT EXISTS {vector_map}_cleaned_cat_idx ON {vector_map}_cleaned (cat);",
    )

    # gscript.run_command("g.copy", overwrite=True, verbose=True, vector="tmp,tmp_backup")

    return 0


def main():
    vector_map = "kommunale_planer_samlet"
    snap = 0.1
    min_area_import = 1
    min_area_clean = 30
    host = "gisdata-db.nina.no"
    dbname = "gisdata"
    user = "stefan.blumentrath"
    schema = "tomtereserver_fritidsbolig"

    gscript.run_command(
        "v.in.ogr",
        overwrite=True,
        verbose=True,
        input="PG:host=gisdata-db.nina.no dbname=gisdata user=stefan.blumentrath",
        layer=f"tomtereserver_fritidsbolig.n50_arealdekke_omrade",
        output="n50_vann",
        snap=0.1,
        min_area=1,
        geometry="geom",
        where="objtype IN ('Innsjø', 'Hav', 'ElvBekk')",
    )

    gscript.run_command(
        "v.in.ogr",
        overwrite=True,
        verbose=True,
        input="PG:host=gisdata-db.nina.no dbname=gisdata user=stefan.blumentrath",
        layer=f"tomtereserver_fritidsbolig.{vector_map}",
        output=vector_map,
        snap=snap,
        min_area=min_area_import,
        geometry="geom",
        where="arealformal not between 400 and 499",
    )

    topo_clean_sort(
        vector_map,
        mask="n50_vann",
        min_area=min_area_clean,
        order_string="date(versjonid) DESC, date(oppdateringsdato) DESC, date(ikrafttredelsesdato) DESC",
    )

    gscript.run_command(
        "v.out.ogr",
        overwrite=True,
        verbose=True,
        layer=3,
        input=f"{vector_map}_cleaned",
        type="area",
        output=f"PG:host={host} dbname={dbname} user={user}",
        output_layer=f"{schema}.{vector_map}_cleaned",
        format="PostgreSQL",
    )
