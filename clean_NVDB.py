    import sqlite3
    import grass.script as gscript
    input_network = "nvdb_nettverk_erfkps"
    cutlines = ["connection_lines", "tettsteder2021_exteriorring"]
    node_layer = "2"
    arc_layer = "3"
    output_map = "nvdb_nettverk_erfkps_split"

    vector_map = "nvdb_nettverk_erfkps"
    gscript.run_command(
        "v.in.ogr",
        overwrite=True,
        verbose=True,
        input="PG:host=gisdata-db.nina.no dbname=gisdata user=stefan.blumentrath",
        layer=f"tomtereserver_fritidsbolig.tettsteder2021_exteriorring",
        output="tettsteder2021_exteriorring",
        snap=0.1,
        geometry="geom",
    )

    gscript.run_command(
        "v.in.ogr",
        overwrite=True,
        verbose=True,
        input="PG:host=gisdata-db.nina.no dbname=gisdata user=stefan.blumentrath",
        layer=f"tomtereserver_fritidsbolig.nvdb_nettverk_erfkps",
        output="nvdb_nettverk_erfkps",
        snap=0.1,
        geometry="geom",
    )

    gscript.run_command(
        "v.in.ogr",
        overwrite=True,
        verbose=True,
        input="PG:host=gisdata-db.nina.no dbname=gisdata user=stefan.blumentrath",
        layer=f"tomtereserver_fritidsbolig.fritidsboligformal",
        output=f"{vector_map}_fritidsbolig_centroid",
        snap=0.1,
        geometry="geom_centroid",
    )

    gscript.run_command(
        "v.distance",
        overwrite=True,
        verbose=True,
        from_=f"{vector_map}_fritidsbolig_centroid",
        from_type="point",
        to="nvdb_nettverk_erfkps",
        to_type="line",
        output="connection_lines",
    )

    gscript.run_command(
        "v.patch",
        flags="b",
        overwrite=True,
        verbose=True,
        input=",".join([input_network] + cutlines),
        output="tmp2",
    )

    gscript.run_command(
        "v.clean",
        flags="bc",
        overwrite=True,
        verbose=True,
        type="line",
        input="tmp2",
        output="tmp3",
        tool="break",
        error="intersection_points",
    )
    gscript.run_command(
        "v.category",
        overwrite=True,
        verbose=True,
        input="intersection_points",
        option="add",
        output="intersection_points2",
        layer=1,
        type="point",
        )

    gscript.run_command(
        "v.extract",
        overwrite=True,
        verbose=True,
        type="point",
        input="intersection_points2",
        output="tmp4",
        flags="t",
    )

    gscript.run_command(
        "v.net",
        overwrite=True,
        flags="cs",
        input=input_network,
        points="tmp4",
        output="TEST",
        operation="connect",
        arc_layer="1",
        arc_type="line",
        node_layer="2",
        threshold=0.01,
        turn_layer="3",
        turn_cat_layer="4",
    )

    gscript.run_command(
        "v.category",
        overwrite=True,
        verbose=True,
        input="TEST",
        layer="3",
        type="line",
        output=output_map,
        option="add",
        cat=1,
        step=1,
    )

    input_layer, vmap, attr_key, path, driver = (
        gscript.read_command(
            "v.db.connect",
            overwrite=True,
            verbose=True,
            map=output_map,
            layer=1,
            flags="g",
        )
        .strip()
        .split("\n")[0]
        .split("|")
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
                "v.category", input=output_map, layer="1,3", option="print"
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

    con.close()

    input_table = input_layer.split("/")[1]

    gscript.run_command(
        "v.db.connect",
        overwrite=True,
        verbose=True,
        flags="o",
        map=output_map,
        table="l1_l3",
        layer=3,
        key="l3_cat",
    )

    gscript.run_command(
        "v.db.join",
        overwrite=True,
        verbose=True,
        map=output_map,
        layer=3,
        column="l1_cat",
        other_table=input_network,
        other_column="cat",
    )

    gscript.run_command(
        "v.out.ogr",
        overwrite=True,
        verbose=True,
        layer=3,
        input=output_map,
        type="line",
        output=f"PG:host={host} dbname={dbname} user={user}",
        output_layer=f"{schema}.nvdb_nettverk_fritidsbolig",
        format="PostgreSQL",
        flags="s2",
        lco="OVERWRITE=YES",
    )
