#!/bin/bash
set -uexo pipefail

base_dir=/data/scratch
export_dir=${base_dir}/export_gis
postgres="PG:host=gisdata-db.nina.no dbname=gisdata user=trond.simensen"
schema="tomtereserver_fritidsbolig"

rm -rf "$export_dir"
mkdir -p "$export_dir"
cd "$export_dir"

for param in formalsomrader kommuner fylker; do
  for output in tomtereserver_fritidsbolig{,_$param}; do
    ogr2ogr -progress -overwrite -f GPKG -geomfield geom -nln "$param" "${output}.gpkg" "$postgres" "${schema}.${output}"
  done
done

cp -r "$export_dir" /data/P-Prosjekter/15227000_kartlegging_av_tomtereserve_for_fritidsbebyggels/
