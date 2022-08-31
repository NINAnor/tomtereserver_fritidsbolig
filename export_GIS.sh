#!/bin/bash
base_dir=/data/scratch
export_dir=${base_dir}/export_gis
if [ ! -d $export_dir ] ; then
  mkdir $export_dir
fi

output_file=${export_dir}/tomtereserver_fritidsbolig.gpkg
if [ -f "$output_file" ] ; then
  rm "$output_file"
fi

ogr2ogr -progress -overwrite -f GPKG -geomfield geom -nln formalsomrader "$output_file" "PG:host=gisdata-db.nina.no dbname=gisdata user=stefan.blumentrath" tomtereserver_fritidsbolig.tomtereserve_formalsomrader
ogr2ogr -progress -update -f GPKG -geomfield geom -nln kommuner "$output_file" "PG:host=gisdata-db.nina.no dbname=gisdata user=stefan.blumentrath" tomtereserver_fritidsbolig.tomtereserve_kommuner
ogr2ogr -progress  -update -f GPKG -geomfield geom -nln fylker "$output_file" "PG:host=gisdata-db.nina.no dbname=gisdata user=stefan.blumentrath" tomtereserver_fritidsbolig.tomtereserve_fylker

output_file=${export_dir}/tomtereserver_fritidsbolig_formalsomrader.gpkg
if [ -f "$output_file" ] ; then
  rm "$output_file"
fi
ogr2ogr -progress -overwrite -f GPKG -geomfield geom -nln formalsomrader "$output_file" "PG:host=gisdata-db.nina.no dbname=gisdata user=stefan.blumentrath" tomtereserver_fritidsbolig.tomtereserve_formalsomrader

output_file=${export_dir}/tomtereserver_fritidsbolig_kommuner.gpkg
if [ -f "$output_file" ] ; then
  rm "$output_file"
fi
ogr2ogr -progress -overwrite -f GPKG -geomfield geom -nln kommuner "$output_file" "PG:host=gisdata-db.nina.no dbname=gisdata user=stefan.blumentrath" tomtereserver_fritidsbolig.tomtereserve_kommuner

output_file=${export_dir}/tomtereserver_fritidsbolig_fylker.gpkg
if [ -f "$output_file" ] ; then
  rm "$output_file"
fi
ogr2ogr -progress -overwrite -f GPKG -geomfield geom -nln fylker "$output_file" "PG:host=gisdata-db.nina.no dbname=gisdata user=stefan.blumentrath" tomtereserver_fritidsbolig.tomtereserve_fylker

cp -r $export_dir /data/P-Prosjekter/15227000_kartlegging_av_tomtereserve_for_fritidsbebyggels/
