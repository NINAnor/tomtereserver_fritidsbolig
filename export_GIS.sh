#!/bin/bash

output_file=/data/scratch/tomtereserver_fritidsbolig.gpkg
if [ -f "$output_file" ] ; then
  rm "$output_file"
fi

ogr2ogr -progress -overwrite -f GPKG -geomfield geom -nln formalsomrader "$output_file" "PG:host=gisdata-db.nina.no dbname=gisdata user=stefan.blumentrath" tomtereserver_fritidsbolig.tomtereserve_formalsomrader
ogr2ogr -progress -update -f GPKG -geomfield geom -nln kommuner "$output_file" "PG:host=gisdata-db.nina.no dbname=gisdata user=stefan.blumentrath" tomtereserver_fritidsbolig.tomtereserve_kommuner
ogr2ogr -progress  -update -f GPKG -geomfield geom -nln fylker "$output_file" "PG:host=gisdata-db.nina.no dbname=gisdata user=stefan.blumentrath" tomtereserver_fritidsbolig.tomtereserve_fylker


if [ -f /data/scratch/tomtereserver_fritidsbolig_.gpkg ] ; then
  rm /data/scratch/tomtereserver_fritidsbolig.gpkg
fi
ogr2ogr -progress -overwrite -f GPKG -geomfield geom -nln formalsomrader /data/scratch/tomtereserver_fritidsbolig.gpkg "PG:host=gisdata-db.nina.no dbname=gisdata user=stefan.blumentrath" tomtereserver_fritidsbolig.tomtereserve_formalsomrader

if [ -f /data/scratch/tomtereserver_fritidsbolig.gpkg ] ; then
  rm /data/scratch/tomtereserver_fritidsbolig.gpkg
fi
ogr2ogr -progress -overwrite -f GPKG -geomfield geom -nln formalsomrader /data/scratch/tomtereserver_fritidsbolig.gpkg "PG:host=gisdata-db.nina.no dbname=gisdata user=stefan.blumentrath" tomtereserver_fritidsbolig.tomtereserve_formalsomrader

if [ -f /data/scratch/tomtereserver_fritidsbolig.gpkg ] ; then
  rm /data/scratch/tomtereserver_fritidsbolig.gpkg
fi
ogr2ogr -progress -overwrite -f GPKG -geomfield geom -nln formalsomrader /data/scratch/tomtereserver_fritidsbolig.gpkg "PG:host=gisdata-db.nina.no dbname=gisdata user=stefan.blumentrath" tomtereserver_fritidsbolig.tomtereserve_formalsomrader
