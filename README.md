# Kartlegging av tomtereserver for fritidsbolig i Norge

This repository contains scripts to map and evaluate 
area designated for cabin development in Norwegian 
municipal plans.

The Pyton scripts are supposed to be run from a GRASS GIS 
session, as GRASS GIS is used to clean the topology of 
planning data and the road network. The first script
is `wrangle_data.py`, the second is `variables.py`.
The scripts `clean_planning_data.py` and `clean_NVDB.py` 
are called from within the `wrangle_data.py` script.

Thus this repository should be the current working 
directoy when running the scripts.

The R scripts (`plots_and_tables.R` and `variables.R`) 
are used in a next step to create plots, figures and 
summary tables.


Please note that precnditions to run the scripts are:
- a running PostGIS instance,
- GRASS GIS >= 7.8, GDAL > 3.4.1, Python >= 3.6 and R >= 4.1
  (the latter two with the respective required packages/libraries)
- libopenfyba for SOSI support
- pgRouting for driving distance analysis
- osgeonorge from pypi (`pip install osgeonorge`)
