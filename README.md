# Kartlegging av tomtereserver for fritidsbolig i Norge

This repository contains scripts to map and evaluate 
area designated for cabin development in Norwegian 
municipal plans.

The Pyton scripts are supposed to be run from a GRASS GIS 
session, as GRASS GIS is used to clean the topology of 
planning data and the road network. The first script
is `data_wrangling.py`, the second is `variables.py`.
The scripts `clean_planning_data.py` and `clean_NVDB.py` 
are called from within the `data_wrangling.py` script.

Thus this repository should be `data_wrangling.py` the 
current working directoy when running the scripts.

The R scripts are used in a next step to crate plots, 
figures and summary tables.
