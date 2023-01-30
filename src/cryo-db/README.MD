# CHARTER WP4 Holocene Cryo DB v2

Version 2 of the CHARTER WP4 holocene cryo database code repository. The code is intentended to be incoorporated with the [Arctic Holocene Biodiversity database](https://github.com/AndrewIOM/holocene-arctic-biodiversity-map) (AHBDB), but also works as a tempelate for any independent cryosphere variable extractions from the [CHELSA TraCE21k dataset](https://chelsa-climate.org/chelsa-trace21k/) or its successors.

In brief, the code:
1. reads in all distinct coordinates from a CSV files following the AHBDB format.
2. accesses the CHELSA TraCE21k data hosted remotely.
    - the core variable rasters are hosted on the WSL S3 servers. 
    - the glacial extent vector geometries are hosted on the author's AWS S3.
3. extracts and calculates the time-series for the following variables:
    - temperature
    - precipitation
    - elevation
    - distance to land ice
    - maximum year in time-series where the location was covered by land ice
    - maximum year in time-series where the location's elevation was below sea-level
4. exports the time-series to a csv file (`cryo_db/cryo_db.csv`). 

The repository also contains a GitHub action workflow that facilitates an 
automated update of the database.

Contact: Jakob J. Assmann, email: [jakob.assmann@uzh.ch](mailto:jakob.assmann@uzh.ch)<br>
License: MIT <br>
Last update: 20 January 2023

## Folder structure

```
.github/
    |- workflows/
        |- fetch_cryo_data.yml             GitHub workflow for generating the database
cryo_db/                                   
    |- source_data/                        url refs to source files and look up tables
    |- CHELSA_TrACE21k_gle_rast_to_poly.R  script to generate glacial extent polys
    |- fetch_cryo_data.R                   SCRIPT FOR THE CRYO DATA EXTRACTION
    |- (cryo_db.csv)                       CRYO DATABASE (once generated)
visualisation/
    |- thalloo-static-site/
        |- map-data/                        
            |- ahbdb.txt                   AHBDB example file for testing*.
```

\* The folder structure minimics the AHBDB repo and it should be possible to directly merge the content of this repo with the AHDBD repo. 

## Cryo DB Variables

The time-series have century time-steps expressed in 1000 years before 1990 (**year kBP**).

**CHELSA_TraCE21k_temp** - Annual mean temperature in °C.

- Cell values of the CHELSA_TraCE21k_bio01* raster files for a given location and centrury. 

**CHELSA_TraCE21k_precip** - Annual Precipitation in kg m-2 year-1.
- Cell values of the CHELSA_TraCE21k_bio12* raster files for a given location and centrury. 

**CHELSA_TraCE21k_elevation** - Surface elevation above sea level in m.
- Cell vlaues of the CHELSA_TraCE21k_glm raster files for a given location and centrury. 

**CHESA_TraCE21k_dist_to_land_ice** - Distance to nearest land ice in m. 
- Calculated distance between location and glacial extent polygons derived from the CHELSA_TraCE21k_gle rasters for a given century.

**CHELSA_TraCE21k_max_year_kBP_glaciated** - Last time location was glaciated in 1000 yrs BP. 
- Last centrury in the time-series where the location was glaciated (CHESA_TraCE21k_dist_to_land_ice == 0).

**CHELSA_TraCE21k_max_year_kBP_with_elev_below_sea_level** - Last time location was below sea-level in 1000 yrs BP. 
- Last centrury in the time-series where the location was below sea-level (CHELSA_TraCE21k_elevation < 0).

## References and source data

The reference for the source data is: 

> Karger, D. N., Nobis, M. P., Normand, S., Graham, C. H., & Zimmermann, N. E. (2021): CHELSA-TraCE21k v1. 0. Downscaled transient temperature and precipitation data since the last glacial maximum. Climate of the Past Discussions, 1-27. [https://doi.org/10.5194/cp-2021-30](https://doi.org/10.5194/cp-2021-30).

A technical description of the source data is also available: [CHELSA-TraCE21k v1.0: Technical Specification](https://www.envidat.ch/dataset/c88baa58-dbae-452c-8c2c-343e4c9fb885/resource/a00bc993-e2e9-49a3-ae76-5fe339cded29/download/chelsa-trace21k_technical_documentation.pdf).

The source data can be found here: [http://dx.doi.org/doi:10.16904/envidat.211](http://dx.doi.org/doi:10.16904/envidat.211).

The polygons of the glacial extent rasters can be generated using the `cryo_db/CHELSA_TrACE21k_gle_rast_to_poly.R` script.

## Known issues with the data

**!!! Glacial extent data contains errors 300 CE - 1990 CE !!!**

The glacial extent data seems to contains errors for the centruries spanning 1.6k - 0 years BP (i.e., after 300 CE). The CHESA_TraCE21k_dist_to_land_ice and CHELSA_TraCE21k_max_year_kBP_glaciated should be treated with care for those time steps. The source dataset is currently in review, I believe that future version of the dataset might address this issue.   

## Notes on performance and timeouts

The slowest and most resource demanding step is the calculation of the `CHESA_TraCE21k_dist_to_land_ice` variable. 

Processing of this step is implemented in parallel. However, it can take multiple hours to complete on the default GitHub runners. Consider a (local) powerful worker with plenty of resources to avoid a "time out" error on the free GitHub runners (this will happen after 6h). This could be useful for example, for the first processing or other subsequent large additions to the AHBDB database.

## Cryo Database Version 1

For legacy reasons version 1 of the code can be found here: [https://github.com/jakobjassmann/cryo_db](https://github.com/jakobjassmann/cryo_db).