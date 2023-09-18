# Fetch cryo db data for the tabular version of the holocene arctic biodiversity
# map
# Jakob J. Assmann jakob.assmann@uzh.ch 17 January 2023

## Prepare envrionment ----
cat("Loading packages...")

# List of packages
library(terra)
library(sf)
library(pbapply)
library(dplyr)
library(tidyr)
library(readr)
library(purrr)
library(parallel)

# Set up parallel environment (note core detection is not always reliable)
if(Sys.info()['sysname'] == "Windows"){
  cl <- makeCluster(detectCores() - 1)
  clusterEvalQ(cl, library(terra))
  clusterEvalQ(cl, library(sf))
  clusterEvalQ(cl, library(tidyverse))
} else {
  cl <- detectCores() - 1
}
cat("done.\n")

# Work relative to cryo_db folder (run this if executing locally)
# setwd("cryo_db") 

## Load site coordinates from ahbddb.txt
cat("Loading ahbdb coordinates...")
ahbdb <- read_delim("../../visualisation/thalloo-static-site/map-data/ahbdb_sites.txt",
                    delim = "\t",
                    show_col_types = FALSE)

ahbdb <- ahbdb[which(!is.na(ahbdb$LatDD) & !is.na(ahbdb$LonDD)),]

# Parse and convert as vect
ahbdb <- st_as_sf(ahbdb, 
                  coords = c("LonDD", "LatDD"), 
                  crs = 4326,
                  remove = F) %>%
  select(LonDD, LatDD, geometry) %>%
  distinct()

cat("done.\n")

## Determine subset to extract
cat("Determining subest to extract...")

# Load already extracted coordinates
if(file.exists("cryo_db.csv")){
  cryo_db <- read_csv("cryo_db.csv",
                      show_col_types = FALSE) %>%
    st_as_sf(coords = c("LonDD", "LatDD"), 
             crs = 4326,
             remove = F) 
}

# Determine subset of coordinates to extract
if(exists("croy_db")){
  coords_to_extract <- ahbdb %>%
    filter(!(geometry %in% unique(cryo_db$geometry))) %>%
    # Add unique ID column
    mutate(ID = 1:nrow(.))
} else {
  coords_to_extract <- ahbdb %>%
    # Add unique ID column
    mutate(ID = 1:nrow(.))
}
cat("done.\n")

# Status
cat("\nExtracting", nrow(coords_to_extract), "new records.",
    "\nTotal distinct locations in DB:", nrow(ahbdb),".\n\n")

## Load CHELSA TraCE21k data remotely

# Helper function to parse years into a more readable form
parse_year <- function(layer_names, var_name, new_var_name = NA){
  if(is.na(new_var_name)) new_var_name <- var_name
  new_layer_names <- layer_names %>% 
    gsub(paste0(".*", var_name, "_(.*)_.*"),
         paste0(new_var_name, "_\\1"), 
         .)
  return(new_layer_names)
}

# Mean temperature per century 
cat("Loading temperature data...")
CHELSA_temp <- read.csv("source_data/CHELSA_TraCE21k_bio01.txt", header = F) %>%
  lapply(function(x) paste0("/vsicurl/", x)) %>%
  pblapply(rast) %>% 
  rast()
# Rename temperature layers
names(CHELSA_temp)<- names(CHELSA_temp) %>% parse_year("bio01", "temp")
cat("done.\n")  

# Mean precipitation per century
cat("Loading precipitation data...")
CHELSA_precip <- read.csv("source_data/CHELSA_TraCE21k_bio12.txt", header = F) %>%
  lapply(function(x) paste0("/vsicurl/", x)) %>%
  pblapply(rast) %>% 
  rast()
# Rename precipitation layers
names(CHELSA_precip) <- names(CHELSA_precip) %>% parse_year("bio12", "precip")
cat("done.\n")  

# Digital elevation model
cat("Loading DEM data...")
CHELSA_dem <- read.csv("source_data/CHELSA_TraCE21k_dem.txt", header = F) %>%
  lapply(function(x) paste0("/vsicurl/", x)) %>%
  pblapply(rast) %>% 
  rast()
# Rename dem layers
names(CHELSA_dem) <- names(CHELSA_dem) %>% parse_year("dem")
cat("done.\n")  

# Glacial extent
cat("Loading list of glacial extent polygons")
CHELSA_gle_files <- read.csv("source_data/CHELSA_TraCE21k_gle_geom.txt", header = F) %>%
  sapply(function(x) paste0("/vsicurl/", x)) 
cat("done.\n")

## Load time-step look_up-table
time_id_look_up <- read.csv("source_data/time_id_look_up.csv")
# Helper function to look up k years BP
look_up_year <- function(time_id_vect){
  sapply(time_id_vect, 
         function(time_id){
           time_id_look_up$kBP[time_id == time_id_look_up$timeID]
         })
}

## Extract temperature, precipitation and elevation

# Temperature
cat("Extracting temperature values...")
cryo_db_new_extract <- terra::extract(CHELSA_temp, coords_to_extract) %>% 
  pivot_longer(2:ncol(.), 
                              names_to = "year_kBP", 
                              values_to = "CHELSA_TraCE21k_temp") %>% 
  mutate(year_kBP = gsub("temp_", "", year_kBP)) %>%
  mutate(year_kBP = look_up_year(year_kBP)) %>% 
  arrange(ID, year_kBP)
cat("done.\n")  

# Precipitation
cat("Extracting precipitation values...")
cryo_db_new_extract <- terra::extract(CHELSA_precip, coords_to_extract) %>% 
  pivot_longer(2:ncol(.), 
               names_to = "year_kBP", 
               values_to = "CHELSA_TraCE21k_precip") %>% 
  mutate(year_kBP = gsub("precip_", "", year_kBP)) %>%
  mutate(year_kBP = look_up_year(year_kBP)) %>%
  full_join(cryo_db_new_extract, .)
cat("done.\n")  

# Elevation
cat("Extracting elevation values...")
cryo_db_new_extract <- terra::extract(CHELSA_dem, coords_to_extract) %>% 
  pivot_longer(2:ncol(.), 
               names_to = "year_kBP", 
               values_to = "CHELSA_TraCE21k_elevation") %>% 
  mutate(year_kBP = gsub("dem_", "", year_kBP)) %>%
  mutate(year_kBP = look_up_year(year_kBP)) %>%
  full_join(cryo_db_new_extract, .)
cat("done.\n")  

# Calculate distance to land ice
cat("Calculating distance to glaciers...")

# Convert coordinates to extract to sf object
coords_to_extract_sf <- st_as_sf(coords_to_extract)

# Export helper funcitons and variables if needed
if(Sys.info()['sysname'] == "Windows"){
  clusterExport(cl, c("CHELSA_gle_files",
                  "coords_to_extract_sf",
                  "parse_year"))
} 

# Distance calculation
cryo_db_new_extract <- pblapply(seq_along(CHELSA_gle_files),
       function(index){
         cat(gsub(".*/(CHELSA_TraCE21k_gle_.*).gpkg", "\\1", 
              CHELSA_gle_files[index]), "\n")
         # Load geometries (remotely)
         CHELSA_gle <- read_sf(CHELSA_gle_files[index])
         # Get distances
         coords_to_extract_sf$CHESA_TraCE21k_dist_to_land_ice <- 
           st_distance(coords_to_extract_sf, CHELSA_gle) %>%
           as.numeric()
         # Parse and add year
         coords_to_extract_sf$year_kBP <- 
           gsub(".*/(CHELSA_TraCE21k_gle_.*).gpkg", "\\1", 
                CHELSA_gle_files[index]) %>% 
           parse_year("gle")
         # Return as data frame
         coords_to_extract_sf %>%
           st_drop_geometry() %>%
           select(-LonDD, -LatDD) %>%
           return()
       }
       # This takes a lot of resources, so run in parallel if possible
       , cl = cl
       ) %>%
  reduce(full_join) %>%
  mutate(year_kBP = gsub("gle_", "", year_kBP)) %>%
  mutate(year_kBP = look_up_year(year_kBP)) %>%
  full_join(cryo_db_new_extract, .)
cat("done.\n")

# Calculate dates last glaciated 
cat("Calculating dates last glaciated...")
cryo_db_new_extract <- pblapply(seq_along(unique(cryo_db_new_extract$ID)),
                              function(id){
                                # filter data by ID
                                data_sub <- filter(cryo_db_new_extract, ID == id)
                                # Get years glaciated
                                data_sub <- filter(data_sub, CHESA_TraCE21k_dist_to_land_ice == 0)
                                # Calculate max year glaciated and return as data frame for merging
                                return(data.frame(
                                  ID = id,
                                  CHELSA_TraCE21k_max_year_kBP_glaciated = 
                                    max(data_sub$year_kBP)
                                ))
                              }) %>%
  bind_rows() %>%
  full_join(cryo_db_new_extract, .)
cat("done.\n")

# Calculate date last below sea level
cat("Calculating dates last below sea-level")
cryo_db_new_extract <- pblapply(seq_along(unique(cryo_db_new_extract$ID)),
       function(id){
         # filter data by ID
         data_sub <- filter(cryo_db_new_extract, ID == id)
         # Filter out years below sea-level
         data_sub <- filter(data_sub, CHELSA_TraCE21k_elevation < 0)
         # Calculate max year below sea-level and return as dataframe for merging
         return(data.frame(
           ID = id,
           CHELSA_TraCE21k_max_year_kBP_with_elev_below_sea_level = 
             suppressWarnings(max(data_sub$year_kBP))
         ))
       }) %>%
  bind_rows() %>%
  full_join(cryo_db_new_extract, .)
cat("done.\n")

## Add coordinates and remove ID
cat("Adding coordinates...")
cryo_db_new_extract <- coords_to_extract %>%
  st_as_sf() %>%
  st_drop_geometry() %>%
  select(ID, LonDD, LatDD) %>%
  full_join(cryo_db_new_extract) %>%
  select(-ID)
cat("done.\n")

## Merge with existing data (if it exist)
cat("Merging with existing cryo_db...")
if(exists("cryo_db")){
cryo_db <- bind_rows(cryo_db,
                     cryo_db_new_extract)
} else {
  cryo_db <- cryo_db_new_extract
}

## Write / update database
write_csv(cryo_db, "cryo_db.csv")
cat("done.\n")

## Shut down parallel environment if needed
if(Sys.info()['sysname'] == "Windows"){
  stopCluster(cl)
} 

# Status
cat("cryo_db update complete.\n")

## EOF