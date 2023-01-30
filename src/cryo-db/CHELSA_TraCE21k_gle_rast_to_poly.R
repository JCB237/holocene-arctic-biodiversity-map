# Quick helper script to convert the CHELSA TraCE21k raster into polygons
# needed for distance to glacial ice caluclations
# Jakob J. Assmann jakob.assmann@uzh.ch 18 January 2022

# Dependencies
library(terra)
library(dplyr)
library(parallel)
install.packages("pbapply")
library(pbapply)
library(parallel)
library(sf)

# To be run relative to cryo_db folder
# setwd("cryo_db")

# Load list of CHELSA gle files and prep for remote access
CHELSA_gle <- read.csv("source_data/CHELSA_TraCE21k_gle.txt",
                       header = F) %>%
  sapply(function(x) paste0("/vsicurl/", x)) 

# Convert rasters to polygons and write out as gpkg
pblapply(CHELSA_gle, 
         function(x){
           # Load raster
           x <- rast(x)
           # convert to polygons
           x <- as.polygons(x)
           # Write out as gpkg
           writeVector(x,
                       paste0("source_data/CHELSA_TraCE21_gle_geom/", 
                              names(x),
                              ".gpkg"),
                       overwrite = TRUE)
           # Status for good measure
           return("OK")
         },
         # Run in parallel (did not work on UZH ScienceCloud workers)
         #cl = 32
         )

# I estimate about 45 min for the calculations to be completed ;)
# Looks like it will be A LOT less!

# In fact, it ended up being 6 hours as I could not run it in parallel for some
# reason. 

# Check validity of geometry and fix if needed
CHELSA_gle_geom_files <- list.files("scratch", "gpkg", full.names = T)
make_valid_outcomes <- pblapply(CHELSA_gle_geom_files,
                                function(x){
                                  # Load geometries as sf object
                                  x_sf <- read_sf(x)
                                  # Check validity 
                                  if(st_is_valid(x_sf)){
                                    # If yes, confirm status
                                    file_is_valid <- T 
                                    repaired <- NA
                                  } else {
                                    # If not, attempt to make it valid
                                    file_is_valid <- F
                                    x_sf <- st_make_valid(x_sf)
                                    # Check status again
                                    if(st_is_valid(x_sf)){
                                      # If yes, overwrite file
                                      write_sf(x_sf, x, delete_dsn = T)
                                      # Set status
                                      repaired <- T
                                    } else{
                                      # If not set status
                                      repaired <- F
                                    }
                                  }
                                  # Return data frame with information
                                  return(data.frame(
                                    file = x,
                                    was_valid = file_is_valid,
                                    repaired = repaired
                                  ))   
                                },
                                cl = 16) %>%
  bind_rows()
view(make_valid_outcomes)