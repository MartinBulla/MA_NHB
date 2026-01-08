# =============================================================
# Adjusted authors' code '04_R4_uneven_biodiversity_data_2023.R'
# Generates observation dataset aggregated by year and neighborhood along with neighborhood's area km2
#❗ Runs relative to the project's root directory, requires
#  '*_Aves_all_observations.Rdata' files in 'original_paper/Data/Biodiversity_holc_all' and 'original_paper/Data/Biodiv_Greeness_Social/soc_dem_max_2022_03_12 17_31_11.csv'
# exports files into ./Data/MaPe.
# ==============================================================

# load packages
pkgs <- c("data.table","dplyr","here","sf","stringr","tibble")
install_if_missing <- function(pkgs) {
  to_install <- setdiff(pkgs, rownames(installed.packages()))
  if (length(to_install)) install.packages(to_install, dependencies = TRUE)
  invisible(lapply(pkgs, require, character.only = TRUE))
}

install_if_missing(pkgs)

# --- --- ---
# [1] Loop through all  HOLC polygons with bird biodiversity data and
# count the number of observations per single HOLC polygon, and year
# --- --- ---

aves_obs = list.files(here::here('original_paper/Data/Biodiversity_holc_all'), pattern = 'Aves_all_observations.Rdata', full.names = T) # List all .Rdata files in input folder that contain bird biodiversity data 

# test start
i = unique(aves_obs)[1]
biodiv_data = aves_obs[str_detect(aves_obs, pattern = i)]
results <- sapply(biodiv_data, function(x) mget(load(x)), simplify = TRUE)  
obj <- results[[1]]   
nm <- names(obj)
nm[order(nm)]  
dt <- as.data.table(results[[1]]) 
# test end

u <- unique(aves_obs)
n <- length(u)
pb <- txtProgressBar(min = 0, max = n, style = 3)

save_all = FALSE # set to TRUE if you want to save all observations; creates 2GB dataset

# exporting data (aggregated and raw observations)
for(k in seq_along(u)) {
   # k = 5555
  i <- u[k]
  setTxtProgressBar(pb, k)
  
  print(paste(k, i))
  
  if(!any(str_detect(aves_obs, pattern = i))==TRUE){
    print(paste0(i, ' has no biodiversity data'))
    next
  }
  
  # Load the single polygon with bird biodiversity data
  biodiv_data = aves_obs[str_detect(aves_obs, pattern = i)]
  results <- sapply(biodiv_data, function(x) mget(load(x)), simplify = TRUE) 
  
  # Keep only desired columns as GBIF has 200+ columns
  mycols = c('species',
             'family',
             'genus',
             'decimalLongitude',
             'decimalLatitude',
             'collectionCode',
             'collectionID',
             'institutionCode',
             'year',
             'state',
             'city',
             'city_state',
             'holc_id',
             'holc_grade',
             #'species', MaPe removed duplicated species name
             'id')
  
  results <- lapply( results , "[", , mycols) 
  
  df <- do.call(rbind, results)
  d = data.table(df)  
  
  # stop running if holc_id not unique, i.e. A, B, C, D, E
  if(is.na(unique(d$id)) ){
    print(paste0('no unique id in', i))
    next # skips files without unique holc ids
  }
  
  # adjust variables
  d[, holc_polygon := gsub('.Rdata','', basename(i))]
  d[ ,lat:= decimalLatitude] # taking one lat value out of all
  d[ ,lon:= decimalLongitude] # taking one lon value out of all
  d[, id2 :=paste(city_state, holc_id)] # create unique ID

  # save all individual observations
  if(save_all) {
    d0 = d[, .(city_state, city, state, year, id, id2, holc_polygon, holc_grade, lat, lon, species, family, genus)] # unique lat/lon per observation
    exists <- file.exists('Data/MaPe/mape_DAT_all.csv')
    fwrite(d0, file = 'Data/MaPe/mape_DAT_all.csv', append = exists, col.names = !exists) 
  }

  # count per year and polygon (note that some ebird records have atlas data)
  d[ ,lat:= first(decimalLatitude)] # taking one lat value out of all
  d[ ,lon:= first(decimalLongitude)] # taking one lon value out of all

  dd = d[, list(sum_bird_obs = length(species)), by = list(city_state, city, state, year, id, holc_polygon, holc_grade, lat, lon)]
  
  exists1 <- file.exists('Data/MaPe/DAT_obs_year_polygon.csv')
  fwrite(dd, file = 'Data/MaPe/DAT_obs_year_polygon.csv', append = exists1, col.names = !exists1)   

  # 2000-2020 count per year, data source and polygon (note that some ebird records have atlas data)
  b =d[year >= 2000 & year <= 2020]  
  b[collectionCode %in%c('GBBC', 'EBIRD'), collectionCode := 'ebird']  
  b[institutionCode %in% 'iNaturalist', collectionCode := 'iNaturalist']
  b[!collectionCode%in%c('ebird','iNaturalist'), collectionCode:='other']
  
  bb = b[, list( sum_bird_obs = length(species)), by = list(city_state, city, state, year, collectionCode, id, holc_polygon, holc_grade, lat, lon)]  
  exists2 <- file.exists('Data/MaPe/DAT_obs_year_polygon_source.csv')
  fwrite(bb, file = 'Data/MaPe/DAT_obs_year_polygon_source.csv', append = exists2, col.names = !exists2)  
  #write.table(dd, file = "Data/MaPe/Mape_R1_biodiv_sum_bird_obs_by_holc_id_year_data-source.csv", append = T, row.names = F, col.names = F, sep = ",") 
}
close(pb)

# CHECK HOW WE STAND
#biodiv_sum = fread('Data/MaPe/2025-11-12_mape_num-of-obs_by_grade_year_polygon.csv') #biodiv_sum = fread('Data/MaPe/mape_DAT_all.csv')

#sum(biodiv_sum$sum_bird_obs) # author' comments indicateL "paper says 10,043,533 georeferenced ocurrences but I have 10,048,895 here"; we have 12,296,735

# --- --- ---
# Merge on to holc dataset 
# --- --- ---

# load downloaded Holc polygons from the Mapping Inequality project form the University of Richmond
#❗the original author's code uses shape file holc_ad_data.shp here, but that file has lower number of polygons and polygon ids that do not match those from raw Rdata observation files (used above); we thus use use the author's file that contains along holc parameters also holc soc dem information (albeit we do not know how that file was generated), as there the holc ids match those from the RData files

holc <- data.table(readr::read_csv('original_paper/Data/Biodiv_Greeness_Social/soc_dem_max_2022_03_12 17_31_11.csv'
                   , col_select = c(id : area_holc_km2
                                    , holc_tot_pop
                                    , msa_GEOID : msa_total_popE
                                    , msa_gini))
)

# keep, filter                 
holc = holc[!holc_grade%in%'E', .(id, state, city, holc_id, holc_grade, city_state, area_holc_km2, holc_tot_pop)]

# load the # observations per polygon, holc grade and year
o = fread('Data/MaPe/2025-11-12_mape_num-of-obs_by_grade_year_polygon.csv')

# check: lat/lon unique per id
chk <- o[, uniqueN(.SD), by = id, .SDcols = c("lat","lon")]
stopifnot(all(chk$V1 == 1L))

# aggregated per id-year (safeguard in case of duplicated rows per id-year)
o_sub <- o[, .(
  sum_bird_obs = sum(sum_bird_obs),
  lat          = first(lat),
  lon          = first(lon)
), by = .(id, year)]

# add one lat/lon per plygon to holc
holc = unique(o_sub[, .(id, lat, lon)], by = "id")[holc, on = "id"]

# set keys (helpful for joins/search)
setkey(o_sub, id, year)
setkey(holc, id)

# all id × year combos
grid <- CJ(id = unique(holc$id), year = 1932:2022, unique = TRUE)

# add static HOLC attributes by id
grid <- holc[grid, on = "id"]   # left-join: brings state/city/... to each (id,year)

# left-join per-year observations; keeps all `grid` rows
res <- o_sub[, .(id, year, sum_bird_obs)][grid, on = .(id, year)]

# fill zeros where a polygon-year had no observations
res[, sum_bird_obs := nafill(sum_bird_obs, fill = 0L)]

# enforce integer type
res[, sum_bird_obs := as.integer(sum_bird_obs)]

# final columns
out <- res[, .(id, state, city,  city_state, lat, lon, holc_id, holc_grade, area_holc_km2, holc_tot_pop, 
              year, sum_bird_obs)]

# checks: 
#stopifnot(nrow(out) == uniqueN(holc$id) * length(1932:2022)) # full id × year coverage
#stopifnot(!anyNA(holc$lat), !anyNA(holc$lon)) # all polygons got lat/lon
#ch = out[, sum(sum_bird_obs), id]
#ch[V1==0]

# ❗the lat/lon is missing for 943 polygons (those with no bird observation) and the authors' shape files (holc_ad_data.shp) contains only polygons where birds were observed; because 'o' already contains many polygons per city with stable coordinates, we use the mean of available polygon lat/lon within city_state as a reasonable fallback for missing polygons in the same city. It preserves city-level spatial context.

  # one lat/lon per id from obs where available
  id_latlon_obs <- unique(o[!is.na(lat) & !is.na(lon), .(id, lat, lon, city_state)], by = "id")
  
  # which ids in HOLC have no lat/lon in obs?
  missing_ids_dt   <- unique(out[is.na(lat), .(id, city_state)], by = "id")

  # mean lat/lon of polygons observed in that city
  city_centroids <- o[!is.na(lat) & !is.na(lon),
    .(lat = mean(lat), lon = mean(lon)),
    by = city_state
  ]

  # add lat/lon
  fixed_latlon = city_centroids[missing_ids_dt, on = "city_state"] 

  # set key
    setkey(out, id)
    setkey(fixed_latlon, id) 

  # fill only where missing, keep existing values otherwise
    out[fixed_latlon, on = .(id),
      `:=`(
        lat = fcoalesce(lat, i.lat),
        lon = fcoalesce(lon, i.lon)
      )
    ]

  # recheck 
    stopifnot(sum(is.na(out$lat) | is.na(out$lon)) == 0)

# export and clean
fwrite(out, 'Data/MaPe/DAT_obs_year_polygon.csv', yaml = TRUE)
gc()