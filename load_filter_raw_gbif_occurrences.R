install.packages(c("RPostgres"), dependencies = TRUE)
install.packages(c("rpostgis"), dependencies = TRUE)
install.packages(c("sf"), dependencies = TRUE)
install.packages(c("readr"), dependencies = TRUE)

#some sql to execute (the 1st time) using pgAdmin, not here in R
#CREATE EXTENSION postgis
#SCHEMA public
#VERSION "3.1.0";
#
#CREATE DATABASE val_occurrences;

#if this is the first time, CREATE the db
res <- dbSendQuery(val_occ, "CREATE DATABASE val_occurrences")
dbFetch(res)
dbClearResult(res)

#connect to a local postres server running on Windows desktop
val_occ <- DBI::dbConnect(
  RPostgres::Postgres(),
  dbname = 'val_occurrences',
  host = 'localhost',
  port = 5432,
  user = 'postgres',
  password = 'EatArugula')

#show the tables in the db
tbl <- dbListTables(val_occ)

# define the source file for importing GBIF raw occurrence data (.txt, tab-delimited)
occ_inp <- "C:/Users/jloomis/Documents/VCE/VAL_Data_Pipelines/VAL_DWcA_Split/dwca_gbif_occs_w_loc/occurrence.txt"

# read the data into R local memory, store it in 'gbif_raw', and time it
system.time( # 547 seconds on 2021-09-08
  gbif_raw <- read.csv(
    occ_inp,
    sep = "\t", # tab separated 
    quote = "") 
)

# write gbif_raw to the database 'val_occurrences', table 'occurrence_buffered'
system.time( # 673 seconds on 2021-09-08
  sf::st_write(
    obj = gbif_raw,
    dsn = val_occ,
    layer = "occurrence_buffered",
    delete_dsn = TRUE, # delete dsn before writing, is this the db, or the table?
    delete_layer = TRUE, # delete the table
    append = FALSE)
)

#drop the output table, 'occurrence_filtered', first. It will cause the query in the file to fail.
dbSendQuery(val_occ, "DROP TABLE IF EXISTS occurrence_filtered;")

# run filter query to filter postgres occurrence_buffered into occurrence_filtered
library(readr)
sql_file <- "C:/Users/jloomis/Documents/VCE/VAL_Data_Pipelines/VAL_DwCA_Split/repo/database/sql/filter_occurrence_buffered.sql"
sql_text <- read_file(sql_file)
system.time(
  res <- dbGetQuery(val_occ, sql_text)
)

# Now we append Occurrences without Location to the filtered table (these were queried as having stateProvince=Vermont on GBIF)
# read occs without location data into local memory
rm(gbif_raw) # clear gbif_raw
occ_inp <- "C:/Users/jloomis/Documents/VCE/VAL_Data_Pipelines/VAL_DWcA_Split/dwca_gbif_occs_wo_loc/occurrence.txt"
system.time( # read the data into R local memory, store it in 'gbif_raw', and time it
  gbif_raw <- read.csv(
    occ_inp,
    sep = "\t", # tab separated 
    quote = "") 
)

# convert occurrence_filtered columns created as type BOOLEAN to type TEXT
# C:/Users/jloomis/Documents/VCE/VAL_Data_Pipelines/VAL_DwCA_Split/repo/database/sql/alter_table_occurrence_filtered_boolean_columns_to_text_function.sql
# Then call that function for this table:
# alter_table_bools("occurrence_filtered")

# ...hmm - that a very long-running query that needs to be repeated lots!

# IF these are the no-location data:
gbif_raw$decimalLatitude <- NULL
gbif_raw$decimalLongitude <- NULL

# append no-location occs to occurrence_filtered
system.time(
  sf::st_write(
    obj = gbif_raw,
    dsn = val_occ,
    layer = "occurrence_filtered",
    delete_dsn = FALSE, # delete dsn before writing, is this the db, or the table?
    delete_layer = FALSE, # do not delete the table
    append = TRUE) 
)

# check you got them all (in pgAdmin):
# select count(*) from occurrence_filtered where "decimalLongitude" IS NULL;

# run export query to copy postgres occurrence_filtered to disk
library(readr)
sql_file <- "C:/Users/jloomis/Documents/VCE/VAL_Data_Pipelines/VAL_DwCA_Split/repo/database/sql/export_occurrence_filtered.sql"
sql_text <- read_file(sql_file)
res <- dbSendQuery(val_occ, "SET client_encoding TO 'UTF8';")
system.time(
  res <- dbGetQuery(val_occ, sql_text)
)
