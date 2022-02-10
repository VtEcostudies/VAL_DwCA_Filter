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

val_occ <- DBI::dbConnect(
  RPostgres::Postgres(),
  dbname = 'val_occurrences',
  host = 'localhost',
  port = 5432,
  user = 'postgres',
  password = 'EatArugula')

dbListTables(val_occ)

# define the source file for importing GBIF raw verbatim data (.txt, tab-delimited)
vrb_inp <- "C:/Users/jloomis/Documents/VCE/VAL_Data_Pipelines/VAL_DWcA_Split/dwca_gbif_occs_w_loc/verbatim.txt"
rm(gbif_raw) # clear gbif_raw
# read the data into R local memory, store it in 'gbif_raw', and time it
system.time(
  gbif_raw <- read.csv(
    vrb_inp,
    sep = "\t", # tab separated 
    quote = "") 
)

# write gbif_raw to the database 'val_occurrences', table 'verbatim_buffered'
system.time(
  sf::st_write(
    obj = gbif_raw,
    dsn = val_occ,
    layer = "verbatim_buffered",
    #delete_dsn = TRUE
    delete_dsn = FALSE, # delete dsn before writing, is this the db, or the table?
    delete_layer = FALSE, # do not delete the table
  )
)

system.time(
  res <- dbSendQuery(val_occ, "SELECT COUNT(*) FROM verbatim_buffered")
)
dbFetch(res)

library(readr)

# run filter query to filter postgres verabatim_buffered into verbatim_filtered
sql_file <- "C:/Users/jloomis/Documents/VCE/VAL_Data_Pipelines/VAL_DwCA_Split/repo/database/sql/filter_verbatim_buffered.sql"
sql_text <- read_file(sql_file)
system.time(
  res <- dbGetQuery(val_occ, sql_text)
)

# We may need to convert verbatim_filtered columns created as type BOOLEAN to type TEXT.
# In pgAdmin, create this function:
# .../VAL_Data_Pipelines/VAL_DwCA_Split/repo/database/sql/create_function_alter_table_bool_columns_to_text.sql
# In pgAdming, call this transaction:
# .../VAL_Data_Pipelines/VAL_DwCA_Split/repo/database/sql/alter_buffered_verbatim_bool_columns_to_text.sql

# read verbatim data no-location data into local memory
rm(gbif_raw) # clear gbif_raw
vrb_inp <- "C:/Users/jloomis/Documents/VCE/VAL_Data_Pipelines/VAL_DWcA_Split/dwca_gbif_occs_wo_loc/verbatim.txt"
system.time( # read the data into R local memory, store it in 'gbif_raw', and time it
  gbif_raw <- read.csv(
    vrb_inp,
    sep = "\t", # tab separated 
    quote = "") 
)

# Next: if we just import no-location data from verbatim.txt, we get "" values in the columns decimalLatitude 
# and decimalLongitude, which throws an error on st_write.
# We do not want to convert those columns to TEXT, so now we need to manipulate gbif_raw in memory to fix all those column values:

# IF these are the no-location data:
gbif_raw$decimalLatitude <- NULL
gbif_raw$decimalLongitude <- NULL

# append verbatim records to verbatim_filtered
system.time(
  sf::st_write(
    obj = gbif_raw,
    dsn = val_occ,
    layer = "verbatim_filtered",
    delete_dsn = FALSE, # delete dsn before writing, is this the db, or the table?
    delete_layer = FALSE, # do not delete the table
    append = TRUE) 
)

# IMPORTANT NOTE: On 1/29/2022 this showed 15,236 imported rows of 202,166 in verbatim.txt! Upon inspection, we found
# escape characters in row 15,236 which apparently interrupted the 'read.csv' local import, above. Removed bad data to
# proceed.  
# check you got them all in pgAdmin:
# select count(*) from verbatim_filtered where "decimalLongitude" IS NULL;

# run export query to copy postgres verbatim_filtered to disk
sql_file <- "C:/Users/jloomis/Documents/VCE/VAL_Data_Pipelines/VAL_DwCA_Split/repo/database/sql/export_verbatim_filtered.sql"
sql_text <- read_file(sql_file)
res <- dbSendQuery(val_occ, "SET client_encoding TO 'UTF8';")
system.time(
  res <- dbGetQuery(val_occ, sql_text)
)
