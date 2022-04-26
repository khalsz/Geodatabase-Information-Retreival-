
library(datamodelr)
library(DiagrammeR)
library(DiagrammeRsvg)
library(tidyverse)
library(dplyr)
library(dm)
library(tmap)
library(htmltools)


#Loading the data into R 
database <- 
  read.csv('C:/Users/khalsz/Documents/geodatabaseCW1.csv')

class(database)
#
head(database)

colnames(database)
database <- database %>% select(-X)
#printing out the columns in the database
colnames(database)




#checking if there is any column with completely unique values
length(apply( X = database, FUN = anyDuplicated, MARGIN = 2)) == length(names(database))

#Hence we need to give it a column with unique variables that will serve as it primary key
database<- database %>% mutate(ID = row_number())


#Checking if the combinationn of ID and Statue can make a unique composite primary key
nrow(unique(database[,c('statue', 'ID')])) == nrow(database)
#Hence the two are worth using as composite primary key


#SPlitting the variable that are not of atomic values in the depictedAltLabel column
#to make it conform with the first normal form


database$depictedAltLabel <- str_split(as.character(database$depictedAltLabel), ",")

database <- database %>% unnest(depictedAltLabel) 

database <- database %>% select(-depicted)
#Removed depicted column because it literately means the same as the statue column, 
#and this violates the 1NF rules. 
database$statueLabel <- str_replace_all(database$statueLabel, ',', ' ')
database$placeLabel <- str_replace_all(database$placeLabel, ',', ' ')
database$placeAdmin <- str_replace_all(database$placeAdmin, ',', ' ')
database$depictedLabel <- str_replace_all(database$depictedLabel, ',', ' ')
database$depictedDescription <- str_replace_all(database$depictedDescription, ',', ' ')


#SPliting the table to conform with second normal form. 
database1 <- database %>% select( statue, inception, ID)  %>% 
  group_by(statue, ID) %>% filter(row_number() == 1) 


database2 <- database %>% select(statue, place, placeLabel, lon, lat, 
                                 placeAdmin, depictedLabel,depictedAltLabel, 
                                 statueLabel, depictedDescription, 
                                 creator, creatorLabel)%>% group_by(statue) %>% 
  filter(row_number() == 1)


statue_DateTable <- database1
statueLoc_table <- database2 %>% ungroup() %>% select( statue, lon, lat, creator, place) %>% group_by(statue) %>% 
  filter(row_number() == 1)

statueplaceTable <- database2 %>% ungroup() %>% select(place, placeLabel,placeAdmin)%>% group_by(place) %>% 
  filter(row_number() == 1)
Depictedtable <- database2 %>% ungroup() %>% select(statue, depictedAltLabel,depictedLabel, depictedDescription) %>% 
  group_by(statue) %>% filter(row_number() == 1)

creatortable <- database2 %>% ungroup() %>% select(creator, creatorLabel) %>% group_by(creator) %>% 
  filter(row_number() == 1)

tablemodel <- dm(statue_DateTable, statueLoc_table, statueplaceTable,Depictedtable,creatortable )

names(tablemodel)

#Checking for the suitable primary key in the tables
dm_enum_pk_candidates(
  dm = tablemodel,
  table = statueLoc_table
)

dm_enum_pk_candidates(
  dm = tablemodel,
  table = statue_DateTable
)

dm_enum_pk_candidates(
  dm = tablemodel,
  table = statueplaceTable
)

dm_enum_pk_candidates(
  dm = tablemodel,
  table = Depictedtable
)

dm_enum_pk_candidates(
  dm = tablemodel,
  table = creatortable
)

#We can no add the identified primary keys
tablemodel_pks <-
  tablemodel %>%
  dm_add_pk(table = statueLoc_table, columns = statue ) %>%
  dm_add_pk(table = statue_DateTable, columns = ID) %>%
  dm_add_pk(table = statueplaceTable, columns = place) %>%
  dm_add_pk(table = Depictedtable, columns = statue) %>%
  dm_add_pk(table = creatortable, columns = creator) 

tablemodel_pks



#Checking the link between tables  statue_DateTable with statueLoc_table
dm_enum_fk_candidates(
  dm = tablemodel_pks,
  table = statue_DateTable,
  ref_table = statueLoc_table
)

#Checking the link between tables  statueLoc_table with Depictedtable
dm_enum_fk_candidates(
  dm = tablemodel_pks,
  table = statueLoc_table,
  ref_table = Depictedtable
)

#Checking the link between tables _table with statueplaceTable
dm_enum_fk_candidates(
  dm = tablemodel_pks,
  table = statueLoc_table,
  ref_table = statueplaceTable
)

#Checking the link between tables statueLoc_table with creatortable
dm_enum_fk_candidates(
  dm = tablemodel_pks,
  table = statueLoc_table,
  ref_table = creatortable
)


#Adding the foreign keys
complete_tablemodel <-
  tablemodel_pks %>%
  dm_add_fk(statue_DateTable, statue, statueLoc_table) %>%
  dm_add_fk(statueLoc_table, statue, Depictedtable) %>%
  dm_add_fk(statueLoc_table, place, statueplaceTable) %>%
  dm_add_fk(statueLoc_table, creator, creatortable)


complete_tablemodel

#Checking the integrity of the Database model. 
complete_tablemodel %>%
  dm_examine_constraints()

#Visualizing the database relationship model
complete_tablemodel %>%
  dm_draw() %>%
  DiagrammeRsvg::export_svg() %>%
  htmltools::HTML() %>%
  htmltools::html_print()

write_csv(statueLoc_table, 'statueLoc_table.csv')
write_csv(statue_DateTable, 'statue_DateTable.csv')
write_csv(statueplaceTable, 'statueplaceTable.csv')
write_csv(Depictedtable, 'Depictedtable.csv')
write_csv(creatortable, 'creatortable.csv')


#Part 2

# Load the RPostgreSQL library
library(RPostgreSQL)


# Initialise a database driver
# which manages the connections
pgsql_drv <- dbDriver("PostgreSQL")

pgsql_drv <- dbDriver('PostgreSQL')


# Connection information
pgsql_user <- "kal41"
pgsql_password <- "219031729"
pgsql_dbname <- "sds27"
pgsql_host <- "pgsql.mcs.le.ac.uk"
pgsql_port <- 5432

# Create the connection 
pgsql_conn <- dbConnect(
  pgsql_drv, 
  host = pgsql_host, port = pgsql_port,
  user = pgsql_user, 
  password = pgsql_password,
  dbname = pgsql_dbname
)

# Remove the connection information 
# from the R environment
rm(pgsql_user)
rm(pgsql_password)
rm(pgsql_dbname)
rm(pgsql_host)
rm(pgsql_port)

#Checking data types for each of the variables in table 
#greater_london_osm_point
dbGetQuery(
  conn = pgsql_conn,
  statement = "SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'greater_london_osm_point' ;"
) 


#Bicycle parking points in the study area: Havering
Cycle_P_in_Haerving <- dbGetQuery(
  conn = pgsql_conn,
  statement = "SELECT * 
FROM greater_london_osm_point glop 
INNER JOIN greater_london_loac gll 
ON st_intersects(
st_transform(glop.geom, 27700), 
st_transform(gll.geom, 27700) 
)
WHERE glop.other_tags like '%bicycle_parking%' AND 
gll.lad11nm = 'Havering';"
)

Cycle_P_in_Haerving
#Bicycle parking points in the study area: Havering
#Converted to human readable coordinate format 

EWKT_Cycle_P_in_Haerving <- dbGetQuery(
  conn = pgsql_conn,
  statement = "SELECT glop.name, glop.other_tags, gll.lad11cd, gll.oa_code, gll.lad11nm, 
ST_AsEWKT(glop.geom) geom_as_wkt, ST_AsEWKT(gll.geom) geom_as_wkt2
FROM greater_london_osm_point glop 
INNER JOIN greater_london_loac gll 
ON st_intersects(
  st_transform(glop.geom, 27700), 
  st_transform(gll.geom, 27700) 
)
WHERE glop.other_tags like '%bicycle_parking%' AND 
gll.lad11nm = 'Havering';"
)

EWKT_Cycle_P_in_Haerving %>% knitr::kable()


#checking if there is any duplicate point
#Based on id, there are unique bicycle parking
IDGrouped_Haerving_Cycle_P <- dbGetQuery(
  conn = pgsql_conn,
  statement = "SELECT glop.id, glop.geom, count(*)
FROM greater_london_osm_point glop 
INNER JOIN greater_london_loac gll 
ON st_intersects(
  st_transform(glop.geom, 27700), 
  st_transform(gll.geom, 27700) 
)
WHERE glop.other_tags like '%bicycle_parking%' AND 
gll.lad11nm = 'Havering'
GROUP BY glop.id;"
) %>% knitr::kable()

IDGrouped_Haerving_Cycle_P

#checking if there is any duplicate point
#Based on OA and Supgroup_CD, there are unique bicycle parking
OA_Grouped_Haerving_Cycle_P <- dbGetQuery(
  conn = pgsql_conn,
  statement = "SELECT gll.oa_code, gll.supgrp_cd, count(*)
FROM greater_london_osm_point glop 
INNER JOIN greater_london_loac gll 
ON st_intersects(
st_transform(glop.geom, 27700), 
st_transform(gll.geom, 27700) 
)
WHERE glop.other_tags like '%bicycle_parking%' AND 
gll.lad11nm = 'Havering'
GROUP BY gll.oa_code, gll.supgrp_cd;"
) %>% knitr::kable()

OA_Grouped_Haerving_Cycle_P

#checking if there is any duplicate point
#Based on OA and grp_cd, there are unique bicycle parking

CD_Grouped_Haerving_Cycle_P <- dbGetQuery(
  conn = pgsql_conn,
  statement = "SELECT gll.grp_cd, count(*)
FROM greater_london_osm_point glop 
INNER JOIN greater_london_loac gll 
ON st_intersects(
st_transform(glop.geom, 27700), 
st_transform(gll.geom, 27700) 
)
WHERE glop.other_tags like '%bicycle_parking%' AND 
gll.lad11nm = 'Havering'
GROUP BY gll.grp_cd;"
) %>% knitr::kable()

CD_Grouped_Haerving_Cycle_P

#Of the over 750 multipolygons in the area, only 37 intersects with 100 meter buffer 
#around the bicyle parking. 
hndBff_Haerving_Cycle_P <- dbGetQuery(
  conn = pgsql_conn,
  statement = "SELECT gll.geom, gll.id, gll.oa_code 
FROM greater_london_osm_point glop 
INNER JOIN greater_london_loac gll 
ON st_intersects(
st_buffer(
st_transform(glop.geom, 27700), 
100),
st_transform(gll.geom, 27700) 
)
WHERE glop.other_tags like '%bicycle_parking%' AND 
gll.lad11nm = 'Havering';"
) 
hndBff_Haerving_Cycle_P

#Roads that intersect bicycle parking points in  Havering
Road_int_Haerving_Cycle_P <- dbGetQuery(
  conn = pgsql_conn,
  statement = "WITH hr
as(
SELECT glop.geom, glop.osm_id, glop.name 
FROM greater_london_osm_point glop 
INNER JOIN greater_london_loac gll 
ON st_Within(
st_transform(glop.geom, 27700), 
st_transform(gll.geom, 27700) 
)
WHERE glop.other_tags like '%bicycle_parking%' AND 
gll.lad11nm = 'Havering'
)
SELECT hr.*, glol.geom , glol.name 
FROM hr 
INNER JOIN greater_london_osm_line glol  
ON st_intersects(st_transform(glol.geom, 27700),
st_transform(hr.geom, 27700))"
) 

Road_int_Haerving_Cycle_P


Road_19m_buff_Haerving_Cycle_P <- dbGetQuery(
  conn = pgsql_conn,
  statement = "WITH  hr
as(
SELECT glop.geom, glop.osm_id, glop.name 
FROM greater_london_osm_point glop 
INNER JOIN greater_london_loac gll 
ON st_intersects(
st_transform(glop.geom, 27700), 
st_transform(gll.geom, 27700) 
)
WHERE glop.other_tags like '%bicycle_parking%' AND 
gll.lad11nm = 'Havering'
)
SELECT hr.*, glol.geom , glol.name, glol.highway 
FROM hr 
INNER JOIN greater_london_osm_line glol  
ON st_intersects(st_buffer(st_transform(hr.geom, 27700),19
), st_transform(glol.geom, 27700))"
) 

Road_19m_buff_Haerving_Cycle_P

#Buildings that intersect bicycle parking in Havering 
Building_int_Haerving_Cycle_P <- dbGetQuery(
  conn = pgsql_conn,
  statement = "WITH  hm
as(
SELECT glop.geom, glop.osm_id, glop.name 
FROM greater_london_osm_point glop 
INNER JOIN greater_london_loac gll 
ON st_intersects(
st_transform(glop.geom, 27700), 
st_transform(gll.geom, 27700) 
)
WHERE glop.other_tags like '%bicycle_parking%' AND 
gll.lad11nm = 'Havering'
)
SELECT hm.geom, hm.osm_id, hm.name,  glop2.building, glop2.tourism, glop2.sport, glop2.office, glop2.geom 
FROM hm
INNER JOIN greater_london_osm_polygon glop2  
ON st_intersects(st_transform( hm.geom, 27700)
, st_transform(glop2.geom, 27700))
WHERE glop2.building IS NOT NULL"
) 
Building_int_Haerving_Cycle_P
#no building intersect

Building_50m_away_Haerving_Cycle_P <- dbGetQuery(
  conn = pgsql_conn,
  statement = "WITH  hm
as(
SELECT glop.geom, glop.osm_id, glop.name, glop.other_tags 
FROM greater_london_osm_point glop 
INNER JOIN greater_london_loac gll 
ON st_intersects(
st_transform(glop.geom, 27700), 
st_transform(gll.geom, 27700) 
)
WHERE glop.other_tags like '%bicycle_parking%' AND 
gll.lad11nm = 'Havering'
)
SELECT hm.geom, hm.osm_id, hm.name,  glop2.building, glop2.tourism, glop2.sport, glop2.office, glop2.geom, hm.other_tags 
FROM hm
INNER JOIN greater_london_osm_polygon glop2  
ON st_dwithin(st_transform( hm.geom, 27700)
, st_transform(glop2.geom, 27700), 50)
WHERE glop2.building IS NOT NULL"
) 

Building_50m_away_Haerving_Cycle_P 

tm_shape(Building_50m_away_Haerving_Cycle_P) +
  # Represent them as filled polygons
  tm_fill(col = "#f8ddbc") +
  # Add the line shapes
  tm_shape(building) +
  # Represent them as lines
  tm_lines(col = "#333333")