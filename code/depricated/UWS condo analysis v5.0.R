## Goal is to determine if Morningside Heights and Upper West Side generally are "Under-Condo'd"

setwd("/Users/billbachrach/Dropbox (hodgeswardelliott)/hodgeswardelliott Team Folder/Teams/Data/Bill Bachrach/Major projects/UWS condo prop")

library(maptools)
library(broom)
library(dplyr)
library(lubridate)
library(stringr)
library(sp)
library(geojsonio)
library(data.table)
library(parallel)


options(scipen=999)

workspacename <- "Data/NYC Condo Analysis"

## cool functions Tim made
source("/Users/billbachrach/Dropbox (hodgeswardelliott)/hodgeswardelliott Team Folder/Teams/Data/Bill Bachrach/useful functions/HWE_FUNCTIONS.r")
## cool thing time made to copy to clipboard 
source("/Users/billbachrach/Dropbox (hodgeswardelliott)/hodgeswardelliott Team Folder/Teams/Data/Bill Bachrach/useful functions/-.R")
## save workspace image with standardized format and date/time stamp
# source("/Users/billbachrach/Dropbox (hodgeswardelliott)/hodgeswardelliott Team Folder/Teams/Data/Bill Bachrach/useful functions/ws_image_save.R")
## get sizes for all objects in workspace 
# source("/Users/billbachrach/Dropbox (hodgeswardelliott)/hodgeswardelliott Team Folder/Teams/Data/Bill Bachrach/useful functions/workspace sizes.R")


## skip down to line 


# ## Script to get size of all objects in workspace 
# # source("/Users/billbachrach/Dropbox (hodgeswardelliott)/hodgeswardelliott Team Folder/Teams/Data/Bill Bachrach/useful functions/workspace sizes.R")
# 
# ## load most recent file
# # load("/Users/billbachrach/Dropbox (hodgeswardelliott)/hodgeswardelliott Team Folder/Teams/Data/Bill Bachrach/Major projects/UWS condo prop/Data/UWS Condo Analysis20161228_1216.RData")
# 
# ## Read and munge pluto and bytes of big apple 
# 
# pluto.files <- list.files("/Users/billbachrach/Dropbox (hodgeswardelliott)/hodgeswardelliott Team Folder/Teams/Data/Bill Bachrach/Turnover rate project/Data/nyc_pluto_16v1",
#            pattern=".csv",
#            full.names=T)
# 
# cl <- makeCluster(detectCores()-1,type="FORK")
# 
# # pluto.list <- parLapply(cl,
# pluto.list <- lapply(cl,
#                      pluto.files, function(x)
#                        read.csv(x,
#                                 stringsAsFactors=F)
# )
# 
# stopCluster(cl)
# 
# pluto <- bind_rows(pluto.list)
# 
# 
# pluto <- pluto %>% select(BBL,Borough,Block,Lot,ZipCode,Address,SplitZone,BldgClass,LandUse,Easements,OwnerType,
#                           OwnerName,BldgArea,ComArea,ResArea,OfficeArea,RetailArea,GarageArea,StrgeArea,
#                           FactryArea,OtherArea,AreaSource,NumBldgs,NumFloors,UnitsRes,UnitsTotal,YearBuilt,
#                           CondoNo,XCoord,YCoord) %>% 
#   mutate(BBL= paste(substr(BBL,start=1,stop=1),
#                     Block,
#                     Lot,
#                     sep="_"),
#          CondoNo= ifelse(CondoNo==0,
#                          0,
#                          paste(substr(BBL,start=1,stop=1),CondoNo,sep="_"))
#   )
# 
# # ## read in and munge PLUTO 
# # pluto <- read.csv("/Users/billbachrach/Dropbox (hodgeswardelliott)/hodgeswardelliott Team Folder/Teams/Data/Bill Bachrach/Turnover rate project/Data/nyc_pluto_16v1/MN.csv",
# #                   stringsAsFactors=F)
# # 
# # pluto[,"BBL"] <- paste(1,pluto[,"Block"],pluto[,"Lot"],sep="_")
# # 
# # pluto <- pluto %>% select(BBL,Block,Lot,ZipCode,Address,SplitZone,BldgClass,LandUse,Easements,OwnerType,
# #                           OwnerName,BldgArea,ComArea,ResArea,OfficeArea,RetailArea,GarageArea,StrgeArea,
# #                           FactryArea,OtherArea,AreaSource,NumBldgs,NumFloors,UnitsRes,UnitsTotal,YearBuilt,
# #                           CondoNo,XCoord,YCoord)
# 
# ## Using Rolling Sales data to come up with coop building types and to attach building type descriptions
# rollingsales <- fread("/Users/billbachrach/Dropbox (hodgeswardelliott)/hodgeswardelliott Team Folder/Teams/Data/Bill Bachrach/Turnover rate project/Data/rolling_sales_nodupes.csv",
#                       data.table=F,
#                       colClasses=rep("character",23))
# 
# ## generate the coop building classes 
# rollingsales.tmp <- rollingsales %>% 
#   arrange(desc(SALE.DATE)) %>% 
#   select(BUILDING.CLASS.AT.TIME.OF.SALE,BUILDING.CLASS.CATEGORY) %>% 
#   rename(BldgClass=BUILDING.CLASS.AT.TIME.OF.SALE,
#          Description=BUILDING.CLASS.CATEGORY)
# 
# rollingsales.small <- rollingsales.tmp[!duplicated(rollingsales.tmp),]
# 
# coopclasses <- rollingsales.small[grep("coop|co-op",rollingsales.small[,"Description"],ignore.case=T),"BldgClass"]
# 
# rm(rollingsales.small,rollingsales.tmp)
# 
# ## attach building type descriptions to pluto
# rollingsales.small <- rollingsales %>% 
#   filter(!duplicated(BUILDING.CLASS.AT.TIME.OF.SALE)) %>% 
#   select(BUILDING.CLASS.CATEGORY,BUILDING.CLASS.AT.TIME.OF.SALE)
# 
# colnames(rollingsales.small) <- c("Description","BldgClass")
# 
# pluto <- left_join(pluto,rollingsales.small,by="BldgClass") %>% 
#   select(ZipCode,Address,Borough,Block,Lot,BldgClass,NumBldgs,NumFloors,
#          UnitsRes,UnitsTotal,CondoNo,XCoord,YCoord,Description,BBL) %>% 
#   rename(Block_pluto = Block, Lot_pluto = Lot)
# 
# rm(rollingsales, rollingsales.small)
# 
# ## create flags for condo unit BBLs and condo lot BBLs
# pluto <- pluto %>%
#   mutate(condoflag_unitpluto = nchar(Lot_pluto)==4 & !(substr(Lot_pluto,start=1,stop=2)==75),
#          condoflag_lotpluto = nchar(Lot_pluto)==4 & (substr(Lot_pluto,start=1,stop=2)==75))
# 
# ## Bytes of the Big Apple PAD by BBL and Address
# boba_bbl.df <- fread("/Users/billbachrach/Dropbox (hodgeswardelliott)/hodgeswardelliott Team Folder/Teams/Data/Bill Bachrach/Data Sources/BOBA/bobabbl.txt",
#                      colClasses=rep("character",23),
#                      na.strings="    ",
#                      stringsAsFactors=F,
#                      data.table=F)
# 
# 
# ########################################################################
# ## Read in and manipulating Chris Whong data
# ## Note: this is derived from DOF dump "rawdata" 
# 
# ## Readin and keep only manhattan 
# bbls.df <- read.csv("/Users/billbachrach/Dropbox (hodgeswardelliott)/hodgeswardelliott Team Folder/Teams/Data/Bill Bachrach/Data Sources/tax_bills_june15_bbls.csv",
#                     stringsAsFactors=F) 
# # %>% 
# #   filter(substr(bbl,start=1,stop=1)=="1")
# 
# ## creating borough block and lot variables
# ## AND adding borough to condonumber
# bbls.df <- bbls.df %>% mutate(borough=substr(bbl,start=1,stop=1),
#                               block=substr(bbl,start=3,stop=6),
#                               lot=substr(bbl,start=7,stop=10),
#                               condonumber=ifelse(!is.na(condonumber),
#                                                  paste(borough,condonumber,sep="_"),
#                                                  NA)
#                                 )
# 
# ## creating flags for condo units (four character lot numbers) and condo billing lots (four characters that start with 75)
# bbls.df <- bbls.df %>% mutate(condoflag_lot = substr(lot,start=1,stop=1)!=0,
#                               condoflag_75 = substr(lot,start=1,stop=2)==75,
#                               condoflag_lotno = (substr(lot,start=1,stop=1) != 0 & 
#                                                    substr(lot,start=1,stop=2) != 75))
# 
# ## keeping only observations that are condos
# bbls.df <- bbls.df %>% 
#   filter(condo %in% c("unit","lot")) %>% 
#   arrange(desc(block))
# 
# ## creating BBL field that is in the same format as the other data
# bbls.df <- bbls.df %>% mutate(BBL=paste(as.numeric(as.character(borough)),
#                                         as.numeric(as.character(block)),
#                                         as.numeric(as.character(lot))
#                                         ,sep="_")
# )
# 
# 
# ####################################################################
# ## condo specific pluto dataframe
# pluto.con <- pluto %>% 
#   filter(!is.na(CondoNo) & CondoNo!=0) %>% 
#   arrange(desc(Block_pluto,Lot_pluto)) %>% 
#   mutate(borough.pl=substr(BBL,start=1,stop=1))
# 
# ## since we're grouping by the DOF condo identifier, some condo numbers have multiple BBLs attached
# ## creating secondary block and lot fields for each condo id
# pluto.con <- pluto.con %>% 
#   group_by(CondoNo) %>% 
#   mutate(Block_pluto2 = ifelse(n()>1,Block_pluto[2],NA),
#          Lot_pluto2 = ifelse(n()>1,Lot_pluto[2],NA)) %>% 
#   arrange(is.na(Block_pluto2),is.na(XCoord)) %>% 
#   filter(!duplicated(CondoNo))
# 
# ## new dataframe derived from bbls.df (only condos) and then joining with pluto
# condo.df <- bbls.df %>% 
#   select(BBL,ownername,taxclass,taxrate,condonumber,
#          borough,block,lot,condo,condoflag_lot,condoflag_75) %>% 
#   rename(CondoNo=condonumber) %>% 
#   filter(!is.na(CondoNo))
# 
# condo.df <- left_join(condo.df,pluto.con,by="CondoNo") %>% 
#   rename(BBL=BBL.x, BBL.lot=BBL.y)
# 
# ## test to see if there are duplicates 
# bbl.dupes <- condo.df[duplicated(condo.df[,"BBL"]),"BBL"]
# # View(condo.df %>% filter(BBL %in% bbl.dupes))
# ## for the moment, just excluding those BBLs, COME BACK TO THIS
# condo.df <- condo.df %>% filter(!BBL %in% bbl.dupes)
# 
# 
# 
# ## calculating number of actual condo units within each condo building
# ## NOTE: BBL has been put in far too redundantly, should go back and fix for production code
# derp <- as.data.frame(condo.df %>% 
#                         filter(condo=="unit" 
#                                # | condoflag_unitpluto==T
#                                & !grepl("commercial",taxclass)
#                         ) %>%
#                         group_by(CondoNo) %>% 
#                         summarize(count= n()),
#                       stringsAsFactors=F)
# 
# ## dataframe of the condo billing lots
# condo.lots <- condo.df %>% 
#   filter(condo=="lot" & !grepl("commercial",taxclass)) %>% 
#   filter(!duplicated(CondoNo))
# 
# ## joining the calculations with the data from condo.df (originally pluto)
# ## when joining, arranging with missing XCoord variable at bottom so that duplicate removal favors complete data
# derp <- left_join(derp,
#                   condo.df %>% 
#                     # mutate(borough = substr(BBL,start=1,stop=1)) %>% 
#                     select(CondoNo,BldgClass,NumBldgs,NumFloors,UnitsRes,
#                                       ZipCode,Address,Borough, borough, Block_pluto,Block_pluto2,Lot_pluto,
#                                       Lot_pluto2,XCoord,YCoord,Description),
#                   by="CondoNo") %>% 
#   arrange(is.na(XCoord))
# 
# ## joining with the condo lot BBL
# ## this is manhattan specific, fix that
# derp <- left_join(derp %>% filter(!duplicated(derp)),
#                   condo.lots %>% select(CondoNo,BBL)
# ) %>% 
#   mutate(BBL.new = paste(borough,Block_pluto,Lot_pluto,sep="_"))
# 
# ## remove duplicates and then choose the appropriate BBL 
# condo.df <- derp[!duplicated(derp),] %>% 
#   select(-BBL) %>% 
#   rename(BBL=BBL.new)
# # 
# # write.csv(condo.df,"condodf.csv",
# #           row.names=F)
# 
# ## pretty sure this is the number of condos
# total.condos <- sum(derp[,"count"])
# 
# ####################################
# ## moving on to coops 
# 
# ## manipulating BOBA BBL file 
# boba_bbl.df[,"BBL"] <- paste(trimws(as.numeric(boba_bbl.df[,"boro"])),
#                              trimws(as.numeric(boba_bbl.df[,"block"])),
#                              trimws(as.numeric(boba_bbl.df[,"lot"])),sep="_")
# 
# # dupes <- boba_bbl.df[,"BBL"] %in% boba_bbl.df[duplicated(boba_bbl.df[,"BBL"]),"BBL"]
# # boba_bbl.df.dupes <- boba_bbl.df[dupes,]
# # boba_bbl.df.hold <- boba_bbl.df
# boba_bbl.df <- boba_bbl.df[!duplicated(boba_bbl.df[,"BBL"]),]
# 
# boba_coop <- boba_bbl.df %>% filter(!is.na(coopnum))
# boba_coop <- boba_coop %>% 
#   mutate(BBL_orig = BBL,
#          BBL2=paste(boro,as.numeric(billblock),as.numeric(billlot),sep="_"),
#          BBL = ifelse(!(BBL %in% pluto[,"BBL"]),BBL2,BBL))
# 
# pluto.coop <- pluto %>% filter(BBL %in% boba_coop[,"BBL"])
# 
# coop.df <- left_join(boba_coop,pluto.coop,by="BBL") %>% 
#   filter(!grepl("RENTAL",Description,ignore.case=T))
# 
# ## droop roosevelt island and addresses individually verified as condo or rental
# drops.addresses <- c("80 NASSAU STREET","23 5 AVENUE","442 AMSTERDAM AVENUE","259 WEST 85 STREET",
#                      "16 EAST 84 STREET","108 EAST 96 STREET","2235 FREDRICK DOUGLASS BL")
# coop.df <- coop.df %>% filter(!(ZipCode %in% 10044) & 
#                                 !(Address %in% drops.addresses) & 
#                                 !duplicated(Address))
# 
# ## number of coop units 
# total.coops <- coop.df %>% 
#   filter(BldgClass %in% coopclasses) %>% 
#   summarize(units=sum(UnitsRes))
# 
# # write.csv(coop.df,"coopdf.csv",
# #           row.names=F)
# # write.csv(condo.df,"condodf.csv",
# #           row.names=F)
# 
# save.image(
#   paste(
#     "UWS Condo Analysis prior_to_shapefiles_"
#     ,format(
#       Sys.time(
#       )
#       ,"%Y%m%d %H%M%S"
#     )
#     ,".RData"
#     ,sep=""
#   )
# )
# 
# 
# 
# 
# #######################################
# ## Read in shapefiles 
# 
# ## For Zip Code
# # zip.map <- readOGR("/Users/billbachrach/Dropbox (hodgeswardelliott)/hodgeswardelliott Team Folder/Teams/Data/Bill Bachrach/Major projects/UWS condo prop/Data/cb_2015_us_zcta510_500k/cb_2015_us_zcta510_500k.shp"
# #                    , layer="cb_2015_us_zcta510_500k")
# # 
# # ## transform into spatialpolygondataframe w/ WGS84 as coordinate system
# # zip.map <- zip.map %>% spTransform(CRS("+proj=longlat +datum=WGS84"))
# # 
# # ## Keep only Zip Codes in manhattan
# # zip.keeps <- as.character(na.omit(pluto$ZipCode))
# # zip.keeps <- zip.keeps[!duplicated(zip.keeps)]
# # 
# # ## Apparently this is how one access objects inside lists inside spatialpolygondataframes
# # zip.map <- zip.map[zip.map@data$ZCTA5CE10 %in% zip.keeps, ]
# # 
# # ## turn into data frame using ggplot's fortify function
# # zip.f <- zip.map %>% fortify(region="ZCTA5CE10")
# # man.zips <- merge(zip.f,zip.map@data,by.x='id',by.y="ZCTA5CE10")
# 
# ## For Pediacities
# pediashape.url <- "http://data.beta.nyc//dataset/0ff93d2d-90ba-457c-9f7e-39e47bf2ac5f/resource/35dd04fb-81b3-479b-a074-a27a37888ce7/download/d085e2f8d0b54d4590b1e7d1f35594c1pediacitiesnycneighborhoods.geojson"
# 
# pedia.map <- geojson_read(as.location(pediashape.url),
#                           method="local",
#                           what="sp")
# 
# pedia.map <- pedia.map %>% spTransform(CRS("+proj=longlat +datum=WGS84"))
# # pedia.f <- pedia.map %>% ggplot2::fortify(region="neighborhood")
# pedia.f <- pedia.map %>% tidy(pedia.map,region="neighborhood")
# nyc.pedia <- merge(pedia.f,pedia.map@data,by.x="id",by.y="neighborhood")
# 
# 
# 
# 
# 
# 
# #########################################################################
# ## Add Neighborhoods to both dataframes 
# condo.df.hold <- condo.df
# # condo.df <- condo.df.hold
# 
# ## First the condos 
# 
# ## Convert NY State Plane X/Y Coords in Pluto to WGS 84
# tmp.df <- condo.df %>% filter(!is.na(XCoord))
# 
# data = tmp.df[,c("XCoord","YCoord")]
# colnames(data) <- c("lat","lon")
# coordinates(data) <- ~ lat+lon
# proj4string(data) <- CRS("+init=ESRI:102718")
# data.proj <- spTransform(data, CRS("+init=epsg:4326"))
# latlong <- as.data.frame(data.proj,stringsAsFactors=F)
# tmp.df <- as.data.frame(cbind(tmp.df[,"BBL"],latlong),
#                         stringsAsFactors=F)
# colnames(tmp.df) <- c("BBL","longitude","latitude")
# tmp.df[,"BBL"] <- as.character(tmp.df[,"BBL"])
# 
# condo.df <- left_join(condo.df,tmp.df,by="BBL")
# 
# 
# ## Point in polygon operations to add neighborhood names 
# tmp.df <- condo.df %>% filter(!is.na(longitude))
# dat <- tmp.df[,c("longitude","latitude","BBL")]
# colnames(dat) <- c("Longitude","Latitude","BBL")
# coordinates(dat) <- ~ Longitude + Latitude
# proj4string(dat) <- proj4string(pedia.map)
# tmp.vec <- over(dat, pedia.map)[,"neighborhood"]
# tmp.df[,"neighborhood"] <- tmp.vec
# 
# condo.df <- left_join(condo.df,tmp.df[,c("BBL","neighborhood")],by="BBL")
# 
# ## next the coops 
# 
# ## Convert NY State Plane X/Y Coords in Pluto to WGS 84
# # coop.df.hold <- coop.df
# 
# tmp.df <- coop.df %>% filter(!is.na(XCoord))
# 
# data = tmp.df[,c("XCoord","YCoord")]
# colnames(data) <- c("lat","lon")
# coordinates(data) <- ~ lat+lon
# proj4string(data) <- CRS("+init=ESRI:102718")
# data.proj <- spTransform(data, CRS("+init=epsg:4326"))
# latlong <- as.data.frame(data.proj,stringsAsFactors=F)
# tmp.df <- as.data.frame(cbind(tmp.df[,"BBL"],latlong),
#                         stringsAsFactors=F)
# colnames(tmp.df) <- c("BBL","longitude","latitude")
# tmp.df[,"BBL"] <- as.character(tmp.df[,"BBL"])
# 
# coop.df <- left_join(coop.df,tmp.df,by="BBL")
# 
# 
# ## Point in polygon operations to add neighborhood names 
# tmp.df <- coop.df %>% filter(!is.na(longitude))
# dat <- tmp.df[,c("longitude","latitude","BBL")]
# colnames(dat) <- c("Longitude","Latitude","BBL")
# coordinates(dat) <- ~ Longitude + Latitude
# proj4string(dat) <- proj4string(pedia.map)
# tmp.vec <- over(dat, pedia.map)[,"neighborhood"]
# tmp.df[,"neighborhood"] <- tmp.vec
# 
# coop.df <- left_join(coop.df,tmp.df[,c("BBL","neighborhood")],by="BBL")
# 
# ## put into one dataframe 
# ## NOTE: where there are overlaps, coops take precedence as they have been more thoroughly vetted
# condo.df.hold <- condo.df
# # condo.df <- condo.df.hold
# condo.df <- condo.df %>% 
#   filter(!BBL %in% coop.df[,"BBL"]) %>%
#   select(CondoNo,BBL,Address,BldgClass,count,ZipCode,longitude,latitude,neighborhood) %>% 
#   rename(Id=CondoNo,
#          UnitsRes=count,
#          Neighborhood=neighborhood) %>% 
#   mutate(Type="condo",
#          Id=as.character(Id)) 
# 
# coop.df.hold <- coop.df
# # coop.df <- coop.df.hold
# coop.df <- coop.df %>% 
#   select(coopnum,Address,BBL,BldgClass,UnitsRes,ZipCode,longitude,latitude,neighborhood) %>% 
#   rename(Id=coopnum,
#          Neighborhood=neighborhood) %>% 
#   mutate(Type="coop",
#          Id=as.character(Id))
# 
# new.df <- bind_rows(condo.df,coop.df)
# # new.df <- new.df  %>% mutate(Type=factor(Type))
# 
# # new.df <- rename(new.df,NAME=Neighborhood)
# 
# 
# ## All residential units 
# library(parallel)
# 
# ## get pluto back in
# 
# pluto.files <- list.files("/Users/billbachrach/Dropbox (hodgeswardelliott)/hodgeswardelliott Team Folder/Teams/Data/Bill Bachrach/Turnover rate project/Data/nyc_pluto_16v1",
#                           pattern=".csv",
#                           full.names=T)
# 
# cl <- makeCluster(detectCores()-1,type="FORK")
# 
# pluto.list <- parLapply(cl,pluto.files, function(x)
# # pluto.list <- lapply(pluto.files, function(x)
#   read.csv(x,
#            stringsAsFactors=F))
# 
# stopCluster(cl)
# 
# pluto <- bind_rows(pluto.list)
# 
# pluto <- pluto %>% select(BBL,Block,Lot,ZipCode,Address,BldgClass,
#                           BldgArea,ComArea,ResArea,OfficeArea,RetailArea,GarageArea,StrgeArea,
#                           FactryArea,OtherArea,AreaSource,NumBldgs,NumFloors,UnitsRes,UnitsTotal,YearBuilt,
#                           CondoNo,XCoord,YCoord)  %>% 
#   mutate(BBL= paste(substr(BBL,start=1,stop=1),
#                     Block,
#                     Lot,
#                     sep="_"),
#          CondoNo= ifelse(CondoNo==0,
#                          0,
#                          paste(substr(BBL,start=1,stop=1),CondoNo,sep="_"))
#   )
# 
# 
# ## Convert NY State Plane X/Y Coords in Pluto to WGS 84
# # coop.df.hold <- coop.df
# tmp.df <- pluto %>% filter(!is.na(XCoord))
# 
# data = tmp.df[,c("XCoord","YCoord")]
# colnames(data) <- c("lat","lon")
# coordinates(data) <- ~ lat+lon
# proj4string(data) <- CRS("+init=ESRI:102718")
# data.proj <- spTransform(data, CRS("+init=epsg:4326"))
# latlong <- as.data.frame(data.proj,stringsAsFactors=F)
# tmp.df <- as.data.frame(cbind(tmp.df[,"BBL"],latlong),
#                         stringsAsFactors=F)
# colnames(tmp.df) <- c("BBL","longitude","latitude")
# tmp.df[,"BBL"] <- as.character(tmp.df[,"BBL"])
# 
# pluto <- left_join(pluto,tmp.df,by="BBL")
# 
# 
# pluto.hold <- pluto
# ## Point in polygon operations to add neighborhood names 
# tmp.df <- pluto %>% filter(!is.na(longitude))
# dat <- tmp.df[,c("longitude","latitude","BBL")]
# colnames(dat) <- c("Longitude","Latitude","BBL")
# coordinates(dat) <- ~ Longitude + Latitude
# proj4string(dat) <- proj4string(pedia.map)
# tmp.vec <- over(dat, pedia.map)[,"neighborhood"]
# tmp.df[,"neighborhood"] <- tmp.vec
# 
# pluto <- left_join(pluto,tmp.df[,c("BBL","neighborhood")],by="BBL")
# # pluto[,"Type"] <- "all"
# # pluto[,"Id"] <- NA
# pluto <- pluto %>% 
#   filter(!BBL %in% new.df[,"BBL"]) %>% 
#   mutate(Type="all",
#          Id=NA) %>% 
#   rename(Neighborhood=neighborhood)
# pluto <- pluto[,colnames(new.df)]
# 
# 
# # colnames(new.df) %in% colnames(pluto)
# 
# # new.df <- new.df.hold
# # new.df.hold <- new.df
# new.df <- bind_rows(new.df,pluto)
# 
# 
# #______________________________________________________________________________________
# 
# 
# 
# 
# 
# 
# 
# ######################################################
# ## Calculate Metrics of Interest
# 
# ## By Zip code and for all of manhattan
# # out.z <- pluto %>% group_by(ZipCode) %>% summarize(total.units=sum(UnitsTotal),  
# #                                                  total.res=sum(UnitsRes),
# #                                                  condo.res=sum(UnitsRes[which(condoflag=="C")]),
# #                                                  coop.res=sum(UnitsRes[!is.na(coopnum) & coopnum!=0]),
# #                                                  total.buildings=n())
# # ## so the issue here is that a building that has the condo flag isn't necessarily ALL condos
# # ## and still unsure of the story with coops, probably should double check with the costar data 
# # 
# # 
# # out.z.all <- pluto  %>% summarize(ZipCode=NA,
# #                                 total.units=sum(UnitsTotal),  
# #                                 total.res=sum(UnitsRes),
# #                                 condo.res=sum(UnitsRes[which(condoflag=="C")]),
# #                                 coop.res=sum(UnitsRes[!is.na(coopnum) & coopnum!=0]),
# #                                 total.buildings=n())
# # 
# # out.z <- bind_rows(out.z,out.z.all)
# # 
# # out.z <- out.z %>% mutate(condo.prop=condo.res/total.res,
# #                       coop.prop=coop.res/total.res)
# 
# 
# ## By NAME and for all of manhattan
# neighbor.levs <- as.character(na.omit(unique(new.df[,"Neighborhood"])))
# out <- lapply(neighbor.levs, function(x){
#   restr <- new.df[,"Neighborhood"]==x
#   restr.con <- new.df[,"Type"]=="condo"
#   restr.coo <- new.df[,"Type"]=="coop"
#   restr.all <- new.df[,"Type"]=="all"
#   condos <- sum(new.df[restr & restr.con,"UnitsRes"],na.rm=T)
#   coops <- sum(new.df[restr & restr.coo,"UnitsRes"],na.rm=T)
#   all <- sum(new.df[restr & restr.all,"UnitsRes"],na.rm=T)
#   out <- c(x,condos,coops,all)
#   return(out)
# }
# )
# 
# man <- c("Manhattan",
#          sum(new.df[new.df[,"Type"]=="condo","UnitsRes"],na.rm=T),
#          sum(new.df[new.df[,"Type"]=="coop","UnitsRes"],na.rm=T),
#          sum(new.df[new.df[,"Type"]=="all","UnitsRes"],na.rm=T)
#          )
# 
# out.n <- as.data.frame(rbind(
#   do.call("rbind",out),
#   man),
#   stringsAsFactors=F)
# 
# colnames(out.n) <- c("Neighborhood","Condo","Coop","All")
# out.n <- out.n %>% mutate(Condo=as.numeric(as.character(Condo)),
#                           Coop=as.numeric(as.character(Coop)),
#                           All=as.numeric(as.character(All)),
#                           Rental=as.numeric(as.character(All-Condo-Coop)),
#                           Condo_Coop_Ratio=Condo/(Condo+Coop),
#                           Condo_all_Ratio=Condo/All,
#                           Coop_all_Ratio=Coop/All,
#                           Rental_all_Ratio=Rental/All,
#                           Owner_Rent_Ratio=(Condo+Coop)/(Condo+Coop+All)) %>% 
#   select(Neighborhood,Condo,Condo_all_Ratio,Coop,Coop_all_Ratio,
#          Rental,Rental_all_Ratio,All,Owner_Rent_Ratio,Condo_Coop_Ratio)
# 
# # out.n <- new.df %>% group_by(Type,Neighborhood) %>% 
# #   # group_by(Type) %>% 
# #   summarize(number=sum(UnitsRes,na.rm=T))
# 
# # out.n.all <- pluto  %>% summarize(NAME=NA,
# #                                 total.units=sum(UnitsTotal),  
# #                                 total.res=sum(UnitsRes),
# #                                 condo.res=sum(UnitsRes[which(condoflag=="C")]),
# #                                 coop.res=sum(UnitsRes[!is.na(coopnum) & coopnum!=0]),
# #                                 total.buildings=n())
# # 
# # out.n <- bind_rows(out.n,out.n.all)
# # 
# # out.n <- out.n %>% mutate(condo.prop=condo.res/total.res,
# #                       coop.prop=coop.res/total.res)
# 
# 
# ### ATTENTION
# ## Use script "ACS_readin_munge" to put ACS data in appropriate format and output dataframe "nbrhd.<ACS name>"
# ### EXTERNAL SCRIPT NEEDED 
# 
# n_acs.match <- match(out.n[,"Neighborhood"],nbrhd.ern[,"neighborhood"])
# out.n[,"Tot100k"] <- nbrhd.ern[n_acs.match,"Tot100k"]
# out.n[,"TotPop"] <- nbrhd.ern[n_acs.match,"TotPop"]
# out.n[,"BoroCode"] <- nbrhd.ern[n_acs.match,"BoroCode"]
# # out.n[,"Condo_100k_Ratio"] <- out.n[,"Condo"]/out.n[,"Tot100k"]
# # out.n[,"Own_100k_Ratio"] <- (out.n[,"Condo"]+out.n[,"Coop"])/out.n[,"Tot100k"]
# 
# out.n <- out.n %>% mutate(Perc100k = Tot100k/TotPop,
#                           Condo_100k_Ratio = Condo/Tot100k,
#                           Own_100k_Ratio = (Condo + Coop) / Tot100k,
#                           # Condo_100k_Ratio = ifelse((Condo_100k_Ratio>1 & Perc100k < .2)
#                           #                           ,NA,Condo_100k_Ratio),
#                           # Condo_100k_Ratio = ifelse((Condo_100k_Ratio>1 & (Perc100k < .2 | Tot100k < 1000))
#                           #                           ,NA,Condo_100k_Ratio),
#                           Condo_100k_Ratio = ifelse(Condo_100k_Ratio>1.5
#                                                     ,1.5,Condo_100k_Ratio),
#                           Condo_100k_Ratio = ifelse(Condo_100k_Ratio < 0.01 & (Perc100k < .2 | Tot100k < 500),
#                                                     NA,Condo_100k_Ratio),
#                           # Own_100k_Ratio = ifelse(Own_100k_Ratio>2 & (Perc100k < .2 | Tot100k < 500)
#                           #                         ,2,Own_100k_Ratio),
#                           Own_100k_Ratio = ifelse(Own_100k_Ratio>3
#                                                   ,3,Own_100k_Ratio),
#                           Own_100k_Ratio = ifelse(Own_100k_Ratio < 0.01 & (Perc100k < .2 | Tot100k < 500),
#                                                   NA,Own_100k_Ratio)
# ) 
# # %>% filter(BoroCode!=5)
# 
# 
# colnames(out.n)
# out.n <- out.n %>%
#   mutate(Condo_100k_Ratio = ifelse(grepl("roosevelt",Neighborhood,ignore.case=T),NA,Condo_100k_Ratio),
#          Own_100k_Ratio = ifelse(grepl("roosevelt",Neighborhood,ignore.case=T),NA,Own_100k_Ratio))
# 
# View(out.n %>% arrange(Own_100k_Ratio))



## writing to CSV to make preliminary Excel PDF
# write.csv(out.z,"out.csv")
# write.csv(out.n,"out_neighborhood.csv",
#           row.names=F)

#___________________________________________________________________________________________





## Pre-compiled data readin

supply.df <- readRDS("/Users/billbachrach/Dropbox (hodgeswardelliott)/hodgeswardelliott Team Folder/Teams/Data/Bill Bachrach/ad_hoc/Multifamily pipeline/Data/multifam_supply20170626_181226.rds") %>% 
  mutate(Multi_Total = Rentals + Condo + Coop
         ,Owner_Rent_Ratio = (Multi_Total - Rentals.adj) / Multi_Total
         ,Renter_ratio = Rentals.adj / Multi_Total
  )

################################
## GGMaps!!! 

## By Neighborhood 

## original
tmp.df <- out.n %>% 
  filter(!is.na(Neighborhood) & Neighborhood != "Manhattan") %>% 
  rename(id = Neighborhood)

## using new calculations 
tmp.df.2016 <- supply.df %>% 
  filter(AreaType %in% "Neighborhood" & Year %in% 2016)
  
tmp.df.1980 <- supply.df %>% 
  filter(AreaType %in% "Neighborhood" & Year %in% 1980)

gg.df <- nyc.pedia %>% 
  filter(!borough %in% "Staten Island")

gg.df.1980 <- left_join(gg.df
                        ,tmp.df.1980 %>% 
                          select(-AreaType,-Year) %>%
                          rename(id = AREA)
                        ,by="id"
)


gg.df <- left_join(gg.df
                   ,tmp.df.2016 %>% 
                     select(-AreaType,-Year) %>%
                     rename(id = AREA)
                   ,by="id"
)



# gg.df <- filter(gg.df,BoroCode != 5)
# gg.df <- filter(gg.df,!borough %in% "Staten Island")
gg.df.hold <- gg.df
gg.df.1980.hold <- gg.df.1980


library(ggplot2)
library(ggmap)

nyc.box <- make_bbox(gg.df[,"long"],gg.df[,"lat"]
                    # ,f=-1
                    )

mn.box <- make_bbox(
  (gg.df %>% 
    filter(borough %in% "Manhattan"))[,"long"]
  ,(gg.df %>% 
      filter(borough %in% "Manhattan"))[,"lat"]
)

mapImage.nyc.s.terline <- get_map(location = nyc.box,
                        maptype="terrain-lines",
                        source = "stamen",
                        color = "bw")


mapImage.mn.s.terline <- get_map(location = mn.box,
                                  maptype="terrain-lines",
                                  source = "stamen",
                                  color = "bw")

## condo / coop proportions 
condo.nbrhd.p <- ggmap(mapImage.nyc.s.terline
                       ,extent='normal') + 
  geom_polygon(aes(fill=Condo_Coop_Ratio,x=long,y=lat,group=group),
               data=gg.df,
               alpha=0.8,
               color="black",
               size= 0.2) + 
  labs(x="",
       y="",
       fill="% Condo",
       title="Condos as a Percentage of All \nOwner Occupied Units by Neighborhood",
       caption="Source:\nDOF PLUTO Database; DOF Property Assessment Data; \nDCP Property Address Directory; Zillow") +
  scale_x_continuous(labels = NULL, limits=c(nyc.box["left"],nyc.box["right"])) +
  scale_y_continuous(labels = NULL, limits=c(nyc.box["bottom"],nyc.box["top"])) +
  # annotate('rect', xmin=x.min, ymin=y.min, xmax=x.max, ymax=y.max, color= "black", fill="grey80") +
  # annotate("text", x=xcenter, y=ycenter, label = "Morningside Heights", colour = I("black"), size = 3) +
  # annotate("segment", x=xcenter, xend=ms.center[1], y=y.min, yend=ms.center[2],
  #          colour=I("black"), arrow = arrow(length=unit(0.1,"cm")), size = .75) +
  scale_fill_continuous(labels = scales::percent) + 
  theme_hwe() + 
  theme(panel.grid.major = element_blank(),
        plot.caption = element_text(hjust=0,vjust=15),
        plot.title = element_text(vjust=-10)
  )

condo.nbrhd.p


## own to rent proportions 
# gg.df <- gg.df.hold %>% 
#   filter(borough %in% "Manhattan") %>% 
#   mutate(Owner_Rent_Ratio = ifelse(Owner_Rent_Ratio > .9,NA,Owner_Rent_Ratio))
# 
# ownrent.nbrhd.2016.p <- ggmap(mapImage.mn.s.terline
#                        ,extent='normal') + 
#   geom_polygon(aes(fill=Owner_Rent_Ratio,x=long,y=lat,group=group),
#                data=gg.df,
#                alpha=0.8,
#                color="black",
#                size= 0.2) + 
#   labs(x="",
#        y="",
#        fill="% Condo/Coop",
#        title="Condos/Coops as % of Residential Stock - 2016",
#        subtitle="\n\nExcluding Single and Two Family Structures",
#        caption="Source:\nDOF PLUTO Database; DOF Property Assessment Data; \nDCP Property Address Directory; PediaCities") +
#   scale_x_continuous(labels = NULL, limits=c(mn.box["left"],mn.box["right"])) +
#   scale_y_continuous(labels = NULL, limits=c(mn.box["bottom"],mn.box["top"])) +
#   scale_fill_continuous(limits=c(0,.4),labels = scales::percent) + 
#   theme_hwe() + 
#   theme(panel.grid.major = element_blank(),
#         plot.caption = element_text(hjust=0,vjust=15),
#         plot.title = element_text(vjust=-10)
#   )
# 
# ownrent.nbrhd.2016.p
# 
# 
# gg.df <- gg.df.1980.hold %>% 
#   filter(borough %in% "Manhattan") %>% 
#   mutate(Owner_Rent_Ratio = ifelse(Owner_Rent_Ratio > .9,NA,Owner_Rent_Ratio))
# 
# 
# 
# ownrent.nbrhd.1980.p <- ggmap(mapImage.mn.s.terline
#                               ,extent='normal') + 
#   geom_polygon(aes(fill=Owner_Rent_Ratio,x=long,y=lat,group=group),
#                data=gg.df,
#                alpha=0.8,
#                color="black",
#                size= 0.2) + 
#   labs(x="",
#        y="",
#        fill="% Condo/Coop",
#        title="Condos/Coops as % of Residential Stock - 1980",
#        subtitle="\n\nExcluding Single and Two Family Structures",
#        caption="Source:\nDOF PLUTO Database; DOF Property Assessment Data; \nDCP Property Address Directory; PediaCities") +
#   scale_x_continuous(labels = NULL, limits=c(mn.box["left"],mn.box["right"])) +
#   scale_y_continuous(labels = NULL, limits=c(mn.box["bottom"],mn.box["top"])) +
#   scale_fill_continuous(limits=c(0,.4),labels = scales::percent) + 
#   theme_hwe() + 
#   theme(panel.grid.major = element_blank(),
#         plot.caption = element_text(hjust=0,vjust=15),
#         plot.title = element_text(vjust=-10)
#   )
# 
# ownrent.nbrhd.1980.p



## rentals as a percent of all units 
gg.df <- gg.df.hold %>% 
  filter(borough %in% "Manhattan")

renter.nbrhd.2016.p <- ggmap(mapImage.mn.s.terline
                              ,extent='normal') + 
  geom_polygon(aes(fill=Renter_ratio,x=long,y=lat,group=group),
               data=gg.df,
               alpha=0.8,
               color="black",
               size= 0.2) + 
  labs(x="",
       y="",
       fill="% Rental",
       title="Rentals as % of Residential Stock - 2016",
       subtitle="\n\nIncluding Condo/Coop Rentals; Excluding Single and Two Family Structures",
       caption="Source:\nDOF PLUTO Database; DOF Property Assessment Data; \nDCP Property Address Directory; PediaCities") +
  scale_x_continuous(labels = NULL, limits=c(mn.box["left"],mn.box["right"])) +
  scale_y_continuous(labels = NULL, limits=c(mn.box["bottom"],mn.box["top"])) +
  scale_fill_continuous(limits=c(.5,1),labels = scales::percent) +
  theme_hwe() + 
  theme(panel.grid.major = element_blank(),
        plot.caption = element_text(hjust=0,vjust=15),
        plot.title = element_text(vjust=-10)
  )

renter.nbrhd.2016.p


gg.df <- gg.df.1980.hold %>% 
  filter(borough %in% "Manhattan")

renter.nbrhd.1980.p <- ggmap(mapImage.mn.s.terline
                              ,extent='normal') + 
  geom_polygon(aes(fill=Renter_ratio,x=long,y=lat,group=group),
               data=gg.df,
               alpha=0.8,
               color="black",
               size= 0.2) + 
  labs(x="",
       y="",
       fill="% Rental",
       title="Rentals as % of Residential Stock - 1980",
       subtitle="\n\nIncluding Condo/Coop Rentals; Excluding Single and Two Family Structures",
       caption="Source:\nDOF PLUTO Database; DOF Property Assessment Data; \nDCP Property Address Directory; PediaCities") +
  scale_x_continuous(labels = NULL, limits=c(mn.box["left"],mn.box["right"])) +
  scale_y_continuous(labels = NULL, limits=c(mn.box["bottom"],mn.box["top"])) +
  scale_fill_continuous(limits=c(.5,1),labels = scales::percent) +
  theme_hwe() + 
  theme(panel.grid.major = element_blank(),
        plot.caption = element_text(hjust=0,vjust=15),
        plot.title = element_text(vjust=-10)
  )

renter.nbrhd.1980.p



## condo as percent of all units 
# gg.df <- gg.df %>% mutate(Condo_all_Ratio=ifelse(Condo_all_Ratio>.3,NA,Condo_all_Ratio))
condoall.nbrhd.p <- ggmap(mapImage.nyc
                         ,extent='normal') + 
  geom_polygon(aes(fill=Condo_all_Ratio,x=long,y=lat,group=group),
               data=gg.df,
               alpha=0.8,
               color="white",
               size= 0.2) + 
  labs(x="",
       y="",
       fill="% Condo",
       title="Condos as a Percentage of All \nResi Units by Neighborhood",
       caption="Source:\nDOF PLUTO Database; DOF Property Assessment Data; \nDCP Property Address Directory; Zillow") +
  scale_x_continuous(labels = NULL, limits=c(nyc.box["left"],nyc.box["right"])) +
  scale_y_continuous(labels = NULL, limits=c(nyc.box["bottom"],nyc.box["top"])) +
  # annotate('rect', xmin=x.min, ymin=y.min, xmax=x.max, ymax=y.max, color= "black", fill="grey80") +
  # annotate("text", x=xcenter, y=ycenter, label = "Morningside Heights", colour = I("black"), size = 3) +
  # annotate("segment", x=xcenter, xend=ms.center[1], y=y.min, yend=ms.center[2],
  #          colour=I("black"), arrow = arrow(length=unit(0.1,"cm")), size = .75) +
  scale_fill_continuous(labels = scales::percent,
                        high="darkslategray1",
                        low="darkslategray") + 
  # scale_color_continuous(low="skyblue",high="darkgrey") + 
  theme_hwe() + 
  theme(panel.grid.major = element_blank(),
        plot.caption = element_text(hjust=0,vjust=15),
        plot.title = element_text(vjust=-10)
  )
condoall.nbrhd.p


## coops as a percent of all units 
coopall.nbrhd.p <- ggmap(mapImage.nyc
                          ,extent='normal') + 
  geom_polygon(aes(fill=Coop_all_Ratio,x=long,y=lat,group=group),
               data=gg.df,
               alpha=0.8,
               color="white",
               size= 0.2) + 
  labs(x="",
       y="",
       fill="% Co-op",
       title="Co-ops as a Percentage of All \nMultifamily Units by Neighborhood",
       caption="Source:\nDOF PLUTO Database; DOF Property Assessment Data; \nDCP Property Address Directory; Zillow") +
  scale_x_continuous(labels = NULL, limits=c(nyc.box["left"],nyc.box["right"])) +
  scale_y_continuous(labels = NULL, limits=c(nyc.box["bottom"],nyc.box["top"])) +
  # annotate('rect', xmin=x.min, ymin=y.min, xmax=x.max, ymax=y.max, color= "black", fill="grey80") +
  # annotate("text", x=xcenter, y=ycenter, label = "Morningside Heights", colour = I("black"), size = 3) +
  # annotate("segment", x=xcenter, xend=ms.center[1], y=y.min, yend=ms.center[2],
  #          colour=I("black"), arrow = arrow(length=unit(0.1,"cm")), size = .75) +
  scale_fill_continuous(labels = scales::percent) + 
  theme_hwe() + 
  theme(panel.grid.major = element_blank(),
        plot.caption = element_text(hjust=0,vjust=15),
        plot.title = element_text(vjust=-10)
        )

coopall.nbrhd.p


## rentals as a percent of all units 
rentalall.nbrhd.p <- ggmap(mapImage.nyc
                         ,extent='normal') + 
  geom_polygon(aes(fill=Rental_all_Ratio,x=long,y=lat,group=group),
               data=gg.df,
               alpha=0.8,
               color="white",
               size= 0.2) + 
  labs(x="",
       y="",
       fill="% Rental",
       title="Rentals as a Percentage of All \nMultifamily Units by Neighborhood",
       caption="Source:\nDOF PLUTO Database; DOF Property Assessment Data; \nDCP Property Address Directory; Zillow") +
  scale_x_continuous(labels = NULL, limits=c(nyc.box["left"],nyc.box["right"])) +
  scale_y_continuous(labels = NULL, limits=c(nyc.box["bottom"],nyc.box["top"])) +
  # annotate('rect', xmin=x.min, ymin=y.min, xmax=x.max, ymax=y.max, color= "black", fill="grey80") +
  # annotate("text", x=xcenter, y=ycenter, label = "Morningside Heights", colour = I("black"), size = 3) +
  # annotate("segment", x=xcenter, xend=ms.center[1], y=y.min, yend=ms.center[2],
  #          colour=I("black"), arrow = arrow(length=unit(0.1,"cm")), size = .75) +
  scale_fill_continuous(labels = scales::percent) + 
  theme_hwe() + 
  theme(panel.grid.major = element_blank(),
        plot.caption = element_text(hjust=0,vjust=15),
        plot.title = element_text(vjust=-10)
  )

rentalall.nbrhd.p



## own to 100k ratio 
own100k.nbrhd.p <- ggmap(mapImage.nyc
                           ,extent='normal') + 
  geom_polygon(aes(fill=Own_100k_Ratio,x=long,y=lat,group=group),
               data=gg.df,
               alpha=0.9,
               color="black",
               size= 0.2) + 
  labs(x="",
       y="",
       fill="Owned Resi Units\nPer > $100k Earner",
       title="Owned Residential Units to Six Figure Earners",
       caption="Source:\nDOF PLUTO Database; DOF Property Assessment Data; PediaCities;\nDCP Property Address Directory; American Community Survey") +
  scale_x_continuous(labels = NULL, limits=c(nyc.box["left"],nyc.box["right"])) +
  scale_y_continuous(labels = NULL, limits=c(nyc.box["bottom"],nyc.box["top"])) +
  scale_fill_continuous(high="black",
                        low="lightblue") +
  # annotate('rect', xmin=x.min, ymin=y.min, xmax=x.max, ymax=y.max, color= "black", fill="grey80") +
  # annotate("text", x=xcenter, y=ycenter, label = "Morningside Heights", colour = I("black"), size = 3) +
  # annotate("segment", x=xcenter, xend=ms.center[1], y=y.min, yend=ms.center[2],
  #          colour=I("black"), arrow = arrow(length=unit(0.1,"cm")), size = .75) +
  # scale_fill_continuous(labels = scales::percent) + 
  theme_hwe() + 
  theme(panel.grid.major = element_blank(),
        plot.caption = element_text(hjust=0,vjust=10),
        plot.title = element_text(vjust=-10)
  )

own100k.nbrhd.p

perc100k.4Q <- as.numeric(tmp.df %>% arrange(Perc100k) %>% summarize(fourthquart = Perc100k[round(sum(!is.na(Perc100k)))*.75]))
gg.df.tmp <- gg.df %>% filter(Perc100k >= perc100k.4Q & !is.na(Condo_100k_Ratio))

## condo to 100k ratio 
condo100k.nbrhd.p <- ggmap(mapImage.nyc
                         ,extent='normal') + 
  geom_polygon(aes(x=long,y=lat,group=group),
               data=gg.df,
               fill="white",
               alpha=0.9,
               color="grey20",
               size= 0.05) + 
  geom_polygon(aes(fill=Condo_100k_Ratio,x=long,y=lat,group=group),
               data=gg.df,
               alpha=0.15,
               color=NA,
               size= 0.01) +
  geom_polygon(aes(fill=Condo_100k_Ratio,x=long,y=lat,group=group),
               data=gg.df.tmp,
               alpha=1,
               color="black",
               size= 0.25) + 
  labs(x="",
       y="",
       fill="Condo Units\nPer > $100k Earner",
       title="Condos to Six Figure Earners",
       subtitle="Among NYC Neighborhoods in Top 25% of Six Figure Earners",
       caption="Source:\nDOF PLUTO Database; DOF Property Assessment Data; PediaCities;\nDCP Property Address Directory; American Community Survey") +
  scale_x_continuous(labels = NULL, limits=c(nyc.box["left"],nyc.box["right"])) +
  scale_y_continuous(labels = NULL, limits=c(nyc.box["bottom"],nyc.box["top"])) +
  scale_fill_continuous(high="black",
                        low="lightblue") +
  # scale_fill_continuous(labels = scales::percent) + 
  theme_hwe() + 
  theme(panel.grid.major = element_blank(),
        plot.caption = element_text(hjust=0,vjust=10),
        plot.title = element_text(vjust=-10),
        plot.subtitle = element_text(vjust=-9)
        
  )

condo100k.nbrhd.p



## Owner occupied units 

gg.df.tmp <- gg.df %>% filter(Perc100k >= perc100k.4Q & !is.na(Own_100k_Ratio))

## own to 100k ratio 
own100k.nbrhd.p <- ggmap(mapImage.nyc
                         ,extent='normal') + 
  geom_polygon(aes(x=long,y=lat,group=group),
               data=gg.df,
               fill="white",
               alpha=0.95,
               color="grey50",
               size= 0.05) + 
  # geom_polygon(aes(fill=Own_100k_Ratio,x=long,y=lat,group=group),
  geom_polygon(aes(x=long,y=lat,group=group),
               fill="grey",
               data=gg.df,
               alpha=0.3,
               color=NA,
               size= 0.01) + 
  geom_polygon(aes(fill=Own_100k_Ratio,x=long,y=lat,group=group),
               data=gg.df.tmp,
               alpha=1,
               color="black",
               size= 0.25) + 
  labs(x="",
       y="",
       fill="Owned Resi Units\nPer > $100k Earner",
       title="Owner Occupied Residential Units to Six Figure Earners",
       subtitle="Among NYC Neighborhoods in Top 25% of Six Figure Earners",
       caption="Source:\nDOF PLUTO Database; DOF Property Assessment Data; PediaCities;\nDCP Property Address Directory; American Community Survey") +
  scale_x_continuous(labels = NULL, limits=c(nyc.box["left"],nyc.box["right"])) +
  scale_y_continuous(labels = NULL, limits=c(nyc.box["bottom"],nyc.box["top"])) +
  scale_fill_continuous(high="black",
                        low="lightblue") +
  # scale_fill_continuous(labels = scales::percent) + 
  theme_hwe() + 
  theme(panel.grid.major = element_blank(),
        plot.caption = element_text(hjust=0,vjust=10),
        plot.title = element_text(vjust=-10),
        plot.subtitle = element_text(vjust=-9)
        
  )

own100k.nbrhd.p


colnames(tmp.df)

out.df <- tmp.df %>% filter(Perc100k >= perc100k.4Q & !is.na(Condo_100k_Ratio)) %>% 
  arrange(Condo_100k_Ratio) %>% 
  select(id,Condo_100k_Ratio,Condo,Tot100k,Perc100k,TotPop,BoroCode) %>% 
  rename(Neighborhood=id,
         Total100k=Tot100k,
         TotalEarners=TotPop)

write.csv(out.df,"condo_100k_earners.csv",
          row.names=F)







# ## saving workspace image 
# save.image(paste("UWS Condo Analysis",
#                  paste(
#                    format(Sys.Date(),"%Y%m%d"),
#                    format(Sys.time(), "%H%M"),sep="_"),
#                  ".RData",sep=""))

