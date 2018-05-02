## Create pluto augmented with conversion date, type and TCO 
library(xlsx)
library(tidyverse)
library(stringr)
library(httr)


source("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach//useful functions/HWE_FUNCTIONS.r")
source("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach//useful functions/-.R")
source("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach//useful functions/useful minor functions.R")
source("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach//useful functions/hwe colors.R")

options(scipen=999)



# Join pluto with condo coop classifier -----------------------------------

condocoop.df <- readRDS("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Refresh/data/condocoop_df 20180430_1615.rds") %>%
  filter(!duplicated(BBL) & !grepl("[[:alpha:]]",BBL))

pluto.all <- readRDS("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Project Data/PLUTO/pluto_lean_compressed_2003_2017.rds")

pluto <- pluto.all %>%
  filter(Year==2017 & !duplicated(BBL)) %>%
  mutate(BBL = as.character(
    paste(as.numeric(BoroCode)
          ,as.numeric(Block)
          ,as.numeric(Lot)
          ,sep="_"
    )
  )
  ,APPBBL = ifelse(APPBBL==0
                   ,NA
                   ,paste(
                     as.numeric(str_sub(APPBBL,start=1,end=1))
                     ,as.numeric(str_sub(APPBBL,start=2,end=-5))
                     ,as.numeric(str_sub(APPBBL,start=-4,end=-1))
                     ,sep="_"
                   )
  )
  ,APPDate = parse_date_time(
                      APPDate
                      ,"m!/d!/y!*"
                    )
  ,APPYear = year(APPDate)
  ,randomId = sample(1:n(),n(),replace=F)
  )

pluto.tmp <- pluto.all %>%
  filter(Year >= 2014 & YearBuilt!=0 & nchar(YearBuilt)==4) %>%
  arrange(desc(Year)) %>%
  filter(!duplicated(BBL)) %>%
  mutate(BBL = as.character(
    paste(as.numeric(BoroCode)
          ,as.numeric(Block)
          ,as.numeric(Lot)
          ,sep="_"
    )
  )
  )

pluto.tmp <- pluto.tmp %>%
  semi_join(
    pluto %>%
      filter(YearBuilt==0 | nchar(YearBuilt)<4) 
    ,by=c("BBL","BldgClass","UnitsRes")
  ) %>%
  arrange(BoroCode,desc(UnitsRes)) %>%
  select(BBL,BldgClass,UnitsRes,YearBuilt,everything())

pluto <- pluto %>%
  left_join(
    pluto.tmp %>%
      select(BBL,BldgClass,UnitsRes,YearBuilt) %>%
      rename(tmp = YearBuilt)
    ,by=c("BBL","BldgClass","UnitsRes")
  ) %>%
  mutate(YearBuilt = ifelse(!is.na(tmp)
                            ,tmp
                            ,YearBuilt
                            )
         )

## see if I can recover some more lat/lon
pluto <- pluto %>%
  left_join(
    pluto.all %>%
      filter(!is.na(lat)) %>%
      arrange(desc(Year)) %>%
      filter(!duplicated(BBL)) %>%
      mutate(BBL = as.character(
        paste(as.numeric(BoroCode)
              ,as.numeric(Block)
              ,as.numeric(Lot)
              ,sep="_"
        )
      )
      ) %>%
      select(BBL,lat,lon) %>%
      rename(lat.tmp = lat
             ,lon.tmp = lon)
    ,by="BBL"
  ) %>%
  mutate(lat = ifelse(is.na(lat)
                      ,lat.tmp
                      ,lat
                      )
         ,lon = ifelse(is.na(lon)
                       ,lon.tmp
                       ,lon
                       )
         )

pluto.aug <- left_join(
  pluto %>% select(BBL,Address,Borough,Block,Lot
                   ,BldgClass,UnitsRes
                   ,Easements,OwnerType,OwnerName,BldgArea,ComArea,ResArea
                   ,OfficeArea,RetailArea,GarageArea,StrgeArea,FactryArea
                   ,OtherArea,NumBldgs,NumFloors,UnitsTotal,YearBuilt,YearAlter1,YearAlter2,APPBBL,APPDate
                   ,CondoNo,XCoord,YCoord,lat,lon,ZipCode)
  ,condocoop.df %>%
    select(BBL,Type,BldgClass_Internal.1,BldgClass_Internal.2,BldgClass_Internal.3)
  ,by="BBL"
)

## making sure A and B class buildings are classified as "small"
pluto.aug <- pluto.aug %>% 
  mutate(BldgClass.broad = str_sub(BldgClass,start=1,end=1)
         ,Type = ifelse(BldgClass.broad %in% c("A","B"),"small",Type)
  )

## pulling in Justin's hand coded data 
## unsure what field justin coded
handcoded.df <- read.xlsx("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Data/zeroyear_buildings filled in above 50 units.xlsx",
                          sheetIndex=1) %>%
  mutate(BBL = as.character(BBL)) %>%
  filter(UnitsRes >= 50 & !duplicated(BBL))


handcoded.yb <- handcoded.df %>%
  filter(nchar(trimws(as.character(YearBuilt)))==4) %>%
  select(BBL,Type,YearBuilt)

handcoded.owned  <- handcoded.df %>% 
  filter(is.na(Type) | Type=="owned" | grepl("condo",notes)) %>% 
  select(BBL,Type,YearBuilt)


## change recent condo conversions to condo
pluto.aug <- bind_rows(pluto.aug %>%
  semi_join(handcoded.owned
            ,by="BBL") %>%
  mutate(Type="condo")
  ,pluto.aug %>%
    anti_join(
      handcoded.owned
      ,by="BBL"
    )
  )

pluto.aug <- pluto.aug %>%
  left_join(
    handcoded.yb %>%
      select(BBL,YearBuilt) %>%
      rename(tmp = YearBuilt)
    ,by="BBL"
  ) %>%
  mutate(YearBuilt = ifelse(!is.na(tmp)
                            ,tmp
                            ,YearBuilt
  )
  ) %>%
  select(-tmp)


# Add TCO data to pluto ---------------------------------------------------
# pluto.aug.hold <- pluto.aug
# pluto.aug <- pluto.aug.hold

tco_url <- "https://data.cityofnewyork.us/resource/2vyb-t2nz.csv"

tco_nyc_httr.dl <- GET(paste(tco_url
                             ,"?$limit=4500000"
                             ,sep=""
)
)

tco.df.hold <- read_csv(tco_nyc_httr.dl$content
                        ,col_types = cols(.default="c")
)  %>%
  setNames(toupper(gsub("number","num",names(.)))) %>% 
  mutate(
    BLOCK = as.character(as.numeric(gsub("[^[:digit:]]","",str_sub(BLOCK,start=-4,end=-1))))
    ,LOT = as.character(as.numeric(gsub("[^[:digit:]].*","",LOT)))
    ,BBL = paste(str_sub(as.character(JOB_NUM),start=1,end=1)
                 ,BLOCK
                 ,LOT
                 ,sep="_"
    )
    ,BBL_BIN = paste(BBL
                     ,BIN_NUM
                     ,sep=".")
    ,C_O_ISSUE_DATE = parse_date_time(
      str_sub(
        C_O_ISSUE_DATE
        ,start=1
        ,end=10
      )
      ,"Y!-m!*-d!"
    )
    ,Address = paste(trimws(HOUSE_NUM),trimws(STREET_NAME),sep=" ")
  ) %>%
  select(BBL_BIN,JOB_NUM,C_O_ISSUE_DATE,JOB_TYPE,everything()) %>%
  select(-BIN)


tco.df <- tco.df.hold %>%
  filter(JOB_TYPE == "NB") %>%
  # filter(year(C_O_ISSUE_DATE) >= 2010) %>%
  group_by(BBL) %>%
  summarize(C_O_ISSUE_DATE = min(C_O_ISSUE_DATE)) %>%
  filter(year(C_O_ISSUE_DATE)>2012)

pluto.aug <- pluto.aug %>%
  left_join(tco.df %>%
              select(BBL,C_O_ISSUE_DATE)
            ,by="BBL"
  )

pluto_tmp.aug <- pluto.aug %>%
  filter(is.na(C_O_ISSUE_DATE)) %>%
  select(-C_O_ISSUE_DATE) %>%
  left_join(tco.df %>%
              select(BBL,C_O_ISSUE_DATE) %>%
              rename(APPBBL = BBL)
            ,by="APPBBL"
  )

pluto.aug <- bind_rows(
  pluto.aug %>%
    anti_join(pluto_tmp.aug
              ,by="BBL"
    )
  ,pluto_tmp.aug
)

tco.df <- tco.df.hold %>%
  filter(JOB_TYPE == "NB") %>%
  group_by(Address) %>%
  summarize(C_O_ISSUE_DATE = min(C_O_ISSUE_DATE)) %>%
  filter(year(C_O_ISSUE_DATE)>2012)

pluto_tmp.aug <- pluto.aug %>%
  filter(is.na(C_O_ISSUE_DATE)) %>%
  select(-C_O_ISSUE_DATE) %>%
  left_join(tco.df %>%
              select(Address,C_O_ISSUE_DATE)
            ,by="Address"
  )

pluto.aug <- bind_rows(
  pluto.aug %>%
    anti_join(pluto_tmp.aug
              ,by="BBL"
    )
  ,pluto_tmp.aug
) 

pluto.aug <- pluto.aug %>%
  mutate(TCO.real = year(C_O_ISSUE_DATE)
         ,TCO.impute = ifelse(
           nchar(YearBuilt)==4
           ,YearBuilt + 2
           ,NA
         )
         ,TCO = ifelse(is.na(TCO.real)
                       ,TCO.impute
                       ,ifelse((TCO.real - YearBuilt>4)
                               ,ifelse(!is.na(TCO.impute)
                                       ,TCO.impute
                                       ,TCO.real
                               )
                               ,TCO.real
                       )
         )
  ) %>%
  filter((YearBuilt !=0 & nchar(YearBuilt)==4) | !is.na(TCO.real))

pluto.aug <- pluto.aug %>% mutate(Borough = ifelse(Borough=="MN","MANHATTAN",Borough)
                                  ,Borough = ifelse(Borough=="BX","BRONX",Borough)
                                  ,Borough = ifelse(Borough=="BK","BROOKLYN",Borough)
                                  ,Borough = ifelse(Borough=="QN","QUEENS",Borough)
                                  ,Borough = ifelse(Borough=="SI","STATEN ISLAND",Borough)
)



# Neighborhood ------------------------------------------------------------

## read in pediacities shapefile
pediashape.url <- "http://data.beta.nyc//dataset/0ff93d2d-90ba-457c-9f7e-39e47bf2ac5f/resource/35dd04fb-81b3-479b-a074-a27a37888ce7/download/d085e2f8d0b54d4590b1e7d1f35594c1pediacitiesnycneighborhoods.geojson"

pedia.map <- geojson_read(as.location(pediashape.url),
                          method="local",
                          what="sp")

pedia.map <- st_as_sf(pedia.map,crs=4326)

## can only geolocate on those with lat/lon
tmp.sf <- st_as_sf(pluto.aug %>% 
                     filter(!is.na(lat)) %>%
                     select(BBL,lon,lat)
                   ,coords = c("lon", "lat"), crs = 4326)

## spatial join and then re-join with resi.out
tmp.sf <- st_join(tmp.sf
                  ,pedia.map %>%
                    rename(Neighborhood = neighborhood) %>%
                    mutate(BOROUGH.PEDIA = toupper(as.character(borough)))) %>% 
  select(BBL,Neighborhood)


pluto.aug <- left_join(pluto.aug
                          ,tmp.sf %>%
                            select(BBL,Neighborhood)
                          ,by="BBL"
) %>%
  select(-geometry)


# AG Data -----------------------------------------------------------------

AGdata.df <- readRDS("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Refresh/data/condo_coop_conversions_df 20180427_1306.rds")

## If conversion year is missing try to use APPDate (only for condos)
## only use APPDate if
## (1) prior BBL was not a condo
## (2) TCO is less than or equal to APPYear
pluto.aug.hold <- pluto.aug
pluto.aug <- pluto.aug.hold %>%
  left_join(AGdata.df
            ,by="BBL") %>%
  mutate(ConversionYear = ifelse(
    Type %in% "condo" & is.na(ConversionYear)
    ,ifelse(
      nchar(
        (str_sub(
          gsub(".*_","",APPBBL)
          ,start=1
          ,end=-1
        ))!=4 & 
          str_sub(
            gsub(".*_","",APPBBL)
            ,start=1
            ,end=2
          ) != 75 &
          TCO <= year(APPDate)
        )
      ,year(APPDate)
      ,ConversionYear
    )
    ,ConversionYear
  )
  ,ConversionYear = ifelse(!is.na(ConversionYear) & abs(ConversionYear - TCO)<=2
                           ,TCO
                           ,ConversionYear
                           )
  ) %>%  
  mutate(
    DECLARE_NEWBUILD = ConversionYear - TCO < 5
  )


saveRDS(pluto.aug,"/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Refresh/data/pluto_augmented 20180501_1332.rds")


# Conversion data frames (depricated) -------------------------------------

Conversions.df <- ungroup(
  pluto.aug %>%
    filter(!is.na(ConversionYear) & Type %in% c("condo","coop")) %>%
    group_by(ConversionYear,ConversionType) %>%
    summarize(CONVERTED_UNITS = sum(UnitsRes.AG,na.rm=T)) %>%
    mutate(AREA = "NYC") %>%
    rename(YEAR = ConversionYear
           ,PLAN_TYPE = ConversionType)
) %>% 
  ungroup()

Conversions_boro.df <- ungroup(
  pluto.aug %>%
    filter(!is.na(ConversionYear) & Type %in% c("condo","coop")) %>%
    group_by(Borough,ConversionYear,ConversionType) %>%
    summarize(CONVERTED_UNITS = sum(UnitsRes.AG,na.rm=T)) %>%
    rename(AREA = Borough) %>%
    rename(YEAR = ConversionYear
           ,PLAN_TYPE = ConversionType)
) %>%
  ungroup()

Conversions_nbrhd.df <- ungroup(
    pluto.aug %>%
      filter(!is.na(ConversionYear) & Type %in% c("condo","coop")) %>%
      group_by(Neighborhood,ConversionYear,ConversionType) %>%
      summarize(CONVERTED_UNITS = sum(UnitsRes.AG,na.rm=T)) %>%
      rename(AREA = Neighborhood) %>%
      rename(YEAR = ConversionYear
             ,PLAN_TYPE = ConversionType)
  ) %>%
  ungroup()


Borough.levs.conversion <- Conversions_boro.df %>%
  mutate(AREA = as.character(AREA)) %>%
  filter(!duplicated(AREA)) %>%
  pull(AREA)

Neighborhood.levs.conversion <- pluto.aug %>%
  mutate(AREA = as.character(Neighborhood)) %>%
  semi_join(Conversions_nbrhd.df %>%
              mutate(AREA = as.character(AREA))
            ,by="AREA"
  ) %>%
  filter(!is.na(AREA) & !duplicated(AREA)) %>%
  pull(AREA)

  


