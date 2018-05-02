## PLUTO AG join and conversion df creation 

# library(xlsx)
library(parallel)
# library(data.table)
library(dplyr)
library(stringr)
library(lubridate)
# library(ggplot2)


setwd("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply")
options(scipen=999)

source("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/useful functions/HWE_FUNCTIONS.r")
source("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/useful functions/-.R")
source("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/useful functions/useful minor functions.R")
source("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/useful functions/hwe colors.R")


# Dataframe containing condo/coop conversions -----------------------------

#############################################################
### just using the aggregate numbers 

# AGdata.df.hold <- AGdata.df
# AGdata.df <- AGdata.df.hold


## Only want conversions or rehabs
## if plan date effective is empty then the project was not completed
## also combining the plan units and total units field as there are varying degrees of missingness 
# AGdata.df <- AGdata.df %>% 
#   filter(
#     !(is.na(PLAN_DATE_EFFECTIVE)
#       | !grepl("[[:digit:]]",PLAN_DATE_EFFECTIVE)
#       )
#     & PLAN_CONSTR_TYPE %in% c("CONVERSION","REHAB") 
#     & grepl("[[:alnum:]]",PLAN_DATE_EFFECTIVE)
#     ) %>%
#   mutate(
#     UNITS = ifelse(is.na(PLAN_UNITS),PLAN_TOT_UNITS,PLAN_UNITS)
#     ,PLAN_DATE_EFFECTIVE = parse_date_time(PLAN_DATE_EFFECTIVE,"Y!-m!-d!")
#     ,Zip = trimws(gsub(".*NY ","",PLCITSTZIP))
#   )
#   # ) %>% 
#   # rename(UNITS = PLAN_TOT_UNITS)

AGdata.df <- ungroup(
  read.csv("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/AG Data/Coop_Condo_AGdata.csv"
           ,stringsAsFactors=F) %>%
    filter(
      !(is.na(PLAN_DATE_EFFECTIVE)
        #   | !grepl("[[:digit:]]",PLAN_DATE_EFFECTIVE)
      )
      & PLAN_CONSTR_TYPE %in% c("CONVERSION","REHAB") 
      & grepl("[[:alnum:]]",PLAN_DATE_EFFECTIVE)
    ) %>%
    select(PLAN_ID,everything()) %>%
    mutate(
      UNITS = ifelse((is.na(PLAN_UNITS) | PLAN_UNITS==0)
                     ,PLAN_TOT_UNITS
                     ,PLAN_UNITS
      )
      ,PLAN_ACCEPT_DT = parse_date_time(PLAN_ACCEPT_DT,"d!-b!-y!*")
      ,PLAN_DATE_SUBMITTED = parse_date_time(PLAN_DATE_SUBMITTED,"d!-b!-y!*")
      ,PLAN_DATE_REVIEWED = parse_date_time(PLAN_DATE_REVIEWED,"d!-b!-y!*")
      ,PLAN_DATE_EFFECTIVE = parse_date_time(PLAN_DATE_EFFECTIVE,"d!-b!-y!*")
      ,MAX_AMND_DATE_SUBMITTED = parse_date_time(MAX_AMND_DATE_SUBMITTED,"d!-b!-y!*")
      ,AMND_DATE_REVIEWED = parse_date_time(AMND_DATE_REVIEWED,"d!-b!-y!*")
      ,PLAN_ID = trimws(PLAN_ID)
      ,PLAN_ID_NUMER = gsub("[^[:digit:]]","",PLAN_ID)
      ,PLAN_BORO = ifelse(PLAN_BORO_COUNTY %in% "NEW YORK"
                          ,"MANHATTAN"
                          ,ifelse(PLAN_BORO_COUNTY %in% "KINGS"
                                  ,"BROOKLYN"
                                  ,ifelse(PLAN_BORO_COUNTY %in% "STATEN ISLAND"
                                          ,"STATEN ISLAND"
                                          ,PLAN_BORO_COUNTY
                                  )
                          )
      )
      ,PLAN_ZIP_OLD = PLAN_ZIP
      ,PLAN_TYPE = ifelse(PLAN_TYPE == "COOPERATIVE/CONDOMINIUM"
                          ,"COOPERATIVE"
                          ,PLAN_TYPE)
    ) %>%
    group_by(PLAN_ID) %>%
    mutate(
      PLAN_ID_UNIQUE = paste(PLAN_ID,0:(n()-1),sep="_")
    )
)


## not sure what manual coded is, need to look into further
manual_coded <- read.csv("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/AG Data/unlinkable manual update 20170613.csv",
                         stringsAsFactors=F) %>% 
  # mutate(PLAN_DATE_EFFECTIVE = parse_date_time(PLAN_DATE_EFFECTIVE,"m!*/d!/y!*")) %>% 
  rename(PLAN_STREET = PLAN_STREET.AG) %>% 
  mutate(
    PLAN_ACCEPT_DT = parse_date_time(PLAN_ACCEPT_DT,"m!*/d!/y!*")
    ,PLAN_DATE_SUBMITTED = parse_date_time(PLAN_DATE_SUBMITTED,"m!*/d!/y!*")
    ,PLAN_DATE_REVIEWED = parse_date_time(PLAN_DATE_REVIEWED,"m!*/d!/y!*")
    ,PLAN_DATE_EFFECTIVE = parse_date_time(PLAN_DATE_EFFECTIVE,"m!*/d!/y!*")
    ,MAX_AMND_DATE_SUBMITTED = parse_date_time(MAX_AMND_DATE_SUBMITTED,"m!*/d!/y!*")
    ,AMND_DATE_REVIEWED = parse_date_time(AMND_DATE_REVIEWED,"m!*/d!/y!*")
    ,PLAN_ID = trimws(PLAN_ID)
    ,PLAN_ID_NUMER = gsub("[^[:digit:]]","",PLAN_ID)
    # ,PLAN_BORO = ifelse(PLAN_BORO_COUNTY %in% "NEW YORK"
    #                     ,"MANHATTAN"
    #                     ,ifelse(PLAN_BORO_COUNTY %in% "KINGS"
    #                             ,"BROOKLYN"
    #                             ,ifelse(PLAN_BORO_COUNTY %in% "STATEN ISLAND"
    #                                     ,"STATEN ISLAND"
    #                                     ,PLAN_BORO_COUNTY
    #                             )
    #                     )
    # )
    # ,PLAN_ZIP_OLD = PLAN_ZIP
    ,PLAN_TYPE = ifelse(PLAN_TYPE == "COOPERATIVE/CONDOMINIUM"
                        ,"COOPERATIVE"
                        ,PLAN_TYPE)
  )


manual_coded.hold <- manual_coded
manual_coded <- manual_coded[,colnames(AGdata.df)]


##

notin.mc <- anti_join(manual_coded
                      ,AGdata.df
                      ,by="PLAN_ID_UNIQUE") %>% 
  arrange(PLAN_ID_UNIQUE)

# overlap.mc <- semi_join(manual_coded
#                         ,AGdata.df
#                         ,by="PLAN_ID_UNIQUE") %>% 
#   arrange(PLAN_ID_UNIQUE)

# overlap.AG <- semi_join(AGdata.df
#                         ,manual_coded
#                         ,by="PLAN_ID_UNIQUE") %>% 
#   arrange(PLAN_ID_UNIQUE)




AGdata.df <- bind_rows(AGdata.df,notin.mc)



dup.ids <- unique(as.character(as.data.frame(AGdata.df %>% 
                                               filter(duplicated(PLAN_ID_UNIQUE))
                                             ,stringsAsFactors=F)[,"PLAN_ID_UNIQUE"]))


# Aggregate conversion numbers by year and borough ------------------------

## by borough, count the number of conversions in a year
Boro.conversions <- AGdata.df %>% 
  group_by(year(PLAN_DATE_EFFECTIVE),PLAN_BORO,PLAN_TYPE) %>% 
  summarize(CONVERTED_UNITS = sum(UNITS,na.rm=T)) %>% 
  rename(YEAR = `year(PLAN_DATE_EFFECTIVE)`
         ,AREA = PLAN_BORO)

## do it for the entire city and combine the two 
City.conversions <- ungroup(Boro.conversions) %>% 
  group_by(YEAR,PLAN_TYPE) %>% 
  summarize(CONVERTED_UNITS=sum(CONVERTED_UNITS)
            ,AREA="NYC")

Conversions.df <- as.data.frame(ungroup(
  bind_rows(Boro.conversions,City.conversions)
)
,stringsAsFactors=F
)



# Create pluto augmented dataframe ----------------------------------------
## pluto augmented is pluto but with a field that indicates whether a building is condo, coop or rental

condocoop.df <- readRDS("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/UWS condo prop/Data/condo_coop_plutojoined_df.rds")
pluto <- readRDS("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/UWS condo prop/Data/AG data/pluto.rds")


## remove duplicate BBLS 
bbl.dupes <- condocoop.df[
  condocoop.df[,"BBL"] %in% 
    condocoop.df[duplicated(condocoop.df[,"BBL"]),"BBL"]
  ,"BBL"]

condocoop.df <- condocoop.df %>% filter(!BBL %in% bbl.dupes)
# condocoop.df.hold <- condocoop.df
condocoop.df <- condocoop.df %>%
  semi_join(
    pluto
    ,by="BBL"
  )

## combine pluto and the condo/coop dataframes 
pluto_cc.match <- match(condocoop.df[,"BBL"],pluto[,"BBL"])
pluto <- pluto[pluto_cc.match,]

# View(cbind(pluto[,"Address"],condocoop.df[,"Address"]))
# colnames(pluto)[
# colnames(pluto) %in% colnames(condocoop.df)
# ]

# which(pluto[,"UnitsRes"] != condocoop.df[,"UnitsRes"])

# View(
# cbind(pluto[which(pluto[,"UnitsRes"] != condocoop.df[,"UnitsRes"]),"UnitsRes"],
#       condocoop.df[which(pluto[,"UnitsRes"] != condocoop.df[,"UnitsRes"]),"UnitsRes"]))

## only want the columns in pluto that are also in the condo/coop dataframe
keep.cols <- colnames(pluto)[
  !colnames(pluto) %in% colnames(condocoop.df)]
keep.cols <- c("BBL",paste(keep.cols, collapse=","))


## derp... keeping these columns and then joining with the condo/coop dataframe
pluto.restrcols <- pluto %>% select(BBL,Borough,Block,Lot,SplitZone,LandUse,Easements
                                    ,OwnerType,OwnerName,BldgArea,ComArea,ResArea
                                    ,OfficeArea,RetailArea,GarageArea,StrgeArea,FactryArea
                                    ,OtherArea,AreaSource,NumBldgs,NumFloors,UnitsTotal,YearBuilt
                                    ,CondoNo,XCoord,YCoord)

pluto.aug <- left_join(condocoop.df,pluto.restrcols,by="BBL")


## changing things up slightly. 
## to get appropriate rentals classifying single family homes as condos and assigning 1/2 of two family homes to rentals
pluto.aug <- pluto.aug %>% 
  rename(UnitsRes.old = UnitsRes) %>% 
  mutate(BldgClass.broad = str_sub(BldgClass,start=1,end=1)
         ,Type = ifelse(BldgClass.broad=="A","condo",Type)
         ,Type = ifelse((BldgClass.broad=="B" & Type=="all"),sample(c("condo","all"),1,replace=F),Type)
  )



## pulling in Justin's hand coded data 
## unsure what field justin coded
handcoded.df <- read.xlsx("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Data/zeroyear_buildings filled in above 50 units.xlsx",
                          sheetIndex=1)


handcoded.bbls <- handcoded.df[,"BBL"]
handcoded.df <- handcoded.df %>% filter(UnitsRes>=50)
handcoded.yb <- handcoded.df %>% 
  filter(nchar(trimws(as.character(YearBuilt)))==4) %>% 
  select(BBL,Type,YearBuilt)
handcoded.co <- handcoded.df %>% 
  filter(nchar(trimws(as.character(CO)))==4) %>% 
  select(BBL,Type,YearBuilt,CO)
handcoded.owned  <- handcoded.df %>% 
  filter(is.na(Type) | Type=="owned" | grepl("condo",notes)) %>% 
  select(BBL,Type,YearBuilt)

handcoded.keeps <- unique(c(as.character(handcoded.yb[,"BBL"])
                            ,as.character(handcoded.co[,"BBL"])))

handcoded.drops <- as.character(handcoded.bbls[!handcoded.bbls %in% handcoded.keeps])

## change recent condo conversions to condo
pluto.aug[pluto.aug[,"BBL"] %in% handcoded.owned[,"BBL"],"Type"] <- "condo"

pluto.aug[as.numeric(na.omit(match(handcoded.yb[,"BBL"],pluto.aug[,"BBL"]))),"YearBuilt"] <- handcoded.yb[,"YearBuilt"]
pluto.aug <- pluto.aug %>% 
  mutate(YearBuilt = as.numeric(
    ifelse((nchar(as.character(YearBuilt))==2),paste(20,YearBuilt,sep=""),YearBuilt)
  )
  ,YearBuilt = as.numeric(
    ifelse((nchar(as.character(YearBuilt))==1 & YearBuilt!=0),paste(200,YearBuilt,sep=""),YearBuilt)
  )
  )

## For TCO lag it may be prudent to try joining with the TCO dataset and see what comes of using actual dates where available


pluto.aug[,"TCO.2"] <- pluto.aug[,"YearBuilt"] + 2
pluto.aug[,"TCO.3"] <- pluto.aug[,"YearBuilt"] + 3

pluto.aug[as.numeric(na.omit(match(handcoded.co[,"BBL"],pluto.aug[,"BBL"]))),"TCO.2"] <- handcoded.co[,"CO"]
pluto.aug[as.numeric(na.omit(match(handcoded.co[,"BBL"],pluto.aug[,"BBL"]))),"TCO.3"] <- handcoded.co[,"CO"]

pluto.aug <- pluto.aug %>% filter(!(BBL %in% handcoded.drops) & TCO.2>1600 & TCO.2<2020)



# sum(pluto.aug[,"YearBuilt"]==0 & pluto.aug[,"UnitsRes"]>=50)
# View(pluto.aug[pluto.aug[,"YearBuilt"]==0 & pluto.aug[,"UnitsRes"]>=50,])
# as.numeric(na.omit(match(handcoded.yb[,"BBL"],pluto.aug[,"BBL"])))
# View(cbind(pluto.aug[as.numeric(na.omit(match(handcoded.yb[,"BBL"],pluto.aug[,"BBL"]))),c("Address","BBL")]
#            ,handcoded.yb))
# yb.derp <- as.data.frame(summary(factor(pluto.aug[,"YearBuilt"]),
#         maxsum=200)
#         ,stringsAsFactors=F
# )
# yb.derp <- as.data.frame(cbind(rownames(yb.derp),yb.derp[,1])
#                          ,stringsAsFactors=F)
# colnames(yb.derp) <- c("year","count")
# yb.derp[,"count"] <- as.numeric(as.character(yb.derp[,"count"]))
# yb.derp <- arrange(yb.derp,desc(count))
# View(yb.derp)



## adding in the lag Tim came up with, probably not going to use this and just use the two year lag instead 
# TCO.lag <- as.data.frame(readRDS("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/UWS condo prop/Data/AG data/BBL_to_ApproxTCO.rds")
#                          ,stringsAsFactors=F)
# 
# TCO.lag <- TCO.lag %>% 
#   mutate(BBL.old = as.character(BBL)
#          ,Boro = as.numeric(str_sub(BBL.old,start=1,end=1))
#          ,Block = as.numeric(str_sub(BBL.old,start=2,end=6))
#          ,Lot = as.numeric(str_sub(BBL.old,start=-4,end=-1))
#          ,BBL = paste(Boro,Block,Lot,sep="_")
#   )
# 
# 
# pluto_tco.match <- match(TCO.lag[,"BBL"],pluto.aug[,"BBL"])
# pluto.aug[,"TCO"] <- TCO.lag[pluto_tco.match,"Approximate_TCO"]
# 
# 
# # summary(pluto.aug[,"TCO"])
# # sum(pluto.aug[,"TCO"]>2016)
# # sum(pluto.aug[,"TCO"]==0)
# # sum(pluto.aug[,"YearBuilt"]>2016)
# # 
# # 
# # View(pluto.aug %>% filter(TCO>2016 | TCO <1600))


## From this point we need to fix the observations with zero years 

## insert the fix here 


# pluto.aug[,"TCO.2"] <- pluto.aug[,"YearBuilt"] + 2


# year.wrong <- (pluto.aug[,"YearBuilt"]<1700 | pluto.aug[,"YearBuilt"]>2016)
# sum(pluto.aug[,"YearBuilt"]>2016)
# year.wrong <- sum(pluto.aug[,"YearBuilt"]<1700)
# summary(pluto.aug[pluto.aug[,"YearBuilt"]<1700,"YearBuilt"])
# sum(pluto.aug[,"YearBuilt"]==0)
# sel <- which(pluto.aug[,"YearBuilt"]==0)


# colnames(pluto.aug)
# View(pluto.aug[sel[1:1000],c("ZipCode","BldgClass","BBL","Address","Type","UnitsRes","UnitsTotal")])
# 
# pluto.zeroyear <- pluto.aug %>% filter(YearBuilt==0 & UnitsRes > 0 & Type == "all") 
# View(pluto.zeroyear[,c("ZipCode","BldgClass","BBL","Address","Type","UnitsRes"
#                        # ,"UnitsTotal"
#                        ,"YearBuilt")] %>% arrange(desc(UnitsRes)))

# summary(factor(str_sub(pluto.zeroyear[,"BBL"],start=1,end=1)))


## two year TCO lag 
# pluto.aug <- pluto.aug %>% mutate(
#   YearBuilt = ifelse()
# )
getwd()

# pluto.aug <- pluto.aug %>% mutate(
#   YearBuilt = ifelse(YearBuilt==2040,2004,YearBuilt)
#   ,TCO = ifelse((TCO>2016 | TCO<1700),YearBuilt,TCO))

# Boro.levs <- unique(pluto.aug[,"Borough"])

# pluto.aug.hold <- pluto.aug
# for(i in 1:length(Boro.levs)){
#   x <- Boro.levs[i]
#   restr <- (pluto.aug[,"Borough"]==x & !(pluto.aug[,"TCO"]<1600 | pluto.aug[,"TCO"]>2016))
#   restr.add <- (pluto.aug[,"Borough"]==x & (pluto.aug[,"TCO"]<1600 | pluto.aug[,"TCO"]>2016))
#   pluto.aug[restr.add,"TCO"] <- mean(pluto.aug[restr,"TCO"])
# }

pluto.aug <- pluto.aug %>%
  rename(UnitsRes = UnitsRes.old)

# pluto.aug %>% filter(TCO.2<1850 | TCO.2>2017) %>% summarize(totunits=sum(UnitsRes,na.rm=T))

# summary(factor(pluto.aug[(pluto.aug[,"UnitsRes.old"]>0),"TCO.2"]),maxsum=500)

TCO.levs <- unique(pluto.aug[,"TCO.2"])

# View(pluto.aug %>% filter(UnitsRes>50) %>% group_by(TCO.2) %>% summarize(totunits=sum(UnitsRes,na.rm=T)))

# View(pluto.aug %>% filter(UnitsRes>50 & TCO.2<1800))


year.levs <- unique(pluto.aug[,"TCO.2"])
year.levs <- round(year.levs[order(year.levs,decreasing=T)])
year.levs <- year.levs[year.levs>=1980 & year.levs<=2017]

# year.levs <- year.levs[year.levs >= 1980]

# out <- lapply(year.levs, function(x){
#   cat(x,"\n")
#   tmp.out <- as.data.frame(pluto.aug %>% 
#                              filter(YearBuilt < x) %>%
#                              mutate(Type = ifelse(Type=="all","rental",Type)
#                                     ,Type = ifelse(Type %in% c("condo","coop"),"owned",Type)) %>%
#                              group_by(Type) %>% 
#                              summarize(count=sum(UnitsRes)) %>% 
#                              # rename(rental = all) %>% 
#                              mutate(Year = as.character(x))
#                            ,stringsAsFactors=F
#   )
#   return(tmp.out)
# }
# )



# i <- length(year.levs)


Borough.levs <- unique(pluto.aug[,"Borough"])
Borough.levs.conversion <- unique(Conversions.df[,"AREA"])

pluto.aug <- pluto.aug %>% mutate(Borough = ifelse(Borough=="MN","MANHATTAN",Borough)
                                  ,Borough = ifelse(Borough=="BX","BRONX",Borough)
                                  ,Borough = ifelse(Borough=="BK","BROOKLYN",Borough)
                                  ,Borough = ifelse(Borough=="QN","QUEENS",Borough)
                                  ,Borough = ifelse(Borough=="SI","STATEN ISLAND",Borough)
)

pluto.aug.old <- readRDS("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Data/pluto_augmented_20170613_1628.rds")

# pluto.aug.old <- readRDS("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/AG Data/pluto_augmented_20170522_1307.rds") %>% 
pluto.aug<- readRDS("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Data/pluto_augmented_20170613_1628.rds") %>% 
  mutate(
    UnitsRes = ifelse((!is.na(UnitsRes.AG) & UnitsRes.AG - UnitsRes.old > 5 & UnitsTotal > UnitsRes.old)
                      ,UnitsTotal
                      ,UnitsRes.old
    )
    , ConversionType.AG = ifelse(ConversionType.AG == "COOPERATIVE/CONDOMINIUM"
                                 ,"COOPERATIVE"
                                 ,ConversionType.AG
    )
    ,Type = ifelse(substr(BldgClass,start=1,stop=1) %in% c("A","B")
                   ,"small"
                   ,Type)
    ,Type = ifelse(BldgClass == "A8"
                   ,"coop"
                   ,Type)
  )

pluto.aug.hold <- pluto.aug

Conversions.df <- ungroup(
  pluto.aug %>%
    filter(!is.na(ConversionDate)) %>%
    mutate(ConversionYear = year(ConversionDate)) %>%
    group_by(ConversionYear,ConversionType.AG) %>%
    summarize(CONVERTED_UNITS = sum(UnitsRes.AG,na.rm=T)) %>%
    mutate(AREA = "NYC") %>%
    rename(YEAR = ConversionYear
           ,PLAN_TYPE = ConversionType.AG)
)

Conversions_boro.df <- ungroup(
  pluto.aug %>%
    filter(!is.na(ConversionDate)) %>%
    mutate(ConversionYear = year(ConversionDate)) %>%
    group_by(Borough,ConversionYear,ConversionType.AG) %>%
    summarize(CONVERTED_UNITS = sum(UnitsRes.AG,na.rm=T)) %>% 
    rename(AREA = Borough
           ,YEAR = ConversionYear
           ,PLAN_TYPE = ConversionType.AG)
)

Conversions_nbrhd.df <- ungroup(
  pluto.aug %>%
    filter(!is.na(ConversionDate)) %>%
    mutate(ConversionYear = year(ConversionDate)) %>%
    group_by(Neighborhood,ConversionYear,ConversionType.AG) %>%
    summarize(CONVERTED_UNITS = sum(UnitsRes.AG,na.rm=T)) %>% 
    rename(AREA = Neighborhood
           ,YEAR = ConversionYear
           ,PLAN_TYPE = ConversionType.AG)
)



anti_join(
  pluto.aug %>% 
    filter(!duplicated(Neighborhood)) %>% 
    rename(AREA = Neighborhood) %>% 
    select(AREA)
  ,Conversions_nbrhd.df
  ,by="AREA"
)[,"AREA"]



# unique(Conversions_nbrhd.df[,"YEAR"])


Borough.levs.conversion <- unique(as.character(as.data.frame(Conversions_boro.df,stringsAsFactors=F)[,"AREA"]))
Neighborhood.levs.conversion <- as.character(
  na.omit(
    c(
      unique(as.character(as.data.frame(Conversions_nbrhd.df,stringsAsFactors=F)[,"AREA"]))
      ,as.character(anti_join(
        pluto.aug %>% 
          filter(!duplicated(Neighborhood)) %>% 
          rename(AREA = Neighborhood) %>% 
          select(AREA)
        ,Conversions_nbrhd.df
        ,by="AREA"
      )[,"AREA"])
    )
  )
)

