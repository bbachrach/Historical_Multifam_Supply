library(xlsx)
library(parallel)
library(data.table)
library(dplyr)
library(stringr)
library(lubridate)
library(ggplot2)

## pick up at line 530
# "Pick up here"

setwd("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply")
options(scipen=999)

source("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/useful functions/HWE_FUNCTIONS.r")
source("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/useful functions/-.R")
source("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/useful functions/useful minor functions.R")
source("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/useful functions/hwe colors.R")


condo_rental.df <- read.csv("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/HVS/condo rental metric small.csv"
                            ,stringsAsFactors=F)

tmp.df <- as.data.frame(
  cbind(1980:2016
        ,rep(NA,37)
        # ,rep(NA,37)
  )
)


for(i in 1:nrow(tmp.df)){
  tmp.vec <- tmp.df[i,1] - condo_rental.df[,1]
  if(sum(tmp.vec>=0)==0){
    sel <- which.max(tmp.vec)
  } else {
    tmp.vec <- ifelse(tmp.vec < 0
                      ,tmp.vec + 1000
                      ,tmp.vec)
    sel <- which.min(tmp.vec)
  }
  tmp.df[i,2] <- condo_rental.df[sel,2]
  # tmp.df[i,3] <- condo_rental.df[sel,1]
  cat(i,"\n")
}

colnames(tmp.df) <- c("Year","condo_rental.adj")

condo_rental.df <- tmp.df

rm(tmp.df)



# AGdata.df <- read.xlsx("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/ad_hoc/UWS condo prop/Data/Coop_Condo_AGdatadata.xlsx",
#                    sheetIndex=1)
# AGdata.df <- read.csv("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/ad_hoc/UWS condo prop/Data/Coop_Condo_AGdata.csv",
#                       stringsAsFactors=F)
# AGdata.df <- read.csv("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Data/AG_DATA_CONVERSION_REHAB_ONLY.csv"
#                       ,stringsAsFactors=F)

# save.image("AG_datamunge_v2.RData")


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

# colnames(manual_coded)[
# !colnames(manual_coded) %in% colnames(AGdata.df)]
# 
# colnames(AGdata.df)[
# !colnames(AGdata.df) %in% colnames(manual_coded)]


manual_coded[,"PLAN_ID_UNIQUE"] %in% as.data.frame(AGdata.df,stringsAsFactors=F)[,"PLAN_ID_UNIQUE"]


notin.mc <- anti_join(manual_coded
                      ,AGdata.df
                      ,by="PLAN_ID_UNIQUE") %>% 
  arrange(PLAN_ID_UNIQUE)

overlap.mc <- semi_join(manual_coded
                        ,AGdata.df
                        ,by="PLAN_ID_UNIQUE") %>% 
  arrange(PLAN_ID_UNIQUE)

overlap.AG <- semi_join(AGdata.df
                        ,manual_coded
                        ,by="PLAN_ID_UNIQUE") %>% 
  arrange(PLAN_ID_UNIQUE)


# dim(notin.mc)
# dim(overlap.mc)
# dim(overlap.AG)
# 
# identical(overlap.mc[,"UNITS"],overlap.AG[,"UNITS"])
# View(overlap.mc)
# View(overlap.AG)
# View(notin.mc)
# 
# sum(overlap.AG[,"UNITS"])
# sum(overlap.mc[,"UNITS"])
# sum(notin.mc[,"UNITS"],na.rm=T)

AGdata.df <- bind_rows(AGdata.df,notin.mc)


# ungroup(Conversions_boro.df) %>% 
#   summarize(sum(CONVERTED_UNITS))
# AGdata.df %>% 
#   summarize(sum(UNITS))


dup.ids <- unique(as.character(as.data.frame(AGdata.df %>% 
                                               filter(duplicated(PLAN_ID_UNIQUE))
                                             ,stringsAsFactors=F)[,"PLAN_ID_UNIQUE"]))

# View(AGdata.df %>% 
#        filter(UWS_zip==T & PLAN_ID %in% dup.ids) %>% 
#        arrange(desc(UNITS))
#      )

# View(pluto.aug %>% filter(grepl("150",Address) & grepl("west end",Address,ignore.case=T)))

# year(AGdata.df[,"PLAN_DATE_EFFECTIVE"])
# colnames(AGdata.df)


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

# Conversions.df <- as.data.frame(Conversions.df
#                                 ,stringsAsFactors=F)

getwd()
setwd("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/ad_hoc/UWS condo prop/Data/AG data")
# saveRDS(Conversions.df,"AG_condo_coop_conversions.rds")
Conversions.df <- readRDS("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/ad_hoc/UWS condo prop/Data/AG data/AG_condo_coop_conversions.rds")
# write.csv(Conversions.df
#           ,file="AG_condo_coop_conversions.csv"
#           ,row.names=F
# )


# Conversions_boro.df <- pluto.aug %>% 
#   mutate(ConversionYear = year(ConversionDate)) %>% 
#   group_by(Borough,ConversionYear,ConversionType.AG) %>% 
#   summarize(CONVERTED_UNITS = sum(UnitsRes.AG))
# 
# Conversions.df.hold <- Conversions.df
  

# View(Conversions_boro.df)
# View(pluto.aug %>% 
#        filter(Borough=="MANHATTAN"
#               & ConversionType.AG == "COOPERATIVE/CONDOMINIUM"
#               & as.numeric(year(ConversionDate))==1987
#        ))
  

## read in condo/coop dataframe and pluto 
condocoop.df <- readRDS("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/UWS condo prop/Data/condo_coop_plutojoined_df.rds")
pluto <- readRDS("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/ad_hoc/UWS condo prop/Data/AG data/pluto.rds")


## remove duplicate BBLS 
bbl.dupes <- condocoop.df[
  condocoop.df[,"BBL"] %in% 
    condocoop.df[duplicated(condocoop.df[,"BBL"]),"BBL"]
  ,"BBL"]

condocoop.df <- condocoop.df %>% filter(!BBL %in% bbl.dupes)
condocoop.df.hold <- condocoop.df
condocoop.df <- condocoop.df[which(condocoop.df[,"BBL"] %in% pluto[,"BBL"]),]

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


## derp... keeping these columns and then joinng with the condo/coop dataframe
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
# TCO.lag <- as.data.frame(readRDS("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/ad_hoc/UWS condo prop/Data/AG data/BBL_to_ApproxTCO.rds")
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

pluto.aug %>% filter(TCO.2<1850 | TCO.2>2017) %>% summarize(totunits=sum(UnitsRes,na.rm=T))

summary(factor(pluto.aug[(pluto.aug[,"UnitsRes"]>0),"TCO.2"]),maxsum=500)

TCO.levs <- unique(pluto.aug[,"TCO.2"])

View(pluto.aug %>% filter(UnitsRes>50) %>% group_by(TCO.2) %>% summarize(totunits=sum(UnitsRes,na.rm=T)))

View(pluto.aug %>% filter(UnitsRes>50 & TCO.2<1800))


year.levs <- unique(pluto.aug[,"TCO.2"])
year.levs <- round(year.levs[order(year.levs,decreasing=T)])
year.levs <- year.levs[year.levs >= 1980]

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


year.levs <- year.levs[year.levs>=1980 & year.levs<=2017]
# i <- length(year.levs)


Borough.levs <- unique(pluto.aug[,"Borough"])
Borough.levs.conversion <- unique(Conversions.df[,"AREA"])

pluto.aug <- pluto.aug %>% mutate(Borough = ifelse(Borough=="MN","MANHATTAN",Borough)
                                  ,Borough = ifelse(Borough=="BX","BRONX",Borough)
                                  ,Borough = ifelse(Borough=="BK","BROOKLYN",Borough)
                                  ,Borough = ifelse(Borough=="QN","QUEENS",Borough)
                                  ,Borough = ifelse(Borough=="SI","STATEN ISLAND",Borough))

pluto.aug <- readRDS("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Data/pluto_augmented_20170613_1628.rds")

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



unique(Conversions_nbrhd.df[,"YEAR"])





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
Neighborhood.levs.conversion <- 



# tmp.df <- pluto.aug %>% filter(YearBuilt<year.levs[x])
# rentals.tmp <- as.numeric(tmp.df %>% filter(Type=="all") %>% summarize(sum(UnitsRes)))
# converted_to_owned <- as.numeric(Conversions.df %>% filter(YEAR>year.levs[x] & AREA== "NYC") %>% summarize(sum(CONVERTED_UNITS)) )
# rentals <- rentals.tmp + converted_to_owned
# owned.tmp <- as.numeric(tmp.df %>% filter(Type!="all") %>% summarize(sum(UnitsRes)))
# owned <- owned.tmp - converted_to_owned
# 
# converted_to_condo <- as.numeric(Conversions.df %>% 
#                                    filter(YEAR>year.levs[x] & AREA== "NYC" & PLAN_TYPE== "CONDOMINIUM") %>% 
#                                    summarize(sum(CONVERTED_UNITS))
# )
# condo.tmp <- as.numeric(tmp.df %>% filter(Type=="condo") %>% summarize(sum(UnitsRes)))
# condo <- condo.tmp - converted_to_condo
# 
# converted_to_coop <- as.numeric(Conversions.df %>% 
#                                   filter(YEAR>year.levs[x] & AREA== "NYC" & PLAN_TYPE== "COOPERATIVE") %>% 
#                                   summarize(sum(CONVERTED_UNITS))
# )
# coop.tmp <- as.numeric(tmp.df %>% filter(Type=="coop") %>% summarize(sum(UnitsRes)))
# coop <- coop.tmp - converted_to_coop
# 
# small <- as.numeric(tmp.df %>% filter(Type=="small") %>% summarize(sum(UnitsRes)))
# 
# out <- c(year.levs[x],"NYC",rentals,owned,condo,coop,small)



cl <- makeCluster(detectCores()-1,type="FORK")

# z <- Neighborhood.levs.conversion[256]
# x <- 1
# tmp.derp <- lapply(Neighborhood.levs.conversion, function(z){
tmp.derp <- parLapply(cl,Neighborhood.levs.conversion, function(z){
  tmp.df.outer <- pluto.aug %>% filter(Neighborhood==z)
  Conversions.df <- Conversions_nbrhd.df  %>% filter(AREA==z)
  cat(z,"\n")
  tmp.out <- lapply(1:length(year.levs), function(x){
    cat(year.levs[x]," ")
    
    if(nrow(Conversions.df)>=1){
      converted_to_owned <- as.numeric(Conversions.df %>% filter(YEAR>year.levs[x] & AREA== z) %>% summarize(sum(CONVERTED_UNITS)))
      
      converted_to_condo <- as.numeric(Conversions.df %>% 
                                         filter(YEAR>year.levs[x] & AREA== z & PLAN_TYPE== "CONDOMINIUM") %>% 
                                         summarize(sum(CONVERTED_UNITS))
      )
      
      converted_to_coop <- as.numeric(Conversions.df %>% 
                                        filter(YEAR>year.levs[x] & AREA== z & PLAN_TYPE== "COOPERATIVE") %>% 
                                        summarize(sum(CONVERTED_UNITS))
      )   
    } else {
      converted_to_owned <- 0
      converted_to_condo <- 0
      converted_to_coop <- 0
    }
    
    tmp.df <- tmp.df.outer %>% filter(YearBuilt<year.levs[x])
    rentals.tmp <- as.numeric(tmp.df %>% filter(Type=="all") %>% summarize(sum(UnitsRes)))
    # converted_to_owned <- as.numeric(Conversions.df %>% filter(YEAR>year.levs[x] & AREA== z) %>% summarize(sum(CONVERTED_UNITS)))
    rentals <- rentals.tmp + converted_to_owned
    owned.tmp <- as.numeric(tmp.df %>% filter(Type!="all") %>% summarize(sum(UnitsRes)))
    owned <- owned.tmp - converted_to_owned
    
    # converted_to_condo <- as.numeric(Conversions.df %>% 
    #                                    filter(YEAR>year.levs[x] & AREA== z & PLAN_TYPE== "CONDOMINIUM") %>% 
    #                                    summarize(sum(CONVERTED_UNITS))
    # )
    condo.tmp <- as.numeric(tmp.df %>% filter(Type=="condo") %>% summarize(sum(UnitsRes)))
    condo <- condo.tmp - converted_to_condo
    
    # converted_to_coop <- as.numeric(Conversions.df %>% 
    #                                   filter(YEAR>year.levs[x] & AREA== z & PLAN_TYPE== "COOPERATIVE") %>% 
    #                                   summarize(sum(CONVERTED_UNITS))
    # )
    coop.tmp <- as.numeric(tmp.df %>% filter(Type=="coop") %>% summarize(sum(UnitsRes)))
    coop <- coop.tmp - converted_to_coop
    
    small <- as.numeric(tmp.df %>% filter(Type=="small") %>% summarize(sum(UnitsRes)))
    
    out <- c(year.levs[x],z,rentals,owned,condo,coop,small)
    return(out)
  }
  )
  
  out.df <- as.data.frame(do.call("rbind",tmp.out)
                          ,stringsAsFactors=F)
  colnames(out.df) <- c("Year","Neighborhood","Rentals","Owned","Condo","Coop","Small")
  out.df <- out.df %>% 
    mutate(
      Rentals = ifelse(as.numeric(Rentals)<0
                       ,0
                       ,as.numeric(Rentals))
      ,Owned = ifelse(as.numeric(Owned)<0
                      ,0
                      ,as.numeric(Owned))
      # ,Owned = as.numeric(Owned)
      ,Condo = ifelse(as.numeric(Condo)<0
                      ,0
                      ,as.numeric(Condo))
      # ,Condo = as.numeric(Condo)
      ,Coop = ifelse(as.numeric(Coop)<0
                     ,0
                     ,as.numeric(Coop))
      # ,Coop = as.numeric(Coop)
      # ,Small = as.numeric(Small)
      ,Small = ifelse(as.numeric(Small)<0
                      ,0
                      ,as.numeric(Small))
      ,Total = Rentals + Condo + Coop + Small
    )
  cat("\n")
  return(out.df)
}
)
stopCluster(cl)

# tmp.derp.hold <- tmp.derp
# getwd()
saveRDS(tmp.derp,"tmp_nbrhd_stats.rds")

out.df <- as.data.frame(do.call("rbind",tmp.derp)
                        ,stringsAsFactors=F)

out.df <- left_join(out.df
                    ,condo_rental.df %>% 
                      mutate(Year = as.character(Year))
) %>% 
  mutate(
    Coop.rentals = Coop * .15
    ,Condo.rentals = Condo * condo_rental.adj
    ,Rentals.adj = (Rentals + (Coop * .15) + (Condo * condo_rental.adj))
    ,Coop.adj = Coop * .85
    ,Condo.adj = Condo * (1-condo_rental.adj)
    ,Owned = Condo + Coop + Small
    ,Owned.cc = Condo.adj + Coop.adj
    ,Year = as.numeric(Year)
    ,AreaType = "Neighborhood"
  ) %>% 
  rename(AREA = Neighborhood)

# colnames(out.df) <- c("Year","Borough","Rentals","Owned","Prop.Rentals","Prop.Owned")
out.df.nbrhd <- out.df
View(out.df.nbrhd)


## Borough calculations 
# z <- Borough.levs.conversion[1]
# x <- 1
cl <- makeCluster(detectCores()-1,type="FORK")
tmp.derp <- lapply(Borough.levs.conversion[1:5], function(z){
  # tmp.derp <- parLapply(cl,Borough.levs.conversion[1:5], function(z){
  tmp.df.outer <- pluto.aug %>% filter(Borough==z)
  Conversions.df <- Conversions_boro.df %>% filter(AREA==z)
  tmp.out <- parLapply(cl,1:length(year.levs), function(x){
    cat(year.levs[x],"\n")
    tmp.df <- tmp.df.outer %>% filter(YearBuilt<year.levs[x])
    rentals.tmp <- as.numeric(tmp.df %>% filter(Type=="all") %>% summarize(sum(UnitsRes)))
    converted_to_owned <- as.numeric(Conversions.df %>% filter(YEAR>year.levs[x] & AREA== z) %>% summarize(sum(CONVERTED_UNITS)) )
    rentals <- rentals.tmp + converted_to_owned
    owned.tmp <- as.numeric(tmp.df %>% filter(Type!="all") %>% summarize(sum(UnitsRes)))
    owned <- owned.tmp - converted_to_owned
    
    converted_to_condo <- as.numeric(Conversions.df %>% 
                                       filter(YEAR>year.levs[x] & AREA== z & PLAN_TYPE== "CONDOMINIUM") %>% 
                                       summarize(sum(CONVERTED_UNITS))
    )
    condo.tmp <- as.numeric(tmp.df %>% filter(Type=="condo") %>% summarize(sum(UnitsRes)))
    condo <- condo.tmp - converted_to_condo
    
    converted_to_coop <- as.numeric(Conversions.df %>% 
                                      filter(YEAR>year.levs[x] & AREA== z & PLAN_TYPE== "COOPERATIVE") %>% 
                                      summarize(sum(CONVERTED_UNITS))
    )
    coop.tmp <- as.numeric(tmp.df %>% filter(Type=="coop") %>% summarize(sum(UnitsRes)))
    coop <- coop.tmp - converted_to_coop
    
    small <- as.numeric(tmp.df %>% filter(Type=="small") %>% summarize(sum(UnitsRes)))
    
    out <- c(year.levs[x],z,rentals,owned,condo,coop,small)
    return(out)
  }
  )
  
  out.df <- as.data.frame(do.call("rbind",tmp.out)
                          ,stringsAsFactors=F)
  colnames(out.df) <- c("Year","Borough","Rentals","Owned","Condo","Coop","Small")
  # out.df <- out.df %>% 
  #   mutate(
  #     Rentals = as.numeric(Rentals)
  #     ,Owned = as.numeric(Owned)
  #     ,Condo = as.numeric(Condo)
  #     ,Coop = as.numeric(Coop)
  #     ,Small = as.numeric(Small)
  #     ,Total = Rentals + Condo + Coop + Small
  #   )
  out.df <- out.df %>% 
    mutate(
      Rentals = ifelse(as.numeric(Rentals)<0
                       ,0
                       ,as.numeric(Rentals))
      ,Owned = ifelse(as.numeric(Owned)<0
                      ,0
                      ,as.numeric(Owned))
      # ,Owned = as.numeric(Owned)
      ,Condo = ifelse(as.numeric(Condo)<0
                      ,0
                      ,as.numeric(Condo))
      # ,Condo = as.numeric(Condo)
      ,Coop = ifelse(as.numeric(Coop)<0
                     ,0
                     ,as.numeric(Coop))
      # ,Coop = as.numeric(Coop)
      # ,Small = as.numeric(Small)
      ,Small = ifelse(as.numeric(Small)<0
                      ,0
                      ,as.numeric(Small))
      ,Total = Rentals + Condo + Coop + Small
    )
  return(out.df)
}
)
stopCluster(cl)

out.df <- as.data.frame(do.call("rbind",tmp.derp)
                        ,stringsAsFactors=F)

out.df <- left_join(out.df
                    ,condo_rental.df %>% 
                      mutate(Year = as.character(Year))
                    ) %>% 
  mutate(
    Coop.rentals = Coop * .15
    ,Condo.rentals = Condo * condo_rental.adj
    ,Rentals.adj = (Rentals + (Coop * .15) + (Condo * condo_rental.adj))
    ,Coop.adj = Coop * .85
    ,Condo.adj = Condo * (1-condo_rental.adj)
    ,Owned = Condo + Coop + Small
    ,Owned.cc = Condo.adj + Coop.adj
    ,Year = as.numeric(Year)
    ,AreaType = "Borough"
  ) %>% 
  rename(AREA = Borough)

# colnames(out.df) <- c("Year","Borough","Rentals","Owned","Prop.Rentals","Prop.Owned")
out.df.boro <- out.df

# load("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Data/multifam_rental_supply_workspace_20170601 1634.RData")
## City calculations 
year.levs <- 1980:2016
# x <- 2
# tmp.out.hold <- tmp.out
cl <- makeCluster(detectCores()-1,type="FORK")
# tmp.out <- lapply(1:length(year.levs), function(x){
tmp.out <- parLapply(cl,1:length(year.levs), function(x){
  cat(year.levs[x],"\n")
  tmp.df <- pluto.aug %>% filter(YearBuilt<year.levs[x])
  rentals.tmp <- as.numeric(tmp.df %>% filter(Type=="all") %>% summarize(sum(UnitsRes)))
  converted_to_owned <- as.numeric(Conversions.df %>% filter(YEAR>year.levs[x] & AREA== "NYC") %>% summarize(sum(CONVERTED_UNITS)) )
  rentals <- rentals.tmp + converted_to_owned
  owned.tmp <- as.numeric(tmp.df %>% filter(Type!="all") %>% summarize(sum(UnitsRes)))
  owned <- owned.tmp - converted_to_owned
  
  converted_to_condo <- as.numeric(Conversions.df %>% 
                                     filter(YEAR>year.levs[x] & AREA== "NYC" & PLAN_TYPE== "CONDOMINIUM") %>% 
                                     summarize(sum(CONVERTED_UNITS))
  )
  condo.tmp <- as.numeric(tmp.df %>% filter(Type=="condo") %>% summarize(sum(UnitsRes)))
  condo <- condo.tmp - converted_to_condo
  
  converted_to_coop <- as.numeric(Conversions.df %>% 
                                    filter(YEAR>year.levs[x] & AREA== "NYC" & PLAN_TYPE== "COOPERATIVE") %>% 
                                    summarize(sum(CONVERTED_UNITS))
  )
  coop.tmp <- as.numeric(tmp.df %>% filter(Type=="coop") %>% summarize(sum(UnitsRes)))
  coop <- coop.tmp - converted_to_coop
  
  small <- as.numeric(tmp.df %>% filter(Type=="small") %>% summarize(sum(UnitsRes)))
  
  out <- c(year.levs[x],"NYC",rentals,owned,condo,coop,small)
  return(out)
}
)
stopCluster(cl)
# out.df.hold <- out.df
out.df <- as.data.frame(do.call("rbind",tmp.out)
                        ,stringsAsFactors=F)
colnames(out.df) <- c("Year","AREA","Rentals","Owned","Condo","Coop","Small")
out.df <- out.df %>% 
  mutate(
    Rentals = as.numeric(Rentals)
    ,Owned = as.numeric(Owned)
    ,Condo = as.numeric(Condo)
    ,Coop = as.numeric(Coop)
    ,Small = as.numeric(Small)
    ,Total_v1 = Rentals + Owned
    ,Total = Rentals + Condo + Coop + Small
  )
# out.df.hold2 <- out.df
# out.df.hold <- out.df

# out.df <- out.df.hold
out.df <- left_join(out.df
                    ,condo_rental.df %>% 
                      mutate(Year = as.character(Year))
) %>% 
  mutate(
    Coop.rentals = Coop * .15
    ,Condo.rentals = Condo * condo_rental.adj
    ,Rentals.adj = (Rentals + (Coop * .15) + (Condo * condo_rental.adj))
    ,Coop.adj = Coop * .85
    ,Condo.adj = Condo * (1-condo_rental.adj)
    ,Owned = Condo + Coop + Small
    ,Owned.cc = Condo.adj + Coop.adj
    ,Year = as.numeric(Year)
    ,AreaType = "City"
  )

out.df.nyc <- out.df

out.df <- bind_rows(out.df.nyc,out.df.boro,out.df.nbrhd) %>% 
  select(Year,AREA,AreaType,Rentals,Condo,Coop,Small,Total,Rentals.adj,Coop.rentals,Condo.rentals,Condo.adj,Coop.adj,Owned.cc) %>% 
  mutate(Rentals.adj = round(Rentals.adj)
         ,Coop.rentals = round(Coop.rentals)
         ,Condo.rentals = round(Condo.rentals)
         ,Coop.adj = round(Coop.adj)
         ,Condo.adj = round(Condo.adj)
         ,Owned.cc = round(Owned.cc)
         # ) %>% 
  # rename(Rentals_inclusive = Rentals.adj
  #        ,Condo_less_rentals = Condo.adj
  #        ,Coop_less_rentals = Coop.adj
  #        ,CondoCoop_ownerocc = Owned.cc)
)
getwd()
setwd("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Data")
write.csv(out.df,
          paste("multifam_supply"
                ,format(
                  Sys.time()
                  ,"%Y%m%d_%H%M%S"
                )
                ,".csv"
                ,sep="")
          ,row.names=F
)

saveRDS(out.df,paste("multifam_supply"
                     ,format(
                       Sys.time()
                       ,"%Y%m%d_%H%M%S"
                     )
                     ,".rds"
                     ,sep="")
        )

saveRDS(pluto.aug
        ,paste("pluto_augmented"
               ,format(
                 Sys.time()
                 ,"%Y%m%d_%H%M%S"
               )
               ,".rds"
               ,sep="")
)

gg.df.tmp <- bind_rows(out.df %>% 
              select(Year,Coop.rentals) %>% 
              mutate(Class = "Coop.rentals") %>% 
              rename(Value = Coop.rentals)
            ,bind_rows(out.df %>% 
                         select(Year,Condo.rentals) %>% 
                         mutate(Class = "Condo.rentals") %>% 
                         rename(Value = Condo.rentals)
                       ,bind_rows(out.df %>% 
                                    select(Year,Coop.adj) %>% 
                                    mutate(Class = "Coop.adj") %>% 
                                    rename(Value = Coop.adj)
                                  ,bind_rows(out.df %>% 
                                               select(Year,Condo.adj) %>% 
                                               mutate(Class = "Condo.adj") %>% 
                                               rename(Value = Condo.adj)
                                             ,bind_rows(out.df %>% 
                                                          select(Year,Rentals.adj) %>% 
                                                          mutate(Class = "Rentals.adj") %>% 
                                                          rename(Value = Rentals.adj)
                                                        ,bind_rows(out.df %>% 
                                                                     select(Year,Coop) %>% 
                                                                     mutate(Class = "Coop") %>% 
                                                                     rename(Value = Coop)
                                                                   ,bind_rows(out.df %>% 
                                                                                select(Year,Rentals) %>% 
                                                                                mutate(Class = "Rentals") %>% 
                                                                                rename(Value = Rentals)
                                                                              ,out.df %>% 
                                                                                select(Year,Condo) %>% 
                                                                                mutate(Class = "Condo") %>% 
                                                                                rename(Value = Condo)
                                                                   )
                                                        )
                                             )
                                  )
                       )
            )
  )


## new plots 

## Total number of rentals NYC line
gg.df <- gg.df.tmp %>% 
  filter(Class=="Rentals.adj")

nycrentals.line <- ggplot(gg.df,aes(y=Value,x=Year)) + 
  geom_line(stat="identity",size=1.5,color=hwe_colors["hwe_blue"]) + 
  scale_x_continuous(
    # limits=c(1980,2016),
                     breaks=(seq(from=1980,to=2016,by=2))
  ) +
  scale_y_continuous(
    # limits=c(1750000,2250000),
                     breaks=(seq(from=1750000,to=2250000,by=50000)),
                     labels= scales::comma) +
  coord_cartesian(
    xlim=c(1980,2016)
    ,ylim=c(1850000,2200000)
  ) +
  labs(x="Year",
       y="Number of Rental Units",
       # color="Borough",
       title="NYC Multifamily Rental Stock",
       subtitle="Including Rented Condos and Coops"
       # caption=""
  ) +
  theme_hwe()
nycrentals.line


color.order <- rev(
  c(
    "hwe_blue"
    ,"hwe_yellow"
    ,"hwe_orange"
    ,"hwe_grey_lighter"
    ,"hwe_grey_darker"
    # ,"hwe_darkblue"
  )
)

hwe_colors["hwe_yellow"] <- "#ADC436"
hwe_colors["hwe_orange"] <- "#EF5B28"


gg.df <- gg.df.tmp %>% 
  filter(
    # (grepl(".adj",Class) | grepl(".rentals",Class))
    #      & Class %in% "Rentals.adj"
         Class %in% 
           c("Rentals"
             ,"Coop.rentals"
             ,"Condo.rentals"
             ,"Coop.adj"
             ,"Condo.adj"
             )
         
         ) %>% 
  mutate(Class = factor(Class
                        ,levels = rev(c("Rentals","Coop.rentals","Condo.rentals","Coop.adj","Condo.adj"))
                        ,labels = rev(c("Rentals","Coop Rentals","Condo Rentals","Coops","Condos"))))

resistock_all.bar <- ggplot(gg.df, aes(x=Year,y=Value,group=Class,fill=Class)) +
  geom_bar(stat="identity",position="stack") + 
  scale_x_continuous(
    # limits=c(1980,2016),
                     breaks=(seq(from=1980,to=2016,by=2))
  ) +
  scale_y_continuous(
    breaks=(seq(from=0,to=3000000,by=250000))
    # ,limits=c(1000000,3000000)
                     ,labels= scales::comma) +
  # scale_fill_manual(values = rev(as.character(hwe_colors)[1:5])) +
  scale_fill_manual(values = c("#F1794B","#ED5729","#AFD8E4","#5DBBD0","#01AFC7")) +
  coord_cartesian(ylim=c(1500000,2750000)
                  ,xlim=c(1980,2016)
                  ) +
  labs(x="Year",
       y="Units",
       fill="",
       title="NYC Multifamily Resi Stock by Unit Type"
       # subtitle="Of Floorplans 50,000 SF or Less and on 20th Story or Higher\nBins of 2500 SF"
       # caption=""
  ) +
  theme_hwe()

resistock_all.bar 
# "#ED5729" "#01AFC7" "#D1D2D4" "#8C8C8C" "#EFBC3D"
# c("#F1794B","#ED5729","#AFD8E4","#5DBBD0","#01AFC7")



as.character(hwe_colors)[1:5]


color.order <- rev(
  c(
    "hwe_blue"
    ,"hwe_grey_lighter"
    ,"hwe_grey_darker"
    # ,"hwe_darkblue"
  )
)

gg.df <- gg.df.tmp %>% 
  filter(
    Class %in% 
      c("Rentals"
        ,"Coop"
        ,"Condo"
      )
    
  ) %>% 
  mutate(Class = factor(Class
                        ,levels = rev(c("Rentals","Coop","Condo"))
                        ,labels = rev(c("Rentals","Coops","Condos"))))

resistock_rcc.bar <- ggplot(gg.df, aes(x=Year,y=Value,group=Class,fill=Class)) +
  geom_bar(stat="identity",position="stack") + 
  scale_x_continuous(
    # limits=c(1980,2016),
                     breaks=(seq(from=1980,to=2016,by=2))
  ) +
  scale_y_continuous(
    breaks=(seq(from=0,to=3000000,by=250000))
    # ,limits=c(1000000,3000000)
    ,labels= scales::comma) +
  scale_fill_manual(values = rev(as.character(hwe_colors)[1:3])) +
  coord_cartesian(ylim=c(1000000,2750000)
                  ,xlim=c(1980,2016)
                  ) +
  labs(x="Year",
       y="Units",
       fill="",
       title="NYC Multifamily Resi Stock by Unit Type"
       # subtitle="Of Floorplans 50,000 SF or Less and on 20th Story or Higher\nBins of 2500 SF"
       # caption=""
  ) +
  theme_hwe()
resistock_rcc.bar


## chart showing color palette
hwe_palette <- as.data.frame(
  cbind(
    # paste(names(hwe_colors),as.character(hwe_colors),sep=" - ")
    as.character(hwe_colors)
    ,names(hwe_colors)
    ,rep(1,length(hwe_colors))
  )
)
colnames(hwe_palette) <- c("hwe_color","hwe_colorname","value")
hwe_palette <- hwe_palette  %>% 
  mutate(hwe_color = factor(hwe_color
                             ,levels = as.character(hwe_colors)
                             ,labels = as.character(hwe_colors))
         ,hwe_colorname = factor(hwe_colorname
                                 ,levels = names(hwe_colors)
                                 ,labels = names(hwe_colors)
                                 )
         )


ggplot(hwe_palette, aes(x=hwe_colorname,y=value
                  # ,group=Class
                  ,fill=hwe_color)
       ) +
  geom_bar(
    stat="identity"
    # ,position="stack"
    ) + 
  scale_fill_manual(
    # values=as.character(hwe_colors)
    values=as.character(hwe_palette[,"hwe_color"])
    ) +
  theme_hwe()


# ggplot(out.df, aes(x=Year,y=Rentals.v2)) + 
#   geom_line(color="blue",size=1.5) + 
#   scale_y_continuous(limits=c(1850000,2200000)
#                      ,breaks = seq(from=1850000,to=2200000,by=50000)
#                      ,labels = scales::comma
#                      ) + 
#   scale_x_continuous(limits=c(1980,2016)
#                      ,breaks= seq(from=1980,to=2016,by=2)) +
#   labs(x="Year",
#        y="Rental Units",
#        # color="Borough",
#        title="Number of Multifamily Rental Units",
#        subtitle="Adjusted for Condo and Co-op Rentals",
#        caption="*Does not include rented one and two family structures"
#   ) +
#   theme_hwe()
  
save.image(
  paste("multifam_rental_supply_workspace_"
        ,format(Sys.time(),"%Y%m%d %H%M")
        ,".RData"
        ,sep="")
)



out.df.NYC <- out.df
multifam.df <- bind_rows(out.df.NYC, out.df.boroughs)

# borough.check <- multifam.df %>% group_by(Year) %>% filter(Borough!="NYC") %>% summarize(rentals=sum(Rentals)
#                                                                         ,owned=sum(Owned))
# nyc.check <- multifam.df %>% group_by(Year) %>% filter(Borough=="NYC") %>% summarize(rentals=sum(Rentals)
#                                                                                      ,owned=sum(Owned))
# View(cbind(borough.check,nyc.check))
# identical(nyc.check,borough.check)


setwd("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Data")
save.image("multifamilypipeline.RData")
saveRDS(multifam.df,"multifam_timeseries.rds")
write.csv(multifam.df,"multifam_timeseries.csv",
          row.names=F)


multifam.df[,"rental_change"] <- NA
for(i in unique(multifam.df[,"Borough"])){
  restr <- multifam.df[,"Borough"]==i
  tmp.vec <- multifam.df[restr,"Rentals"]
  tmp.secondvec <- vector(length=length(tmp.vec))
  for(j in 1:(length(tmp.vec)-1)){
    tmp.secondvec[j] <- tmp.vec[j] - tmp.vec[j+1]
  }
  multifam.df[restr,"rental_change"] <- tmp.secondvec
}

multifam.df <- multifam.df %>% mutate(perc_change = rental_change/Rentals)

multifam.df[,"Year"] <- as.numeric(multifam.df[,"Year"])

#######################################
## am doin a plot
## wow




## Lineplot of rental units by borough 
gg.df <- multifam.df %>% filter(!Borough %in% c("NYC","STATEN ISLAND")) %>% 
  mutate(Borough = factor(Borough, levels=c("MANHATTAN","BROOKLYN","QUEENS","BRONX")))

rentunits.line <- ggplot(gg.df, aes(y=Rentals,x=Year,group=Borough,color=Borough)) + 
  geom_line(stat="identity",size=1.5) + 
  scale_x_continuous(limits=c(1980,2016),
    breaks=(seq(from=1980,to=2016,by=2))
                     ) +
  scale_y_continuous(limits=c(250000,700000),
    breaks=(seq(from=0,to=1000000,by=50000))
                     ,labels= scales::comma) +
  labs(x="Year",
       y="Rental Units",
       color="Borough",
       title="Number of Rental Units By Year and Borough"
       # subtitle="Of Floorplans 50,000 SF or Less and on 20th Story or Higher\nBins of 2500 SF"
       # caption=""
  ) +
  theme_hwe()
rentunits.line



multifam.df <- readRDS("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Data/multifam_timeseries.rds")
## Absolute change in rental units by borough stacked bar chart
gg.df <- multifam.df %>% filter(Borough!="NYC") %>% 
  mutate(Borough = factor(Borough, levels=c("MANHATTAN","BROOKLYN","QUEENS","BRONX","STATEN ISLAND")))

rentchange.bar <- ggplot(gg.df, aes(y=rental_change,x=Year,group=Borough,fill=Borough)) + 
  # geom_line(stat="identity",size=1.5) + 
  geom_bar(stat="identity",position="stack") + 
  geom_hline(aes(yintercept=0),color="grey20",size=.5
             # ,linetype="dashed"
             ) +
  scale_x_continuous(limits=c(1980,2016),
    breaks=(seq(from=1980,to=2016,by=2))
  ) +
  scale_y_continuous(limits=c(-40000,20000),
    breaks=(seq(from=-50000,to=20000,by=5000))
                     ,labels= scales::comma) +
  labs(x="Year",
       y="Rental Units Added/Lost",
       color="Borough",
       title="Change in Number of Rental Units YoY"
       # subtitle="Of Floorplans 50,000 SF or Less and on 20th Story or Higher\nBins of 2500 SF"
       # caption=""
  ) +
  theme_hwe()
rentchange.bar


## Percentage change in rental units MN,BK and NYC bar chart
gg.df <- multifam.df %>% filter(Borough %in% c(
  # "NYC",
  "MANHATTAN","BROOKLYN")) %>% 
  mutate(Borough = factor(Borough, levels = c(
    # "NYC",
    "MANHATTAN","BROOKLYN")))
rentchange_perc.bar <- ggplot(gg.df, aes(y=perc_change,x=Year
                                         # ,group=Borough
                                         ,fill=Borough)) + 
  # geom_line(stat="identity",size=1.5) + 
  geom_bar(stat="identity",position="dodge") + 
  scale_x_continuous(limits=c(1980,2017),
    breaks=(seq(from=1980,to=2017,by=2))
  ) +
  # scale_y_continuous(limits=c(-.035,.01),
  #   breaks=(seq(from=-.035,to=.01,by=.005)),
  #                    labels= scales::percent) +
  # scale_y_continuous(limits=c(1750000,2100000),
  #                    breaks=(seq(from=1750000,to=2100000,by=50000)),
  #                    labels= scales::comma) +
  labs(x="Year",
       y="% Rental Units Added/Lost",
       color="Borough",
       title="Percentage Change in Number of Rental Units YoY"
       # subtitle="Of Floorplans 50,000 SF or Less and on 20th Story or Higher\nBins of 2500 SF"
       # caption=""
  ) +
  theme_hwe()
rentchange_perc.bar



## Percentage change in rental units MN,BK and NYC line
gg.df <- multifam.df %>% filter(Borough %in% c(
  # "NYC",
  "MANHATTAN","BROOKLYN")) %>% 
  mutate(Borough = factor(Borough, levels = c(
    # "NYC",
    "MANHATTAN","BROOKLYN")))

rentchange_perc.line <- ggplot(gg.df, aes(y=perc_change,x=Year
                                         # ,group=Borough
                                         ,color=Borough)) + 
  geom_hline(aes(yintercept=0),color="grey20",size=.5
             # ,linetype="dashed"
  ) +
  geom_line(stat="identity",size=1.5) + 
  scale_x_continuous(limits=c(1980,2016),
                     breaks=(seq(from=1980,to=2016,by=2))
  ) +
  scale_y_continuous(limits=c(-.03,.025),
                     breaks=(seq(from=-.03,to=.025,by=.005)),
                     labels= scales::percent) +
  # scale_y_continuous(limits=c(1750000,2100000),
  #                    breaks=(seq(from=1750000,to=2100000,by=50000)),
  #                    labels= scales::comma) +
  labs(x="Year",
       y="% Rental Units Added/Lost",
       color="Borough",
       title="Percentage Change in Number of Rental Units YoY"
       # subtitle="Of Floorplans 50,000 SF or Less and on 20th Story or Higher\nBins of 2500 SF"
       # caption=""
  ) +
  theme_hwe()
rentchange_perc.line



## Percentage change in rental units NYC line
gg.df <- multifam.df %>% filter(Borough %in% c("NYC")) %>% 
  mutate(neg=perc_change<0)

rentchange_perc_NYC.line <- ggplot(gg.df, aes(y=perc_change,x=Year
                                              # ,fill=neg
                                              # ,color=neg
                                              )) + 
  geom_hline(aes(yintercept=0),color="grey20",size=1,linetype="dashed") +
    geom_line(stat="identity",size=1.5,color="blue") +
  # geom_area(data=gg.df,aes(fill=neg)) +
  # geom_bar(stat="identity",position="dodge") + 
  scale_x_continuous(limits=c(1980,2016),
                     breaks=(seq(from=1980,to=2016,by=2))
  ) +
  scale_y_continuous(limits=c(-.02,.02),
                     breaks=(seq(from=-.02,to=.02,by=.005)),
                     labels= scales::percent) +
  # scale_y_continuous(limits=c(1750000,2100000),
  #                    breaks=(seq(from=1750000,to=2100000,by=50000)),
  #                    labels= scales::comma) +
  labs(x="Year",
       y="% Rental Units Added/Lost",
       # color="Borough",
       title="Percentage Change in Number of Rental Units YoY"
       # subtitle="Of Floorplans 50,000 SF or Less and on 20th Story or Higher\nBins of 2500 SF"
       # caption=""
  ) +
  theme_hwe()
rentchange_perc_NYC.line



## Total number of rentals NYC line
gg.df <- multifam.df %>% filter(Borough=="NYC")
nycrentals.line <- ggplot(gg.df,aes(y=Rentals,x=Year)) + 
  geom_line(stat="identity",size=1.5,color="blue") + 
  scale_x_continuous(limits=c(1980,2016),
    breaks=(seq(from=1980,to=2016,by=2))
  ) +
  scale_y_continuous(limits=c(1750000,2100000),
    breaks=(seq(from=1750000,to=2100000,by=50000)),
    labels= scales::comma) +
  labs(x="Year",
       y="Number of Rental Units",
       # color="Borough",
       title="Number of Rental Units in NYC"
       # subtitle="Of Floorplans 50,000 SF or Less and on 20th Story or Higher\nBins of 2500 SF"
       # caption=""
  ) +
  theme_hwe()
nycrentals.line

chart6.df <- as.data.frame(ggplot_build(nycrentals.line)$data)[,1:2] %>% 
  rename(Year = x
         ,Units=y) %>% 
  filter(!is.na(Units))


## Number of rental units by borough stacked bar
gg.df <- multifam.df %>% filter(!Borough %in% c("NYC")) %>% 
  mutate(Borough = factor(Borough, levels=c("MANHATTAN","BROOKLYN","QUEENS","BRONX","STATEN ISLAND")))

nycrentals.stackbar <- ggplot(gg.df,aes(y=Rentals,x=Year,fill=Borough)) + 
  # geom_line(stat="identity",size=1.5,color="blue") + 
  geom_bar(stat="identity",position="stack") + 
  scale_x_continuous(breaks=(seq(from=1980,to=2016,by=2))
  ) +
  scale_y_continuous(limits=c(0,2250000),
                     breaks=(seq(from=0,to=2250000,by=250000)),
                     labels= scales::comma) +
  labs(x="Year",
       y="Number of Rental Units",
       # color="Borough",
       title="Number of Rental Units in NYC"
       # subtitle="Of Floorplans 50,000 SF or Less and on 20th Story or Higher\nBins of 2500 SF"
       # caption=""
  ) +
  theme_hwe()
nycrentals.stackbar




## Number of conversions by borough
gg.df <- Conversions_boro.df %>% 
  filter(AREA %in% c("MANHATTAN","BROOKLYN","QUEENS","BRONX")) %>% 
  mutate(AREA = factor(AREA, levels=c("MANHATTAN","BROOKLYN","QUEENS","BRONX")))

conversion.bar <- ggplot(gg.df, aes(y=CONVERTED_UNITS,x=YEAR,group=AREA,fill=AREA)) + 
  # geom_line(stat="identity",size=1.5) + 
  geom_bar(stat="identity",position="stack") + 
  scale_x_continuous(limits=c(1980,2017),
    breaks=(seq(from=1980,to=2017,by=2))
  ) +
  scale_y_continuous(limits=c(0,45000),
    breaks=(seq(from=0,to=45000,by=5000)),
    labels= scales::comma
    ) +
  scale_fill_manual(values=rev(as.character(hwe_colors[4:1]))) + 
  labs(x="Year",
       y="Rental Units Converted",
       color="Borough",
       title="Number of Rental Units Converted to Condo/Coop"
       # subtitle="Of Floorplans 50,000 SF or Less and on 20th Story or Higher\nBins of 2500 SF"
       # caption=""
  ) +
  theme_hwe()
conversion.bar



## looking at total number of units, owned and 
tmp.df1 <- as.data.frame(
  cbind(multifam.df[,c("Year","Borough","Rentals")],"Rentals")
  ,stringsAsFactors=F
)
colnames(tmp.df1) <- c("Year","Borough","Value","Tenure")

tmp.df2 <- as.data.frame(
  cbind(multifam.df[,c("Year","Borough","Owned")],"Owned")
,stringsAsFactors=F
)
colnames(tmp.df2) <- c("Year","Borough","Value","Tenure")

gg.df <- as.data.frame(
  rbind(tmp.df1,tmp.df2)
  ,stringsAsFactors=F)

gg.df.main <- gg.df %>% 
  mutate(
  Year = as.numeric(as.character(Year))
  ,Borough = factor(Borough, levels=c("NYC","MANHATTAN","BROOKLYN","QUEENS","BRONX","STATEN ISLAND"))
  ,Value = as.numeric(as.character(Value))
  ,Tenure = factor(as.character(Tenure),levels=c("Owned","Rentals"))
)


## total units NYC
gg.df <- gg.df.main %>% filter(Borough=="NYC")

tot_NYC.stackbar <- ggplot(gg.df, aes(x=Year,y=Value,fill=Tenure)) + 
  geom_bar(stat="identity",position="stack") +
  scale_x_continuous(limits=c(1980,2016),
                     breaks=(seq(from=1980,to=2016,by=2))
  ) +
  scale_y_continuous(limits=c(0,3500000),
                     breaks=(seq(from=0,to=3500000,by=500000)),
                     labels= scales::comma
  ) +
  labs(x = "Year",
       y = "Total Units",
       fill = "Ownership Type",
       title = "Total Number of Resi Units - NYC"
       # subtitle="Of Floorplans 50,000 SF or Less and on 20th Story or Higher\nBins of 2500 SF"
       # caption=""
  ) +
  theme_hwe() + 
  theme(axis.text.x=element_text(angle = 65, hjust = 1))

tot_NYC.stackbar


## total units MN
gg.df <- gg.df.main %>% filter(Borough=="MANHATTAN")

tot_MN.stackbar <- ggplot(gg.df, aes(x=Year,y=Value,fill=Tenure)) + 
  geom_bar(stat="identity",position="stack") +
  scale_x_continuous(limits=c(1980,2016),
                     breaks=(seq(from=1980,to=2016,by=2))
  ) +
  scale_y_continuous(limits=c(0,1000000),
                     breaks=(seq(from=0,to=1000000,by=100000)),
                     labels= scales::comma
  ) +
  labs(x = "Year",
       y = "Total Units",
       fill = "Ownership Type",
       title = "Total Number of Resi Units - Manhattan"
       # subtitle="Of Floorplans 50,000 SF or Less and on 20th Story or Higher\nBins of 2500 SF"
       # caption=""
  ) +
  theme_hwe() + 
  theme(axis.text.x=element_text(angle = 65, hjust = 1))

tot_MN.stackbar


## total units BK
gg.df <- gg.df.main %>% filter(Borough=="BROOKLYN")

tot_BK.stackbar <- ggplot(gg.df, aes(x=Year,y=Value,fill=Tenure)) + 
  geom_bar(stat="identity",position="stack") +
  scale_x_continuous(limits=c(1980,2016),
                     breaks=(seq(from=1980,to=2016,by=2))
  ) +
  scale_y_continuous(limits=c(0,1000000),
                     breaks=(seq(from=0,to=1000000,by=100000)),
                     labels= scales::comma
  ) +
  labs(x = "Year",
       y = "Total Units",
       fill = "Ownership Type",
       title = "Total Number of Resi Units - Brooklyn"
       # subtitle="Of Floorplans 50,000 SF or Less and on 20th Story or Higher\nBins of 2500 SF"
       # caption=""
  ) +
  theme_hwe() + 
  theme(axis.text.x=element_text(angle = 65, hjust = 1))

tot_BK.stackbar


## total units QN
gg.df <- gg.df.main %>% filter(Borough=="QUEENS")

tot_QN.stackbar <- ggplot(gg.df, aes(x=Year,y=Value,fill=Tenure)) + 
  geom_bar(stat="identity",position="stack") +
  scale_x_continuous(limits=c(1980,2016),
                     breaks=(seq(from=1980,to=2016,by=2))
  ) +
  scale_y_continuous(limits=c(0,900000),
                     breaks=(seq(from=0,to=900000,by=100000)),
                     labels= scales::comma
  ) +
  labs(x = "Year",
       y = "Total Units",
       fill = "Ownership Type",
       title = "Total Number of Resi Units - Queens"
       # subtitle="Of Floorplans 50,000 SF or Less and on 20th Story or Higher\nBins of 2500 SF"
       # caption=""
  ) +
  theme_hwe() + 
  theme(axis.text.x=element_text(angle = 65, hjust = 1))

tot_QN.stackbar


## total units BX
gg.df <- gg.df.main %>% filter(Borough=="BRONX")

tot_BX.stackbar <- ggplot(gg.df, aes(x=Year,y=Value,fill=Tenure)) + 
  geom_bar(stat="identity",position="stack") +
  scale_x_continuous(limits=c(1980,2016),
                     breaks=(seq(from=1980,to=2016,by=2))
  ) +
  scale_y_continuous(limits=c(0,600000),
                     breaks=(seq(from=0,to=600000,by=100000)),
                     labels= scales::comma
  ) +
  labs(x = "Year",
       y = "Total Units",
       fill = "Ownership Type",
       title = "Total Number of Resi Units - Bronx"
       # subtitle="Of Floorplans 50,000 SF or Less and on 20th Story or Higher\nBins of 2500 SF"
       # caption=""
  ) +
  theme_hwe() + 
  theme(axis.text.x=element_text(angle = 65, hjust = 1))

tot_BX.stackbar



#######################################
#######################################
## by neighborhood 
#######################################
#######################################


tmp.summary <- out.df %>% 
  filter(!is.na(AREA)) %>% 
  group_by(AREA) %>% 
  summarize(min_rental = min(Rentals_inclusive)
            ,min_rental_year = Year[which.min(Rentals_inclusive)]
            ,max_rental = max(Rentals_inclusive)
            ,max_rental_year = Year[which.max(Rentals_inclusive)]
            # ,min_rental_prop = min(Rentals_inclusive/(Rentals_inclusive + Condo_less_rentals + Coop_less_rentals))
            ,min_rental_prop = min(Rentals_inclusive/(Total-Small))
            ,min_rental_prop_year = Year[which.min(Rentals_inclusive/(Total-Small))[1]]
            ,max_rental_prop = max(Rentals_inclusive/(Total-Small))
            ,max_rental_prop_year = Year[which.max(Rentals_inclusive/(Total-Small))[length(which.max(Rentals_inclusive/(Total-Small)))]]
            ,min_max_diff = max_rental - min_rental
            ,min_max_diff_prop = min_max_diff/(Total[Year==1980]-Small[Year==1980])
            ,rentals_80 = Rentals_inclusive[Year==1980]
            ,rentals_16 = Rentals_inclusive[Year==2016]
            ,diff_80_16 = Rentals_inclusive[Year==2016] - Rentals_inclusive[Year==1980]
            ,diff_80_16_prop = diff_80_16/Rentals_inclusive[Year==1980]
  ) %>% arrange(diff_80_16_prop)



hwe_colors["hwe_yellow"] <- "#ADC436"
hwe_colors["hwe_orange"] <- "#EF5B28"


mutate_df.fun <- function(gg.df.tmp1){
  gg.df.tmp <- bind_rows(gg.df.tmp1 %>% 
                           select(Year,Small) %>% 
                           mutate(Class = "Small") %>% 
                           rename(Value = Small)
                         ,bind_rows(gg.df.tmp1 %>% 
                                      select(Year,Coop.rentals) %>% 
                                      mutate(Class = "Coop.rentals") %>% 
                                      rename(Value = Coop.rentals)
                                    ,bind_rows(gg.df.tmp1 %>% 
                                                 select(Year,Condo.rentals) %>% 
                                                 mutate(Class = "Condo.rentals") %>% 
                                                 rename(Value = Condo.rentals)
                                               ,bind_rows(gg.df.tmp1 %>% 
                                                            select(Year,Coop.adj) %>% 
                                                            mutate(Class = "Coop.adj") %>% 
                                                            rename(Value = Coop.adj)
                                                          ,bind_rows(gg.df.tmp1 %>% 
                                                                       select(Year,Condo.adj) %>% 
                                                                       mutate(Class = "Condo.adj") %>% 
                                                                       rename(Value = Condo.adj)
                                                                     ,bind_rows(gg.df.tmp1 %>% 
                                                                                  select(Year,Rentals.adj) %>% 
                                                                                  mutate(Class = "Rentals.adj") %>% 
                                                                                  rename(Value = Rentals.adj)
                                                                                ,bind_rows(gg.df.tmp1 %>% 
                                                                                             select(Year,Coop) %>% 
                                                                                             mutate(Class = "Coop") %>% 
                                                                                             rename(Value = Coop)
                                                                                           ,bind_rows(gg.df.tmp1 %>% 
                                                                                                        select(Year,Rentals) %>% 
                                                                                                        mutate(Class = "Rentals") %>% 
                                                                                                        rename(Value = Rentals)
                                                                                                      ,gg.df.tmp1 %>% 
                                                                                                        select(Year,Condo) %>% 
                                                                                                        mutate(Class = "Condo") %>% 
                                                                                                        rename(Value = Condo)
                                                                                           )
                                                                                )
                                                                     )
                                                          )
                                               )
                                    )
                         )
  )
  return(gg.df.tmp)
}



## Forest Hills 

gg.df <- mutate_df.fun(
  out.df %>% 
    filter(AREA == "Forest Hills")
) %>% 
  filter(Class=="Rentals.adj")


FHrentals.line <- ggplot(gg.df,aes(y=Value,x=Year)) + 
  geom_line(stat="identity",size=1.5,color=hwe_colors["hwe_blue"]) + 
  scale_x_continuous(
    # limits=c(1980,2016),
    breaks=(seq(from=1980,to=2016,by=2))
  ) +
  scale_y_continuous(
    # limits=c(1750000,2250000),
    breaks=(seq(from=10000,to=30000,by=2500)),
    labels= scales::comma) +
  coord_cartesian(
    xlim=c(1980,2016)
    ,ylim=c(10000,25000)
  ) +
  labs(x="Year",
       y="Number of Rental Units",
       # color="Borough",
       title="Forest Hills Multifamily Rental Stock",
       subtitle="Including Rented Condos and Coops",
       caption="Excluding Single and Two Family Buildings"
  ) +
  theme_hwe()
FHrentals.line


color.order <- rev(
  c(
    "hwe_blue"
    ,"hwe_yellow"
    ,"hwe_orange"
    ,"hwe_grey_lighter"
    ,"hwe_grey_darker"
    # ,"hwe_darkblue"
  )
)

hwe_colors["hwe_yellow"] <- "#ADC436"
hwe_colors["hwe_orange"] <- "#EF5B28"



color.order.inc_small <- rev(
  c(
    "hwe_blue"
    ,"hwe_yellow"
    ,"hwe_orange"
    ,"hwe_grey_lighter"
    ,"hwe_grey_darker"
    ,"hwe_darkblue"
  )
)



gg.df <- mutate_df.fun(
  out.df %>% 
    filter(AREA == "Forest Hills")
) %>% 
  filter(
    # (grepl(".adj",Class) | grepl(".rentals",Class))
    #      & Class %in% "Rentals.adj"
    Class %in% 
      c("Rentals"
        ,"Coop.rentals"
        ,"Condo.rentals"
        ,"Coop.adj"
        ,"Condo.adj"
        # ,"Small"
      )
  ) %>% 
  mutate(Class = factor(Class
                        # ,levels = rev(c("Rentals","Coop.rentals","Condo.rentals","Coop.adj","Condo.adj","Small"))
                        # ,labels = rev(c("Rentals","Coop Rentals","Condo Rentals","Coops","Condos","1-2 Fam"))))
                        ,levels = rev(c("Rentals","Coop.rentals","Condo.rentals","Coop.adj","Condo.adj"))
                        ,labels = rev(c("Rentals","Coop Rentals","Condo Rentals","Coops","Condos"))))

FHresistock_all.bar <- ggplot(gg.df, aes(x=Year,y=Value,group=Class,fill=Class)) +
  geom_bar(stat="identity",position="stack") + 
  scale_x_continuous(
    # limits=c(1980,2016),
    breaks=(seq(from=1980,to=2016,by=2))
  ) +
  scale_y_continuous(
    breaks=(seq(from=0,to=100000,by=5000))
    # ,limits=c(1000000,3000000)
    ,labels= scales::comma) +
  scale_fill_manual(values = as.character(hwe_colors[color.order])) +
  coord_cartesian(ylim=c(0,35000)
                  ,xlim=c(1980,2016)
  ) +
  labs(x="Year",
       y="Units",
       fill="",
       title="Forest Hills Multifamily Resi Stock by Unit Type"
       # subtitle="Of Floorplans 50,000 SF or Less and on 20th Story or Higher\nBins of 2500 SF"
       # caption=""
  ) +
  theme_hwe()

FHresistock_all.bar 




## west village 
gg.df <- mutate_df.fun(
  out.df %>% 
    filter(AREA == "West Village")
) %>% 
  filter(Class=="Rentals.adj")


WVrentals.line <- ggplot(gg.df,aes(y=Value,x=Year)) + 
  geom_line(stat="identity",size=1.5,color=hwe_colors["hwe_blue"]) + 
  scale_x_continuous(
    # limits=c(1980,2016),
    breaks=(seq(from=1980,to=2016,by=2))
  ) +
  scale_y_continuous(
    # limits=c(1750000,2250000),
    breaks=(seq(from=0,to=50000,by=2500)),
    labels= scales::comma) +
  coord_cartesian(
    xlim=c(1980,2016)
    ,ylim=c(15000,22500)
  ) +
  labs(x="Year",
       y="Number of Rental Units",
       # color="Borough",
       title="West Village Multifamily Rental Stock",
       subtitle="Including Rented Condos and Coops",
       caption="Excluding Single and Two Family Buildings"
  ) +
  theme_hwe()
WVrentals.line


color.order <- rev(
  c(
    "hwe_blue"
    ,"hwe_yellow"
    ,"hwe_orange"
    ,"hwe_grey_lighter"
    ,"hwe_grey_darker"
    # ,"hwe_darkblue"
  )
)



gg.df <- mutate_df.fun(
  out.df %>% 
    filter(AREA == "West Village")
) %>% 
  filter(
    Class %in% 
      c("Rentals"
        ,"Coop.rentals"
        ,"Condo.rentals"
        ,"Coop.adj"
        ,"Condo.adj"
      )
  ) %>% 
  mutate(Class = factor(Class
                        ,levels = rev(c("Rentals","Coop.rentals","Condo.rentals","Coop.adj","Condo.adj"))
                        ,labels = rev(c("Rentals","Coop Rentals","Condo Rentals","Coops","Condos"))))

WVresistock_all.bar <- ggplot(gg.df, aes(x=Year,y=Value,group=Class,fill=Class)) +
  geom_bar(stat="identity",position="stack") + 
  scale_x_continuous(
    # limits=c(1980,2016),
    breaks=(seq(from=1980,to=2016,by=2))
  ) +
  scale_y_continuous(
    breaks=(seq(from=0,to=100000,by=2500))
    # ,limits=c(1000000,3000000)
    ,labels= scales::comma) +
  scale_fill_manual(values = as.character(hwe_colors[color.order])) +
  coord_cartesian(ylim=c(10000,25000)
                  ,xlim=c(1980,2016)
  ) +
  labs(x="Year",
       y="Units",
       fill="",
       title="West Village Multifamily Resi Stock by Unit Type",
       caption="Excluding Single and Two Family Structures"
  ) +
  theme_hwe()

WVresistock_all.bar 



## Brooklyn Heights
gg.df <- mutate_df.fun(
  out.df %>% 
    filter(AREA == "Brooklyn Heights")
) %>% 
  filter(Class=="Rentals.adj")


BHrentals.line <- ggplot(gg.df,aes(y=Value,x=Year)) + 
  geom_line(stat="identity",size=1.5,color=hwe_colors["hwe_blue"]) + 
  scale_x_continuous(
    # limits=c(1980,2016),
    breaks=(seq(from=1980,to=2016,by=2))
  ) +
  scale_y_continuous(
    # limits=c(1750000,2250000),
    breaks=(seq(from=0,to=50000,by=500)),
    labels= scales::comma) +
  coord_cartesian(
    xlim=c(1980,2016)
    ,ylim=c(6000,8500)
  ) +
  labs(x="Year",
       y="Number of Rental Units",
       # color="Borough",
       title="Brooklyn Heights Multifamily Rental Stock",
       subtitle="Including Rented Condos and Coops",
       caption="Excluding Single and Two Family Structures"
  ) +
  theme_hwe()
BHrentals.line




color.order <- rev(
  c(
    "hwe_blue"
    ,"hwe_yellow"
    ,"hwe_orange"
    ,"hwe_grey_lighter"
    ,"hwe_grey_darker"
    # ,"hwe_darkblue"
  )
)



gg.df <- mutate_df.fun(
  out.df %>% 
    filter(AREA == "Brooklyn Heights")
) %>% 
  filter(
    Class %in% 
      c("Rentals"
        ,"Coop.rentals"
        ,"Condo.rentals"
        ,"Coop.adj"
        ,"Condo.adj"
      )
  ) %>% 
  mutate(Class = factor(Class
                        ,levels = rev(c("Rentals","Coop.rentals","Condo.rentals","Coop.adj","Condo.adj"))
                        ,labels = rev(c("Rentals","Coop Rentals","Condo Rentals","Coops","Condos"))))

BHresistock_all.bar <- ggplot(gg.df, aes(x=Year,y=Value,group=Class,fill=Class)) +
  geom_bar(stat="identity",position="stack") + 
  scale_x_continuous(
    # limits=c(1980,2016),
    breaks=(seq(from=1980,to=2016,by=2))
  ) +
  scale_y_continuous(
    breaks=(seq(from=0,to=100000,by=1000))
    # ,limits=c(1000000,3000000)
    ,labels= scales::comma) +
  scale_fill_manual(values = as.character(hwe_colors[color.order])) +
  coord_cartesian(ylim=c(2500,12500)
                  ,xlim=c(1980,2016)
  ) +
  labs(x="Year",
       y="Units",
       fill="",
       title="Brooklyn Heights Multifamily Resi Stock by Unit Type",
       caption="Excluding Single and Two Family Structures"
  ) +
  theme_hwe()

BHresistock_all.bar 



## Greenwich Village
gg.df <- mutate_df.fun(
  out.df %>% 
    filter(AREA == "Greenwich Village")
) %>% 
  filter(Class=="Rentals.adj")


GVrentals.line <- ggplot(gg.df,aes(y=Value,x=Year)) + 
  geom_line(stat="identity",size=1.5,color=hwe_colors["hwe_blue"]) + 
  scale_x_continuous(
    # limits=c(1980,2016),
    breaks=(seq(from=1980,to=2016,by=2))
  ) +
  scale_y_continuous(
    # limits=c(1750000,2250000),
    breaks=(seq(from=0,to=50000,by=1000)),
    labels= scales::comma) +
  coord_cartesian(
    xlim=c(1980,2016)
    ,ylim=c(10000,16000)
  ) +
  labs(x="Year",
       y="Number of Rental Units",
       # color="Borough",
       title="Greenwich Village Multifamily Rental Stock",
       subtitle="Including Rented Condos and Coops",
       caption="Excluding Single and Two Family Structures"
  ) +
  theme_hwe()
GVrentals.line




color.order <- rev(
  c(
    "hwe_blue"
    ,"hwe_yellow"
    ,"hwe_orange"
    ,"hwe_grey_lighter"
    ,"hwe_grey_darker"
    # ,"hwe_darkblue"
  )
)



gg.df <- mutate_df.fun(
  out.df %>% 
    filter(AREA == "Greenwich Village")
) %>% 
  filter(
    Class %in% 
      c("Rentals"
        ,"Coop.rentals"
        ,"Condo.rentals"
        ,"Coop.adj"
        ,"Condo.adj"
      )
  ) %>% 
  mutate(Class = factor(Class
                        ,levels = rev(c("Rentals","Coop.rentals","Condo.rentals","Coop.adj","Condo.adj"))
                        ,labels = rev(c("Rentals","Coop Rentals","Condo Rentals","Coops","Condos"))))

GVresistock_all.bar <- ggplot(gg.df, aes(x=Year,y=Value,group=Class,fill=Class)) +
  geom_bar(stat="identity",position="stack") + 
  scale_x_continuous(
    # limits=c(1980,2016),
    breaks=(seq(from=1980,to=2016,by=2))
  ) +
  scale_y_continuous(
    breaks=(seq(from=0,to=100000,by=1000))
    # ,limits=c(1000000,3000000)
    ,labels= scales::comma) +
  scale_fill_manual(values = as.character(hwe_colors[color.order])) +
  coord_cartesian(ylim=c(7000,20000)
                  ,xlim=c(1980,2016)
  ) +
  labs(x="Year",
       y="Units",
       fill="",
       title="Greenwich Village Multifamily Resi Stock by Unit Type",
       caption="Excluding Single and Two Family Structures"
  ) +
  theme_hwe()

GVresistock_all.bar 





## Upper West Side
gg.df <- mutate_df.fun(
  out.df %>% 
    filter(AREA == "Upper West Side")
) %>% 
  filter(Class=="Rentals.adj")


UWSrentals.line <- ggplot(gg.df,aes(y=Value,x=Year)) + 
  geom_line(stat="identity",size=1.5,color=hwe_colors["hwe_blue"]) + 
  scale_x_continuous(
    # limits=c(1980,2016),
    breaks=(seq(from=1980,to=2016,by=2))
  ) +
  scale_y_continuous(
    # limits=c(1750000,2250000),
    breaks=(seq(from=0,to=500000,by=5000)),
    labels= scales::comma) +
  coord_cartesian(
    xlim=c(1980,2016)
    ,ylim=c(70000,100000)
  ) +
  labs(x="Year",
       y="Number of Rental Units",
       # color="Borough",
       title="Upper West Side Multifamily Rental Stock",
       subtitle="Including Rented Condos and Coops",
       caption="Excluding Single and Two Family Structures"
  ) +
  theme_hwe()
UWSrentals.line




color.order <- rev(
  c(
    "hwe_blue"
    ,"hwe_yellow"
    ,"hwe_orange"
    ,"hwe_grey_lighter"
    ,"hwe_grey_darker"
  )
)



gg.df <- mutate_df.fun(
  out.df %>% 
    filter(AREA == "Upper West Side")
) %>% 
  filter(
    Class %in% 
      c("Rentals"
        ,"Coop.rentals"
        ,"Condo.rentals"
        ,"Coop.adj"
        ,"Condo.adj"
      )
  ) %>% 
  mutate(Class = factor(Class
                        ,levels = rev(c("Rentals","Coop.rentals","Condo.rentals","Coop.adj","Condo.adj"))
                        ,labels = rev(c("Rentals","Coop Rentals","Condo Rentals","Coops","Condos"))))

UWSresistock_all.bar <- ggplot(gg.df, aes(x=Year,y=Value,group=Class,fill=Class)) +
  geom_bar(stat="identity",position="stack") + 
  scale_x_continuous(
    # limits=c(1980,2016),
    breaks=(seq(from=1980,to=2016,by=2))
  ) +
  scale_y_continuous(
    breaks=(seq(from=0,to=1000000,by=5000))
    # ,limits=c(1000000,3000000)
    ,labels= scales::comma) +
  scale_fill_manual(values = as.character(hwe_colors[color.order])) +
  coord_cartesian(ylim=c(50000,125000)
                  ,xlim=c(1980,2016)
  ) +
  labs(x="Year",
       y="Units",
       fill="",
       title="Upper West Side Multifamily Resi Stock by Unit Type",
       caption="Excluding Single and Two Family Structures"
  ) +
  theme_hwe()

UWSresistock_all.bar 



## Upper East Side
gg.df <- mutate_df.fun(
  out.df %>% 
    filter(AREA == "Upper East Side")
) %>% 
  filter(Class=="Rentals.adj")


UESrentals.line <- ggplot(gg.df,aes(y=Value,x=Year)) + 
  geom_line(stat="identity",size=1.5,color=hwe_colors["hwe_blue"]) + 
  scale_x_continuous(
    # limits=c(1980,2016),
    breaks=(seq(from=1980,to=2016,by=2))
  ) +
  scale_y_continuous(
    # limits=c(1750000,2250000),
    breaks=(seq(from=0,to=500000,by=2500)),
    labels= scales::comma) +
  coord_cartesian(
    xlim=c(1980,2016)
    ,ylim=c(82500,97500)
  ) +
  labs(x="Year",
       y="Number of Rental Units",
       # color="Borough",
       title="Upper East Side Multifamily Rental Stock",
       subtitle="Including Rented Condos and Coops",
       caption="Excluding Single and Two Family Structures"
  ) +
  theme_hwe()
UESrentals.line




color.order <- rev(
  c(
    "hwe_blue"
    ,"hwe_yellow"
    ,"hwe_orange"
    ,"hwe_grey_lighter"
    ,"hwe_grey_darker"
  )
)



gg.df <- mutate_df.fun(
  out.df %>% 
    filter(AREA == "Upper East Side")
) %>% 
  filter(
    Class %in% 
      c("Rentals"
        ,"Coop.rentals"
        ,"Condo.rentals"
        ,"Coop.adj"
        ,"Condo.adj"
      )
  ) %>% 
  mutate(Class = factor(Class
                        ,levels = rev(c("Rentals","Coop.rentals","Condo.rentals","Coop.adj","Condo.adj"))
                        ,labels = rev(c("Rentals","Coop Rentals","Condo Rentals","Coops","Condos"))))

UESresistock_all.bar <- ggplot(gg.df, aes(x=Year,y=Value,group=Class,fill=Class)) +
  geom_bar(stat="identity",position="stack") + 
  scale_x_continuous(
    # limits=c(1980,2016),
    breaks=(seq(from=1980,to=2016,by=2))
  ) +
  scale_y_continuous(
    breaks=(seq(from=0,to=500000,by=10000))
    # ,limits=c(1000000,3000000)
    ,labels= scales::comma) +
  scale_fill_manual(values = as.character(hwe_colors[color.order])) +
  coord_cartesian(ylim=c(60000,140000)
                  ,xlim=c(1980,2016)
  ) +
  labs(x="Year",
       y="Units",
       fill="",
       title="Upper East Side Multifamily Resi Stock by Unit Type",
       caption="Excluding Single and Two Family Structures"
  ) +
  theme_hwe()

UESresistock_all.bar 





## Williamsburg
gg.df <- mutate_df.fun(
  out.df %>% 
    filter(AREA == "Williamsburg")
) %>% 
  filter(Class=="Rentals.adj")


WillyBrentals.line <- ggplot(gg.df,aes(y=Value,x=Year)) + 
  geom_line(stat="identity",size=1.5,color=hwe_colors["hwe_blue"]) + 
  scale_x_continuous(
    # limits=c(1980,2016),
    breaks=(seq(from=1980,to=2016,by=2))
  ) +
  # scale_y_continuous(
  #   # limits=c(1750000,2250000),
  #   breaks=(seq(from=0,to=500000,by=2500)),
  #   labels= scales::comma) +
  # coord_cartesian(
  #   xlim=c(1980,2016)
  #   ,ylim=c(82500,97500)
  # ) +
  labs(x="Year",
       y="Number of Rental Units",
       # color="Borough",
       title="Williamsburg Multifamily Rental Stock",
       subtitle="Including Rented Condos and Coops",
       caption="Excluding Single and Two Family Structures"
  ) +
  theme_hwe()
WillyBrentals.line




color.order <- rev(
  c(
    "hwe_blue"
    ,"hwe_yellow"
    ,"hwe_orange"
    ,"hwe_grey_lighter"
    ,"hwe_grey_darker"
  )
)



gg.df <- mutate_df.fun(
  out.df %>% 
    filter(AREA == "Williamsburg")
) %>% 
  filter(
    Class %in% 
      c("Rentals"
        ,"Coop.rentals"
        ,"Condo.rentals"
        ,"Coop.adj"
        ,"Condo.adj"
      )
  ) %>% 
  mutate(Class = factor(Class
                        ,levels = rev(c("Rentals","Coop.rentals","Condo.rentals","Coop.adj","Condo.adj"))
                        ,labels = rev(c("Rentals","Coop Rentals","Condo Rentals","Coops","Condos"))))

WillyBresistock_all.bar <- ggplot(gg.df, aes(x=Year,y=Value,group=Class,fill=Class)) +
  geom_bar(stat="identity",position="stack") + 
  scale_x_continuous(
    # limits=c(1980,2016),
    breaks=(seq(from=1980,to=2016,by=2))
  ) +
  # scale_y_continuous(
  #   breaks=(seq(from=0,to=500000,by=10000))
  #   # ,limits=c(1000000,3000000)
  #   ,labels= scales::comma) +
  scale_fill_manual(values = as.character(hwe_colors[color.order])) +
  # coord_cartesian(ylim=c(60000,140000)
  #                 ,xlim=c(1980,2016)
  # ) +
  labs(x="Year",
       y="Units",
       fill="",
       title="Williamsburg Multifamily Resi Stock by Unit Type",
       caption="Excluding Single and Two Family Structures"
  ) +
  theme_hwe()

WillyBresistock_all.bar 




## BROOKLYN
gg.df <- mutate_df.fun(
  out.df %>% 
    filter(AREA == "BROOKLYN")
) %>% 
  filter(Class=="Rentals.adj")


BROOKLYNrentals.line <- ggplot(gg.df,aes(y=Value,x=Year)) + 
  geom_line(stat="identity",size=1.5,color=hwe_colors["hwe_blue"]) + 
  scale_x_continuous(
    # limits=c(1980,2016),
    breaks=(seq(from=1980,to=2016,by=2))
  ) +
  scale_y_continuous(
    # limits=c(1750000,2250000),
    breaks=(seq(from=0,to=1000000,by=25000)),
    labels= scales::comma) +
  coord_cartesian(
    xlim=c(1980,2016)
    ,ylim=c(550000,700000)
  ) +
  labs(x="Year",
       y="Number of Rental Units",
       # color="Borough",
       title="Brooklyn Multifamily Rental Stock",
       subtitle="Including Rented Condos and Coops",
       caption="Excluding Single and Two Family Structures"
  ) +
  theme_hwe()
BROOKLYNrentals.line




color.order <- rev(
  c(
    "hwe_blue"
    ,"hwe_yellow"
    ,"hwe_orange"
    ,"hwe_grey_lighter"
    ,"hwe_grey_darker"
  )
)



gg.df <- mutate_df.fun(
  out.df %>% 
    filter(AREA == "BROOKLYN")
) %>% 
  filter(
    Class %in% 
      c("Rentals"
        ,"Coop.rentals"
        ,"Condo.rentals"
        ,"Coop.adj"
        ,"Condo.adj"
      )
  ) %>% 
  mutate(Class = factor(Class
                        ,levels = rev(c("Rentals","Coop.rentals","Condo.rentals","Coop.adj","Condo.adj"))
                        ,labels = rev(c("Rentals","Coop Rentals","Condo Rentals","Coops","Condos"))))

BROOKLYNresistock_all.bar <- ggplot(gg.df, aes(x=Year,y=Value,group=Class,fill=Class)) +
  geom_bar(stat="identity",position="stack") + 
  scale_x_continuous(
    # limits=c(1980,2016),
    breaks=(seq(from=1980,to=2016,by=2))
  ) +
  scale_y_continuous(
    breaks=(seq(from=0,to=750000,by=50000))
    # ,limits=c(1000000,3000000)
    ,labels= scales::comma) +
  scale_fill_manual(values = as.character(hwe_colors[color.order])) +
  coord_cartesian(ylim=c(400000,750000)
                  ,xlim=c(1980,2016)
  ) +
  labs(x="Year",
       y="Units",
       fill="",
       title="Brooklyn Multifamily Resi Stock by Unit Type",
       caption="Excluding Single and Two Family Structures"
  ) +
  theme_hwe()

BROOKLYNresistock_all.bar 


## Midwood
gg.df <- mutate_df.fun(
  out.df %>% 
    filter(AREA == "Midwood")
) %>% 
  filter(Class=="Rentals.adj")


Midwoodrentals.line <- ggplot(gg.df,aes(y=Value,x=Year)) + 
  geom_line(stat="identity",size=1.5,color=hwe_colors["hwe_blue"]) + 
  scale_x_continuous(
    # limits=c(1980,2016),
    breaks=(seq(from=1980,to=2016,by=2))
  ) +
  scale_y_continuous(
    # limits=c(1750000,2250000),
    breaks=(seq(from=0,to=100000,by=1000)),
    labels= scales::comma) +
  coord_cartesian(
    xlim=c(1980,2016)
    ,ylim=c(15000,20000)
  ) +
  labs(x="Year",
       y="Number of Rental Units",
       # color="Borough",
       title="Midwood Multifamily Rental Stock",
       subtitle="Including Rented Condos and Coops",
       caption="Excluding Single and Two Family Structures"
  ) +
  theme_hwe()
Midwoodrentals.line




color.order <- rev(
  c(
    "hwe_blue"
    ,"hwe_yellow"
    ,"hwe_orange"
    ,"hwe_grey_lighter"
    ,"hwe_grey_darker"
  )
)



gg.df <- mutate_df.fun(
  out.df %>% 
    filter(AREA == "Midwood")
) %>% 
  filter(
    Class %in% 
      c("Rentals"
        ,"Coop.rentals"
        ,"Condo.rentals"
        ,"Coop.adj"
        ,"Condo.adj"
      )
  ) %>% 
  mutate(Class = factor(Class
                        ,levels = rev(c("Rentals","Coop.rentals","Condo.rentals","Coop.adj","Condo.adj"))
                        ,labels = rev(c("Rentals","Coop Rentals","Condo Rentals","Coops","Condos"))))

Midwoodresistock_all.bar <- ggplot(gg.df, aes(x=Year,y=Value,group=Class,fill=Class)) +
  geom_bar(stat="identity",position="stack") + 
  scale_x_continuous(
    # limits=c(1980,2016),
    breaks=(seq(from=1980,to=2016,by=2))
  ) +
  scale_y_continuous(
    breaks=(seq(from=0,to=750000,by=1000))
    # ,limits=c(1000000,3000000)
    ,labels= scales::comma) +
  scale_fill_manual(values = as.character(hwe_colors[color.order])) +
  coord_cartesian(ylim=c(10000,21000)
                  ,xlim=c(1980,2016)
  ) +
  labs(x="Year",
       y="Units",
       fill="",
       title="Midwood Multifamily Resi Stock by Unit Type",
       caption="Excluding Single and Two Family Structures"
  ) +
  theme_hwe()

Midwoodresistock_all.bar 


Morningside Heights

## Morningside Heights
gg.df <- mutate_df.fun(
  out.df %>% 
    filter(AREA == "Morningside Heights")
) %>% 
  filter(Class=="Rentals.adj")


MorningsideHeightsrentals.line <- ggplot(gg.df,aes(y=Value,x=Year)) + 
  geom_line(stat="identity",size=1.5,color=hwe_colors["hwe_blue"]) + 
  scale_x_continuous(
    # limits=c(1980,2016),
    breaks=(seq(from=1980,to=2016,by=2))
  ) +
  scale_y_continuous(
    # limits=c(1750000,2250000),
    breaks=(seq(from=0,to=100000,by=500)),
    labels= scales::comma) +
  coord_cartesian(
    xlim=c(1980,2016)
    ,ylim=c(11000,14000)
  ) +
  labs(x="Year",
       y="Number of Rental Units",
       # color="Borough",
       title="Morningside Heights Multifamily Rental Stock",
       subtitle="Including Rented Condos and Coops",
       caption="Excluding Single and Two Family Structures"
  ) +
  theme_hwe()
MorningsideHeightsrentals.line




color.order <- rev(
  c(
    "hwe_blue"
    ,"hwe_yellow"
    ,"hwe_orange"
    ,"hwe_grey_lighter"
    ,"hwe_grey_darker"
  )
)



gg.df <- mutate_df.fun(
  out.df %>% 
    filter(AREA == "Morningside Heights")
) %>% 
  filter(
    Class %in% 
      c("Rentals"
        ,"Coop.rentals"
        ,"Condo.rentals"
        ,"Coop.adj"
        ,"Condo.adj"
      )
  ) %>% 
  mutate(Class = factor(Class
                        ,levels = rev(c("Rentals","Coop.rentals","Condo.rentals","Coop.adj","Condo.adj"))
                        ,labels = rev(c("Rentals","Coop Rentals","Condo Rentals","Coops","Condos"))))

MorningsideHeightsresistock_all.bar <- ggplot(gg.df, aes(x=Year,y=Value,group=Class,fill=Class)) +
  geom_bar(stat="identity",position="stack") + 
  scale_x_continuous(
    # limits=c(1980,2016),
    breaks=(seq(from=1980,to=2016,by=2))
  ) +
  scale_y_continuous(
    breaks=(seq(from=0,to=750000,by=1000))
    # ,limits=c(1000000,3000000)
    ,labels= scales::comma) +
  scale_fill_manual(values = as.character(hwe_colors[color.order])) +
  coord_cartesian(ylim=c(9000,16000)
                  ,xlim=c(1980,2016)
  ) +
  labs(x="Year",
       y="Units",
       fill="",
       title="Morningside Heights Multifamily Resi Stock by Unit Type",
       caption="Excluding Single and Two Family Structures"
  ) +
  theme_hwe()

MorningsideHeightsresistock_all.bar 



write.csv(multifam.df,"multifam_df.csv",row.names=F)

#######################################
## Joining REIS rents data 

rents.df <- read.xlsx("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Data/REIS_multifam_rents.xlsx"
                      ,sheetIndex=1)



