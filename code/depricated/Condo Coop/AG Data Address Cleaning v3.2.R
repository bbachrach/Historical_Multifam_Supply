
library(dplyr)
library(lubridate)
library(stringr)
library(parallel)
library(doParallel)
library(httr)
library(RCurl)
library(RJSONIO)

setwd("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/AG Data")

ag_raw.df <-   read.csv("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/AG Data/Coop_Condo_AGdata.csv"
                        ,stringsAsFactors=F)

AGdata.df <- ungroup(
  read.csv("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/AG Data/Coop_Condo_AGdata.csv"
           ,stringsAsFactors=F) %>% 
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
                                          ,"STATEN_ISLAND"
                                          ,PLAN_BORO_COUNTY
                                  )
                          )
      )
      ,PLAN_ZIP_OLD = PLAN_ZIP
    ) %>% 
    group_by(PLAN_ID) %>% 
    mutate(
      PLAN_ID_UNIQUE = paste(PLAN_ID,0:(n()-1),sep="_")
    )
)


## load pluto 

pluto <- readRDS("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Project Data/PLUTO/pluto_lean_compressed_2017.rds")
pluto <- pluto %>% 
  select(BBL,Borough,Block,Lot,ZipCode,Address
                          ,BldgClass,Easements,OwnerType,OwnerName
                          ,BldgArea,ComArea,ResArea,OfficeArea,RetailArea,GarageArea,StrgeArea,FactryArea,OtherArea
                          ,NumBldgs,NumFloors,UnitsRes,UnitsTotal,YearBuilt,
                          CondoNo,XCoord,YCoord,lat,lon) %>% 
  mutate(
    borocode = ifelse(Borough=="MN"
                      ,1
                      ,ifelse(Borough=="BX"
                              ,2
                              ,ifelse(Borough=="BK"
                                      ,3
                                      ,ifelse(Borough=="QN"
                                              ,4
                                              ,ifelse(
                                                Borough=="SI"
                                                ,5
                                                ,str_sub(BBL,start=1,end=1)
                                              )
                                      )
                              )
                      )
    )
    ,BBL= paste(
      borocode
      ,Block
      ,Lot
      ,sep="_"),
    CondoNo= ifelse(CondoNo==0,
                    0,
                    paste(str_sub(BBL,start=1,end=1),CondoNo,sep="_")),
    BldgClass_1 = str_sub(BldgClass,start=1,end=1)
  )


pluto_boro.levs <- as.character(as.data.frame(
  pluto %>%
    filter(!duplicated(Borough)) %>% 
    arrange(substr(BBL,start=1,stop=1))
  ,stringsAsFactors=F
)[,"Borough"])


AGdata.df <- AGdata.df %>%
  mutate(PLAN_ZIP = ifelse(!grepl("[[:digit:]]",PLAN_ZIP)
                           ,NA
                           ,PLAN_ZIP
  )
  )

AGdata.df <- AGdata.df %>%
  filter(
    PLAN_RESIDENTIAL %in% c("RESIDENTIAL","SPLIT/RESIDENTIAL")
         )


## make address key 
ag_tmp <- AGdata.df

address.key <- as.data.frame(
  cbind(
    ag_tmp[,"PLAN_ID_UNIQUE"]
    ,ag_tmp[,"PLAN_NAME"]
    ,ag_tmp[,"PLAN_STREET"]
  )
  ,stringsAsFactors=F
) %>%
  setNames(c("PLAN_ID_UNIQUE","PLAN_NAME","Original_Address"))

## fix the Saints
tmp.key <- address.key %>%
  filter(
    (grepl("ST",Original_Address) & grepl("MARKS",Original_Address)) | 
      (grepl("ST",Original_Address) & (grepl("NICHOLAS",Original_Address) | grepl("NICOLAS",Original_Address))) |
      (grepl("ST",Original_Address) & grepl("JOHN",Original_Address)) |
      (grepl("ST",Original_Address) & grepl("PAUL",Original_Address))
  ) %>%
  mutate(
    Corrected_Address = gsub("\\.","",Original_Address)
    ,Corrected_Address = gsub("ST MARK","SAINT MARK",Corrected_Address)
    ,Corrected_Address = gsub("STMARK","SAINT MARK",Corrected_Address)
    ,Corrected_Address = gsub("ST NICHOLAS","SAINT NICHOLAS",Corrected_Address)
    ,Corrected_Address = gsub("STNICHOLAS","SAINT NICHOLAS",Corrected_Address)
    ,Corrected_Address = gsub("ST JOHN","SAINT JOHN",Corrected_Address)
    ,Corrected_Address = gsub("STJOHN","SAINT JOHN",Corrected_Address)
    ,Corrected_Address = gsub("ST PAUL","SAINT PAUL",Corrected_Address)
    ,Corrected_Address = gsub("STPAUL","SAINT PAUL",Corrected_Address)
  )

address.key <- left_join(
  address.key
  ,tmp.key %>%
    select(PLAN_ID_UNIQUE,Corrected_Address)
) %>%
  mutate(Corrected_Address = ifelse(is.na(Corrected_Address)
                                    ,Original_Address
                                    ,Corrected_Address
                                    )
         )

## Do the corrections in the order they were done before but with dplyr syntax
address.key <- address.key %>%
  mutate(
    Corrected_Address = gsub("\\*[^[:blank:]].*","",Corrected_Address)
    ,Corrected_Address = gsub("STREET.*","STREET",Corrected_Address)
    ,Corrected_Address = gsub(" AKA*.","",Corrected_Address)
    ,Corrected_Address = gsub("\\(RESUBMIT*.","",Corrected_Address)
    ,Corrected_Address = gsub("ST/.*","ST",Corrected_Address)
    ,Corrected_Address = trimws(as.character(Corrected_Address))
    
    ,Corrected_Address = gsub("\\(REAR)","",Corrected_Address)
    ,Corrected_Address = gsub("\\(.*","",Corrected_Address)
    ,Corrected_Address = gsub("63, 65, & 67", "67",Corrected_Address)
    ,Corrected_Address = gsub("AKA.*","",Corrected_Address)
    ,Corrected_Address = gsub("A/K/A.*","",Corrected_Address)
    ,Corrected_Address = paste(Corrected_Address,"       ",sep="")
    ,Corrected_Address = gsub(" ST    "," ST.",Corrected_Address)
    ,Corrected_Address = gsub("([[:alnum:]][[:alnum:]] ST\\.).*","\\1",Corrected_Address)
    ,Corrected_Address = gsub("CONDOMINIUM","",Corrected_Address)
    ,Corrected_Address = gsub("ST\\.","STREET",Corrected_Address)
    
    ,Corrected_Address = gsub("\\*CPS.*.","",Corrected_Address)
    ,Corrected_Address = gsub("\\*.*","",Corrected_Address)
    ,Corrected_Address = gsub("CPS.*","",Corrected_Address)
    
    ,Corrected_Address = gsub(",    ","",paste(Corrected_Address,"     ",sep=""))
    ,Corrected_Address = gsub("AVE\\.  "," AVENUE  ",Corrected_Address)
    ,Corrected_Address = gsub("AVE  "," AVENUE  ",Corrected_Address)
    ,Corrected_Address = gsub(" S\\.T"," STREET",Corrected_Address)
    ,Corrected_Address = gsub(" PL\\.  "," PLACE  ",Corrected_Address)
    ,Corrected_Address = gsub(" PL   "," PLACE   ",Corrected_Address)
    ,Corrected_Address = gsub(" RD\\.  "," ROAD  ",Corrected_Address)
    ,Corrected_Address = gsub(" RD   "," ROAD   ",Corrected_Address)
    ,Corrected_Address = gsub(" PKWY","  PARKWAY",Corrected_Address)
    ,Corrected_Address = gsub(" BLVD","  BOULEVARD",Corrected_Address)
    ,Corrected_Address = gsub(" AVENUE"," AVENUE ",Corrected_Address)
    
    ,Corrected_Address = gsub(" E\\. "," EAST ",Corrected_Address)
    ,Corrected_Address = gsub(" W\\. "," WEST ",Corrected_Address)
    ,Corrected_Address = gsub(" N\\. "," NORTH ",Corrected_Address)
    ,Corrected_Address = gsub(" S\\. "," SOUTH ",Corrected_Address)
        
    ,Corrected_Address = gsub("'","",Corrected_Address)
    ,Corrected_Address = gsub("\\.    ","",Corrected_Address)
    ,Corrected_Address = trimws(Corrected_Address)
  )




address.key <- left_join(address.key
                         ,ag_tmp %>% 
                           filter(!duplicated(PLAN_STREET)) %>% 
                           select(PLAN_STREET,AMND_NO,PLAN_BORO,PLAN_ZIP) %>% 
                           dplyr::rename(Original_Address = PLAN_STREET)
                         ,by="Original_Address") %>% 
  filter(!duplicated(Corrected_Address))

address.key <- address.key %>%
  mutate(
    housenum = paste(gsub(" .*","",Corrected_Address),"   ")
    ,housenum = trimws(gsub(",  ","",housenum))
    ,street = sub(".+? ", "",Corrected_Address)
    ,borough = PLAN_BORO
    ,zip=PLAN_ZIP
    ,street = gsub(
      "/.*",""
      ,gsub(",","",street)
    )
  )


## call to NYC Geoclient 

url_create.fun <- function(baseurl = "https://api.cityofnewyork.us/geoclient/v1/address.json?"
                           ,keyid = "&app_id=43dc14b8&app_key=2df323d9189dfd5f1d2c8bf569843588"
                           ,housenum
                           ,street
                           ,borough=NA
                           ,borocode=NA
                           ,zip=zip
){
  url <- gsub(" ","+"
              ,paste(
                baseurl
                ,"houseNumber="
                ,housenum
                ,"&street="
                ,street
                ,sep=""
              )
  )
  
  if(!is.na(borough)){
    url <- gsub(" ","+"
                ,paste(
                  url
                  ,"&borough="
                  ,borough
                  ,sep=""
                )
    )
  }
  
  if(!is.na(zip)){
    url <- gsub(" ","+"
                ,paste(
                  url
                  ,"&zip="
                  ,zip
                  ,sep=""
                )
    )
  }
  
  url <- gsub(" ","+"
              ,paste(
                url
                ,keyid
                ,sep=""
              )
  )
  return(url)
}


tmp.vec <- unlist(
  lapply(1:nrow(address.key), function(x){
    url <- url_create.fun(
      housenum = address.key[x,"housenum"]
      ,street =  address.key[x,"street"]
      ,borough =  address.key[x,"borough"]
      ,zip =  address.key[x,"zip"]
    )
    return(url)}
  )
)

address.key[,"url"] <- tmp.vec


start <- 1
end <- length(tmp.vec)

cl <- makeCluster(round(detectCores()*.75))
registerDoParallel(cl)

ptm <- proc.time()
initial_output.list <- foreach(i = start:end
                               , .packages = c("httr")) %dopar% {
                                 out <- try(GET(url = tmp.vec[i]))
                                 return(out)
                               }
stopCluster(cl)
api_call.time <- proc.time() - ptm


tmp_out.list <- list()
for(i in 1:length(initial_output.list)){
  output <- initial_output.list[[i]]
  
  if(class(output)=="response"){
    output <- content(output)
    if(class(output)=="list"){
      output <- output$address
    }
  }
  
  if(class(output)=="list"){
    
    out <- list(
      bbl = output$bbl
      ,buildingIdentificationNumber = output$buildingIdentificationNumber
      ,houseNumber = output$houseNumber
      ,houseNumberIn = output$houseNumberIn
      ,firstStreetNameNormalized = output$firstStreetNameNormalized
      ,boePreferredStreetName = output$boePreferredStreetName
      ,giStreetName1 = output$giStreetName1
      ,streetName1In = output$streetName1In
      ,returnCode1a  = output$returnCode1a
      ,cooperativeIdNumber = output$cooperativeIdNumber
      ,latitude = output$latitude
      ,longitude = output$longitude
      ,url = tmp.vec[i]
    )
    
  } else {
    out <- list(url = tmp.vec[i])
  }
  out <- t(unlist(out)) %>% as.data.frame()
  tmp_out.list[[i]] <- out
  cat(i,"\n")
}

content_time.all <- proc.time() - ptm

tmp_out.list[which(unlist(
  lapply(tmp_out.list, function(x) class(x))
) != "data.frame")] <- NULL

tmp.out <- bind_rows(tmp_out.list)


# Run through again -------------------------------------------------------

notcoded.df <- bind_rows(
  address.key %>%
    anti_join(
      tmp.out
      ,by="url"
    )
  ,address.key %>%
    semi_join(
      tmp.out %>%
        filter(is.na(bbl))
    )
)

start <-1
end <- nrow(notcoded.df)

cl <- makeCluster(round(detectCores()/2))
registerDoParallel(cl)

ptm <- proc.time()

second_output.list <- foreach(i = start:end
                              , .packages = c("httr")) %dopar% {
                                out <- try(GET(url = notcoded.df[i,"url"]))
                                return(out)
                              }
stopCluster(cl)
second_api_call.time <- proc.time() - ptm


tmp_out2.list <- list()

for(i in 1:length(second_output.list)){
  output <- second_output.list[[i]]
  
  if(class(output)=="response"){
    output <- content(output)
    if(class(output)=="list"){
      output <- output$address
      content.names <- names(output)
    }
  }
  
  if(class(output)=="list"){
    
    out <- list(
      bbl = output$bbl
      ,buildingIdentificationNumber = output$buildingIdentificationNumber
      ,houseNumber = output$houseNumber
      ,houseNumberIn = output$houseNumberIn
      ,firstStreetNameNormalized = output$firstStreetNameNormalized
      ,boePreferredStreetName = output$boePreferredStreetName
      ,giStreetName1 = output$giStreetName1
      ,streetName1In = output$streetName1In
      ,returnCode1a  = output$returnCode1a
      ,cooperativeIdNumber = output$cooperativeIdNumber
      ,latitude = output$latitude
      ,longitude = output$longitude
      ,url = tmp.vec[i]
    )
    
  } else {
    out <- list(url = tmp.vec[i])
  }
  out <- t(unlist(out)) %>% as.data.frame()
  tmp_out2.list[[i]] <- out
  cat(i,"\n")
}

tmp_out2.list[which(unlist(
  lapply(tmp_out2.list, function(x) class(x))
) != "data.frame")] <- NULL

tmp.out2 <- bind_rows(tmp_out2.list) %>%
  filter(returnCode1a %in% c("00","01","88"))

tmp.out <- bind_rows(
  tmp.out
  ,tmp.out2
)

notcoded.df <- bind_rows(
  address.key %>%
    anti_join(
      tmp.out
      ,by="url"
    )
  ,address.key %>%
    semi_join(
      tmp.out %>%
        filter(is.na(bbl))
    )
)



# Write to disk for addresses to be manually fixed ------------------------

# notcoded.df <- notcoded.df %>%
#   left_join(
#     ag_tmp %>%
#       select(PLAN_NAME,PLAN_ID_UNIQUE)
#     ,by="PLAN_ID_UNIQUE"
#   )

notcoded.df <- notcoded.df %>%
  select(PLAN_ID_UNIQUE,Original_Address,PLAN_NAME,Corrected_Address,borough,zip)

## read in already manually coded and drop out
manual_coded.df <- read.csv("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Refresh/data/condo_coop_conversions manual_code 20180419_1604.csv"
                            ,stringsAsFactors=F
)
mancoded_secondary.df <- read.csv("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Refresh/data/secondary_notcoded_condo_coop_conversions manual_code 20180420_1134.csv"
                                  ,stringsAsFactors=F)

notcoded.df <- notcoded.df %>%
  anti_join(
    manual_coded.df
    ,by="PLAN_ID_UNIQUE"
  ) %>%
  anti_join(
    mancoded_secondary.df
    ,by="PLAN_ID_UNIQUE"
  )

write.csv(notcoded.df
          ,"/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Refresh/data/initial_notcoded_condo_coop_include_new_construction.csv"
          ,row.names=F
          ,na=""
)

# notcoded.old <- read.csv("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Refresh/data/initial_notcoded_condo_coop_conversions.csv"
#                          ,stringsAsFactors=F)


# Run through the names which were fixed manually -------------------------

manual_coded.df <- read.csv("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Refresh/data/condo_coop_conversions manual_code 20180419_1604.csv"
                            ,stringsAsFactors=F
)

tmp2.vec <- unlist(
  lapply(1:nrow(manual_coded.df), function(x){
    url <- url_create.fun(
      housenum = manual_coded.df[x,"HOUSE_NUMBER"]
      ,street =  manual_coded.df[x,"STREET_NAME"]
      ,borough =  manual_coded.df[x,"borough"]
      ,zip =  manual_coded.df[x,"zip"]
    )
    return(url)}
  )
)

manual_coded.df[,"url"] <- tmp2.vec

start <-1
end <- nrow(manual_coded.df)

cl <- makeCluster(round(detectCores()/2))
registerDoParallel(cl)

ptm <- proc.time()

second_output.list <- foreach(i = start:end
                              , .packages = c("httr")) %dopar% {
                                out <- try(GET(url = manual_coded.df[i,"url"]))
                                return(out)
                              }
stopCluster(cl)
second_api_call.time <- proc.time() - ptm


tmp_out2.list <- list()

for(i in 1:length(second_output.list)){
  output <- second_output.list[[i]]
  
  if(class(output)=="response"){
    output <- content(output)
    if(class(output)=="list"){
      output <- output$address
      content.names <- names(output)
    }
  }
  
  if(class(output)=="list"){
    
    out <- list(
      bbl = output$bbl
      ,buildingIdentificationNumber = output$buildingIdentificationNumber
      ,houseNumber = output$houseNumber
      ,houseNumberIn = output$houseNumberIn
      ,firstStreetNameNormalized = output$firstStreetNameNormalized
      ,boePreferredStreetName = output$boePreferredStreetName
      ,giStreetName1 = output$giStreetName1
      ,streetName1In = output$streetName1In
      ,returnCode1a  = output$returnCode1a
      ,cooperativeIdNumber = output$cooperativeIdNumber
      ,latitude = output$latitude
      ,longitude = output$longitude
      ,url = manual_coded.df[i,"url"]
    )
    
  } else {
    out <- list(url = tmp.vec[i])
  }
  out <- t(unlist(out)) %>% as.data.frame()
  tmp_out2.list[[i]] <- out
  
  cat(i,"\n")
}


tmp_out2.list[which(unlist(
  lapply(tmp_out2.list, function(x) class(x))
) != "data.frame")] <- NULL

tmp.out2 <- bind_rows(tmp_out2.list)

notcoded_secondary.df <- address.key %>%
  semi_join(tmp.out2 %>%
              filter(is.na(bbl)) %>%
              left_join(manual_coded.df %>%
                          select(url,PLAN_ID_UNIQUE)
                        ,by="url"
              )
            ,by="PLAN_ID_UNIQUE"
  ) %>%
  mutate(bbl = NA) %>%
  select(PLAN_ID_UNIQUE,bbl,Original_Address,PLAN_NAME,Corrected_Address,borough,zip)

# notcoded_secondary.df <- manual_coded.df %>%
#   left_join(tmp.out2 %>%
#               select(url,bbl)
#             ,by="url"
#   )


write.csv(notcoded_secondary.df
          ,"/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Refresh/data/initial_secondary_notcoded_condo_coop_conversions.csv"
          ,row.names=F
          ,na=""
)

save.image(
  paste(
    "/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Refresh/data/AGdata_cleanse_workspace "
    ,format(Sys.time()
            ,"%Y%m%d_%H%S"
    )
    ,".RData"
    ,sep=""
  )
)



# read in and join dataframes ---------------------------------------------

mancoded_secondary.df <- read.csv("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Refresh/data/secondary_notcoded_condo_coop_conversions manual_code 20180420_1134.csv"
                                  ,stringsAsFactors=F)


tmp.out <- tmp.out %>%
  left_join(
    address.key %>%
      select(PLAN_ID_UNIQUE,url)
    ,by="url"
  ) %>%
  filter(!is.na(bbl))

tmp.out2 <- tmp.out2 %>%
  left_join(
    manual_coded.df %>%
      select(PLAN_ID_UNIQUE,url)
    ,by="url"
  ) %>%
  filter(!is.na(bbl))

tmp.out2 <- tmp.out2 %>%
  group_by(PLAN_ID_UNIQUE) %>%
  group_by(bbl) %>%
  mutate(appearances = n()) %>%
  filter(appearances == max(appearances)) %>%
  ungroup() %>%
  filter(!duplicated(bbl))

tmp.out <- bind_rows(tmp.out
                     ,tmp.out2)

tmp.out <- tmp.out %>%
  filter(!is.na(bbl) & !duplicated(bbl)) %>%
  mutate(BBL = 
           paste(as.numeric(str_sub(
             bbl,start=1,end=1
           ))
           ,as.numeric(str_sub(
             bbl,start=2,end=-5
           ))
           ,as.numeric(str_sub(
             bbl,start=-4,end=-1
           ))
           ,sep="_"
           )
  )

tmp.out <- tmp.out %>%
  left_join(mancoded_secondary.df %>%
              select(PLAN_ID_UNIQUE,BBL) %>%
              rename(tmp = BBL)
            ,by="PLAN_ID_UNIQUE"
            ) %>%
  mutate(BBL = ifelse(!is.na(tmp)
                      ,tmp
                      ,BBL
                      )
         )

tmp.out <- bind_rows(
  tmp.out
  ,mancoded_secondary.df %>%
    anti_join(tmp.out
              ,by="PLAN_ID_UNIQUE")
)

AGdata.df <- tmp.out %>%
  select(BBL,PLAN_ID_UNIQUE,latitude,longitude,buildingIdentificationNumber) %>%
  rename(BIN = buildingIdentificationNumber) %>%
  left_join(AGdata.df
            ,by="PLAN_ID_UNIQUE")
  
AGdata.df <- AGdata.df %>%
  mutate(EFFECTIVE_YEAR = ifelse(!is.na(PLAN_DATE_EFFECTIVE)
                                 ,year(PLAN_DATE_EFFECTIVE)
                                 ,year(PLAN_DATE_REVIEWED) + 1
  )
  )


out.df <- AGdata.df %>%
  select(BBL,EFFECTIVE_YEAR,PLAN_TYPE,UNITS) %>%
  rename(ConversionYear = EFFECTIVE_YEAR
         ,ConversionType = PLAN_TYPE
         ,UnitsRes.AG = UNITS) %>%
  mutate(ConversionType = ifelse(
    ConversionType == "COOPERATIVE/CONDOMINIUM"
    ,"COOPERATIVE"
    ,ConversionType
  )
  )

saveRDS(out.df,"/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Refresh/data/condo_coop_conversions_df 20180427_1306.rds")
AGdata.df.hold <- AGdata.df