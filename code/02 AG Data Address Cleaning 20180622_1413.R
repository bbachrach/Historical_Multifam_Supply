

##############################################################################################################
##
## This script cleans condo/coop conversion data recieved from the NY Attorney General office
## It proceeds in the following steps
## 1. Initial readin and munging of data
## 2. Cursory address cleaning based on observation of data
## 3. Calling NYC GeoClient API to standardize addresses and get BBL
## 4. Output unlinkable addresses for manual cleaning
## 5. Call API with cleaned addresses
## 6. Output final addresses that could not be linked to BBL
## 7. Read in, combine and put into format that can be placed in pluto augmented
##
##############################################################################################################

library(dplyr)
library(lubridate)
library(stringr)
library(parallel)
library(doParallel)
library(httr)
library(RCurl)
library(RJSONIO)

setwd("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/AG Data")


# Read in and initial munge -----------------------------------------------

## keep raw format of ag data for reference and troubleshooting
ag_raw.df <-   bind_rows(
  read.csv("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/AG Data/Coop_Condo_AGdata.csv"
                        ,stringsAsFactors=F)
  ,read.csv("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/AG Data/data/Coop_Condo_AGdata 20180523.csv"
            ,stringsAsFactors=F)
  ,read.csv("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/AG Data/data/Coop_Condo_AGdata 20180619.csv"
            ,stringsAsFactors=F)
)

ag_raw.df <- ag_raw.df %>%
  arrange(PLAN_DATE_SUBMITTED) %>%
  filter(!duplicated(gsub("[^[:alnum:]]","",PLAN_ID)))

AGdata.df <- ag_raw.df %>%
    select(PLAN_ID,everything()) %>%
    mutate(
      UNITS = ifelse((is.na(PLAN_UNITS) | PLAN_UNITS==0 | !grepl("[[:digit:]]",PLAN_UNITS))
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
    ) %>%
  ungroup()

## only residential conversions
AGdata.df <- AGdata.df %>%
  filter(grepl("RESIDENTIAL",PLAN_RESIDENTIAL))

AGdata.df <- AGdata.df %>%
  group_by(PLAN_STREET) %>%
  arrange(PLAN_DATE_SUBMITTED,PLAN_DATE_EFFECTIVE)  %>%
  ungroup() %>%
  filter(!duplicated(PLAN_STREET))

## where zip code is an unusable format, set to NA
AGdata.df <- AGdata.df %>%
  mutate(PLAN_ZIP = ifelse(!grepl("[[:digit:]]",PLAN_ZIP)
                           ,NA
                           ,PLAN_ZIP
  )
  )


## read in coded data
mancoded_initial.df <- bind_rows(
  read.csv("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Refresh/data/condo_coop_conversions manual_code 20180419_1604.csv"
           ,stringsAsFactors=F)
  ,read.csv("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Refresh/data/condo_coop_conversions manual_code 20180419.csv"
            ,stringsAsFactors=F)
)

mancoded_initial.df <- mancoded_initial.df %>%
  filter(!duplicated(paste(PLAN_ID_UNIQUE,HOUSE_NUMBER,STREET_NAME)))

mancoded_bbl.df <- bind_rows(
  read.csv("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Refresh/data/secondary_notcoded_condo_coop_conversions manual_code 20180419_1641.csv"
           ,stringsAsFactors=F)
  ,read.csv("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Refresh/data/secondary_notcoded_condo_coop_conversions manual_code 20180420_1134.csv"
            ,stringsAsFactors=F)
  ,read.csv("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Refresh/data/mancoded_condo_coop_20180618.csv"
            ,stringsAsFactors=F)
  ,read.csv("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Refresh/data/mancoded_condo_coop_20180619.csv"
            ,stringsAsFactors=F)
  ,read.csv("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Refresh/data/mancoded_condo_coop_20180621.csv"
            ,stringsAsFactors=F
  ) %>%
    select(PLAN_ID_UNIQUE,BBL,Original_Address,borough,zip,PLAN_NAME,zola) %>%
    rename(bbl = BBL
           ,Corrected_Address = zola
    )
) %>%
  rename(BBL = bbl) %>%
  filter(!duplicated(paste(PLAN_ID_UNIQUE,BBL,Corrected_Address)))


old_method_geocoded <- readRDS("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Refresh/data/google_method_geocoded_addresses_v1.rds") %>%
  mutate(PLAN_ID_UNIQUE = ifelse(grepl("_",plan_id)
                                 ,plan_id
                                 ,paste(plan_id,"_0",sep="")
  )
  )


AGdata_mancoded.df <- AGdata.df %>%
  semi_join(
    mancoded_bbl.df
    ,by="PLAN_ID_UNIQUE"
  )

AGdata.df <- AGdata.df %>%
  anti_join(
    mancoded_bbl.df
    ,by="PLAN_ID_UNIQUE"
  )


## load pluto 
# pluto <- readRDS("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Project Data/PLUTO/pluto_lean_compressed_2017.rds")
# pluto <- pluto %>% 
#   select(BBL,Borough,Block,Lot,ZipCode,Address
#                           ,BldgClass,Easements,OwnerType,OwnerName
#                           ,BldgArea,ComArea,ResArea,OfficeArea,RetailArea,GarageArea,StrgeArea,FactryArea,OtherArea
#                           ,NumBldgs,NumFloors,UnitsRes,UnitsTotal,YearBuilt,
#                           CondoNo,XCoord,YCoord,lat,lon) %>% 
#   mutate(
#     borocode = ifelse(Borough=="MN"
#                       ,1
#                       ,ifelse(Borough=="BX"
#                               ,2
#                               ,ifelse(Borough=="BK"
#                                       ,3
#                                       ,ifelse(Borough=="QN"
#                                               ,4
#                                               ,ifelse(
#                                                 Borough=="SI"
#                                                 ,5
#                                                 ,str_sub(BBL,start=1,end=1)
#                                               )
#                                       )
#                               )
#                       )
#     )
#     ,BBL= paste(
#       borocode
#       ,Block
#       ,Lot
#       ,sep="_"),
#     CondoNo= ifelse(CondoNo==0,
#                     0,
#                     paste(str_sub(BBL,start=1,end=1),CondoNo,sep="_")),
#     BldgClass_1 = str_sub(BldgClass,start=1,end=1)
#   )





# Make Address Key --------------------------------------------------------
ag_tmp <- AGdata.df

## initial key with plan id, plan name and address
address.key <- as.data.frame(
  cbind(
    ag_tmp[,"PLAN_ID_UNIQUE"]
    ,ag_tmp[,"PLAN_NAME"]
    ,ag_tmp[,"PLAN_STREET"]
  )
  ,stringsAsFactors=F
) %>%
  setNames(c("PLAN_ID_UNIQUE","PLAN_NAME","Original_Address"))

## fix "the Saints"
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

## prior munging didn't fix saints first, recombining in such a way that the previous munging can be re-used
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

## re-join with AGdata to get other fields of interest
address.key <- left_join(address.key
                         ,ag_tmp %>% 
                           filter(!duplicated(PLAN_STREET)) %>% 
                           select(PLAN_STREET,AMND_NO,PLAN_BORO,PLAN_ZIP) %>% 
                           dplyr::rename(Original_Address = PLAN_STREET)
                         ,by="Original_Address") %>% 
  filter(!duplicated(Corrected_Address))

## put into a format which will be easily used with API
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

## take the addresses from the manually coded dataframe
address.key <- address.key %>%
  left_join(
    mancoded_initial.df %>%
      select(PLAN_ID_UNIQUE,HOUSE_NUMBER,STREET_NAME)
    ,by="PLAN_ID_UNIQUE"
  ) %>%
  mutate(
    housenum = ifelse(!is.na(HOUSE_NUMBER)
                      ,HOUSE_NUMBER
                      ,housenum
                      )
    ,street = ifelse(!is.na(STREET_NAME)
                     ,STREET_NAME
                     ,street
                     )
  ) %>%
  select(-HOUSE_NUMBER,-STREET_NAME)

# colnames(mancoded_initial.df)




# Call to NYC GeoClient API -----------------------------------------------

## function to create urls
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

## create urls and add into address key
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




# API Call Function -------------------------------------------------------
nyc_geoclient_call.fun <- function(df,max_sleep.time=.001){
  require(httr)
  require(dplyr)
  
  start <-1
  end <- nrow(df)
  
  cl <- makeCluster(round(detectCores()/2))
  registerDoParallel(cl)
  
  ptm <- proc.time()
  
  api_output.list <- foreach(i = start:end
                             , .packages = c("httr")) %dopar% {
                               out <- try(GET(url = df[i,"url"]))
                               Sys.sleep(sample(seq(from=0,to=max_sleep.time,by=.00001),1))
                               return(out)
                             }
  stopCluster(cl)
  api_call.time <- proc.time() - ptm
  
  
  tmp_out.list <- list()
  
  for(i in 1:length(api_output.list)){
    output <-api_output.list[[i]]
    
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
        ,url = df[i,"url"]
      )
      
    } else {
      out <- list(url = tmp.vec[i])
    }
    out <- t(unlist(out)) %>% as.data.frame()
    tmp_out.list[[i]] <- out
    cat(i,"\n")
  }
  
  tmp_out.list[which(unlist(
    lapply(tmp_out.list, function(x) class(x))
  ) != "data.frame")] <- NULL
  
  tmp.out <- bind_rows(tmp_out.list) %>%
    filter(returnCode1a %in% c("00","01","88"))
  
  return(tmp.out)
}

## first API call 
tmp.out <- nyc_geoclient_call.fun(address.key)

## addresses which couldn't be coded by API
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


# second API call ---------------------------------------------------------

tmp.out2 <- nyc_geoclient_call.fun(notcoded.df,max_sleep.time=.1)

out.df <- bind_rows(
  tmp.out
  ,tmp.out2
)

notcoded.df <- bind_rows(
  address.key %>%
    anti_join(
      out.df
      ,by="url"
    )
  ,address.key %>%
    semi_join(
      out.df %>%
        filter(is.na(bbl))
    )
)



# Filter out to only conversions and join with previous google add --------

notcoded.df <- notcoded.df %>%
  left_join(
    AGdata.df %>%
      select(PLAN_ID_UNIQUE,UNITS,PLAN_DATE_SUBMITTED,PLAN_DATE_EFFECTIVE,PLAN_CONSTR_TYPE)
    ,by="PLAN_ID_UNIQUE"
  ) %>%
  filter(PLAN_CONSTR_TYPE %in% c("CONVERSION","REHAB")) %>%
  left_join(
    old_method_geocoded %>%
      select(PLAN_ID_UNIQUE,formatted_address,latitude,longitude)
    ,by="PLAN_ID_UNIQUE"
  )

notcoded.df <- notcoded.df %>%
  mutate(
    google_address = gsub(",.*","",formatted_address)
    ,google_housenum = gsub(" .*","",google_address)
    ,google_street = word(google_address,start=2,end=-1)
    ,housenum = ifelse(!is.na(google_housenum)
                       ,google_housenum
                       ,housenum
                       )
    ,street = ifelse(!is.na(google_street)
                     ,google_street
                     ,street
                     )
  )


tmp.vec <- unlist(
  lapply(1:nrow(notcoded.df), function(x){
    url <- url_create.fun(
      housenum = notcoded.df[x,"housenum"]
      ,street =  notcoded.df[x,"street"]
      ,borough =  notcoded.df[x,"borough"]
      ,zip =  notcoded.df[x,"zip"]
    )
    return(url)}
  )
)

notcoded.df[,"url"] <- tmp.vec


# Third API call using google addresses -----------------------------------

tmp.out3 <- nyc_geoclient_call.fun(notcoded.df,max_sleep.time=.1)

out.df <- bind_rows(
  out.df
  ,tmp.out3
)

notcoded.df <- bind_rows(
  address.key %>%
    anti_join(
      out.df
      ,by="url"
    )
  ,address.key %>%
    semi_join(
      out.df %>%
        filter(is.na(bbl))
    )
)

notcoded.df <- notcoded.df %>%
  left_join(
    AGdata.df %>%
      select(PLAN_ID_UNIQUE,UNITS,PLAN_DATE_EFFECTIVE,PLAN_CONSTR_TYPE)
  ) %>%
  filter(!is.na(PLAN_DATE_EFFECTIVE) & PLAN_CONSTR_TYPE %in% c("CONVERSION","REHAB"))


## write to disk for manual entry of BBLs on final pass
write.csv(notcoded.df
          ,paste(
            "/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Refresh/data/initial_notcoded_condo_coop "
            ,format(
              Sys.time()
              ,"%Y%m%d_%H%M"
            )
            ,".csv"
            ,sep=""
          )
          ,row.names=F
          ,na=""
)

## Read back in 

mancoded_bbl_secondary <- read.csv(
  "/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Refresh/data/mancoded_condo_coop_20180622.csv"
  ,stringsAsFactors=F
)



# Final join and prep -----------------------------------------------------

geocoded.df <- address.key %>%
  inner_join(
    out.df %>%
      select(url,bbl,latitude,longitude,buildingIdentificationNumber) %>%
      rename(lat = latitude
             ,lon =longitude
             ,BIN = buildingIdentificationNumber)
    ,by="url"
  ) %>%
  mutate(BBL = paste(
    str_sub(bbl,start=1,end=1) %>% as.numeric()
    ,str_sub(bbl,start=2,end=-5) %>% as.numeric()
    ,str_sub(bbl,start=-4,end=-1) %>% as.numeric()
    ,sep="_"
  )
  )

geocoded.df <- bind_rows(
  geocoded.df
  ,mancoded_bbl.df
)

AGdata.df <- bind_rows(
  AGdata.df
  ,AGdata_mancoded.df
)

geocoded.df <- geocoded.df %>%
  select(PLAN_ID_UNIQUE,BBL,lat,lon) %>%
  full_join(
    AGdata.df
    ,by="PLAN_ID_UNIQUE"
  )

## create a variable that is year in which the conversion took place  
geocoded.df <- geocoded.df %>%
  mutate(EFFECTIVE_YEAR = ifelse(!is.na(PLAN_DATE_EFFECTIVE)
                                 ,year(PLAN_DATE_EFFECTIVE)
                                 ,year(PLAN_DATE_REVIEWED) + 1
  )
  )


## put into format easily combined with pluto augmented
geocoded.df <- geocoded.df %>%
  select(BBL,EFFECTIVE_YEAR,PLAN_TYPE,UNITS,PLAN_ID_UNIQUE,PLAN_CONSTR_TYPE) %>%
  rename(ConversionYear = EFFECTIVE_YEAR
         ,ConversionType = PLAN_TYPE
         ,UnitsRes.AG = UNITS) %>%
  mutate(ConversionType = ifelse(
    ConversionType == "COOPERATIVE/CONDOMINIUM"
    ,"COOPERATIVE"
    ,ConversionType
  )
  )

## save to disk
saveRDS(geocoded.df
        ,paste(
          "/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Refresh/data/condo_coop_conversions_df "
          ,format(
            Sys.time()
            ,"%Y%m%d_%H%M"
          )
          ,".csv"
          ,sep=""
        )
        ,row.names=F
        ,na=""
)