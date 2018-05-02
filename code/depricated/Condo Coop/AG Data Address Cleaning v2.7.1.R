
library(dplyr)
library(lubridate)
library(stringr)
library(parallel)
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
pluto.files <- list.files("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/nyc_pluto_16v2",
                          pattern=".csv",
                          full.names=T)
cl <- makeCluster(5, type="FORK")
# pluto.list <- lapply(pluto.files, function(x)
pluto.list <- parLapply(cl,pluto.files, function(x){
  read.csv(x,
           stringsAsFactors=F)
}
)
stopCluster(cl)
pluto <- bind_rows(pluto.list)


pluto <- pluto %>% select(BBL,Borough,Block,Lot,ZipCode,Address,SplitZone,BldgClass,LandUse,Easements,OwnerType,
                          OwnerName,BldgArea,ComArea,ResArea,OfficeArea,RetailArea,GarageArea,StrgeArea,
                          FactryArea,OtherArea,AreaSource,NumBldgs,NumFloors,UnitsRes,UnitsTotal,YearBuilt,
                          CondoNo,XCoord,YCoord) %>% 
  mutate(BBL= paste(substr(BBL,start=1,stop=1),
                    Block,
                    Lot,
                    sep="_"),
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


zips.list.hold <- zips.list
zips.list <- lapply(pluto_boro.levs, function(x){
  restr <- pluto[,"Borough"] == x
  zips <- pluto[restr,"ZipCode"]
  zips <- trimws(as.character(na.omit(zips[!duplicated(zips)])))
  return(zips)}
  )
names(zips.list) <- c("MANHATTAN"
                      ,"BRONX"
                      ,"BROOKLYN"
                      ,"QUEENS"
                      ,"STATEN ISLAND"
)


AGdata.df <- as.data.frame(AGdata.df
                           ,stringsAsFactors=F
                           )

for(i in 1:length(zips.list)){
  target.zips <- zips.list[[i]]
  # target.boro <- names(zips.list)[i]
  restr <- which(AGdata.df[,"PLAN_BORO"]==names(zips.list)[i] &
                   !AGdata.df[,"PLAN_ZIP"] %in% target.zips)
  AGdata.df[restr,"PLAN_ZIP"] <- NA
  cat(i,"\n")
}


boro.levs <- as.character(unique(as.data.frame(
  AGdata.df
  ,stringsAsFactors=F)[,"PLAN_BORO"]
)
)

## split into groups 
AGdata.df.init <- AGdata.df

AGdata_new.df <- AGdata.df %>% 
  filter(!PLAN_CONSTR_TYPE %in% c("CONVERSION","REHAB"))
# AGdata.df <- AGdata.df %>% 
#   filter(PLAN_CONSTR_TYPE %in% c("CONVERSION","REHAB"))


AGdata_mn.df <- AGdata.df %>% 
  filter(PLAN_BORO_COUNTY %in% c("MANHATTAN","NEW YORK"))

AGdata_bk.df <- AGdata.df %>% 
  filter(PLAN_BORO_COUNTY %in% c("BROOKLYN","KINGS"))

AGdata_qn.df <- AGdata.df %>% 
  filter(PLAN_BORO_COUNTY %in% c("QUEENS"))

AGdata_bx.df <- AGdata.df %>% 
  filter(PLAN_BORO_COUNTY %in% c("BRONX"))

AGdata_si.df <- AGdata.df %>% 
  filter(PLAN_BORO %in% c("STATEN_ISLAND"))


## make address key 
ag_tmp <- AGdata_mn.df

address.key <- as.data.frame(
  cbind(
    ag_tmp[,"PLAN_STREET"]
    ,trimws(as.character(
      # gsub("*.ST/","ST",
      gsub("ST/.*","ST",
           gsub("\\(RESUBMIT*.","",
                gsub(" AKA*.",""
                     ,gsub("STREET.*","STREET",
                           gsub("\\*[^[:blank:]].*","",
                           ag_tmp[,"PLAN_STREET"]
                           )
                           )
                )
           )
      )
    ))
    # gsub("STREET.*","",ag_nozip.df[,"PLAN_STREET"])
  )
  ,stringsAsFactors=F
)

# gsub("\\(REAR)","","35(REAR) & 37(REAR) CROSBY STREET")
# gsub("//(","","35(REAR) & 37(REAR) CROSBY STREET")

address.key[,2] <- trimws(
  gsub("CONDOMINIUM","",
       gsub("([[:alnum:]][[:alnum:]] ST\\.).*","\\1"
            ,gsub(" ST   "," ST."
                  ,paste(
                    gsub("/A/K/A*.","",
                         gsub("A/K/A.*","",
                              gsub("AKA.*","",
                                   gsub("63, 65, & 67", "67",
                                        # gsub("\\*\\(CPS*.","",
                                        #      gsub("\\**CPS-9\\**","",
                                        gsub("\\(.*","",
                                             gsub("\\(REAR)","",
                                                  address.key[,2]))
                                        #      )
                                        # )
                                   )
                              )
                         )
                    )
                    ,"       "
                    ,sep=""
                  )
            )
       )
  )
)



zip.vec <- ag_tmp[,"PLAN_ZIP"]
zip.vec[is.na(zip.vec)] <- ""

address.key[,3] <- paste(trimws(address.key[,2])
                         ," "
                         ,trimws(ag_tmp[,"PLAN_BORO"])
                         ," "
                         ,trimws(ag_tmp[,"PLAN_STATE"])
                         ,", "
                         ,zip.vec
                         ,sep="")

address.key[,4] <- paste(trimws(address.key[,2])
                         # ," "
                         # ,trimws(ag_tmp[,"PLAN_BORO"])
                         ," "
                         ,trimws(ag_tmp[,"PLAN_STATE"])
                         ,", "
                         ,zip.vec
                         ,sep="")


address.key[,5] <- paste(trimws(address.key[,2])
                         ," "
                         ,trimws(ag_tmp[,"PLAN_BORO"])
                         ," "
                         ,trimws(ag_tmp[,"PLAN_STATE"])
                         # ,", "
                         # ,zip.vec
                         ,sep="")



View(address.key)

colnames(address.key) <- c("Original_Address","Corrected_Address","Address_to_Google","Address_to_Google_noboro","Address_to_Google_nozip")

# address.key.hold <- address.key
# address.key <- address.key.hold
address.key <- left_join(address.key
                             ,ag_tmp %>% 
                               filter(!duplicated(PLAN_STREET)) %>% 
                               select(PLAN_STREET,PLAN_ID_UNIQUE,AMND_NO) %>% 
                               dplyr::rename(Original_Address = PLAN_STREET)
                             ,by="Original_Address")

address.key <- address.key %>% 
  filter(!duplicated(Address_to_Google))

## sending off to google 



## geo code the addreses w/ google API 
url <- function(address, return.call = "json", sensor = "false") {
  root <- "http://maps.google.com/maps/api/geocode/"
  u <- paste(root, return.call, "?address=", address, "&sensor=", sensor, sep = "")
  return(URLencode(u))
}

geoCode <- function(address,verbose=FALSE) {
  if(verbose) cat(address,"\n")
  u <- url(address)
  doc <- getURL(u)
  x <- fromJSON(doc,simplify = FALSE)
  if(x$status=="OK") {
    lat <- x$results[[1]]$geometry$location$lat
    lng <- x$results[[1]]$geometry$location$lng
    location_type <- x$results[[1]]$geometry$location_type
    formatted_address <- x$results[[1]]$formatted_address
    zip <- x$results[[1]]$address_components[[8]]$short_name
    return(list("init_address" = address
                ,"latitude" = lat
                ,"longitude" = lng
                ,"location_type" = location_type
                ,"formatted_address" = formatted_address
                ,"zip" = zip
                ,"full_results"= x$results)
           # c(lat, lng, location_type, formatted_address)
    )
  } else {
    return(list("init_address" = address
                ,"latitude" = NA
                ,"longitude" = NA
                ,"location_type" = NA
                ,"formatted_address" = NA
                ,"zip" = NA
                ,"full_results"= NA))
  }
}


curr.boro <- unique(ag_tmp[,"PLAN_BORO"])
if(length(curr.boro)>1){
  curr.boro <- paste("boro_uknown",Sys.Date(),"_")
}


# full_result.list <- list()
# selected_results.list <- list()
# loop_len <- nrow(address.key)
start.time <- proc.time()


full_result.list <- readRDS("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/AG Data/Recoded Addresses/AG_full_google_recode_results_MANHATTAN.rds")
selected_results.list <- readRDS("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/AG Data/Recoded Addresses/AG_selected_google_recode_results_MANHATTAN_3056.rds")

loop_len <- nrow(address.key)
setwd("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/AG Data/Recoded Addresses/second try")
# new.start <- length(full_result.list) + 1
# for(i in 61:loop_len){
for(i in 2449:loop_len){
  init_address <- address.key[i,"Address_to_Google"]
  plan_id <- address.key[i,"PLAN_ID_UNIQUE"]
  cat("Starting iteration",i,"out of",loop_len,"\nOur address: ",address.key[i,"Original_Address"],"\nAddress sent to Google: ")
  
  tmp <- try(geoCode(address=as.character(address.key[i,"Address_to_Google"]),verbose=T))
  
  ## if it doesn't take, try without the zip code and without borough
  if(class(tmp)=="try-error"){
    cat("Didn't work, waiting 10 seconds and trying without zip code\n")
    Sys.sleep(10)
    tmp <- try(geoCode(address=as.character(address.key[i,"Address_to_Google_nozip"]),verbose=T))
    if(class(tmp)=="try-error"){
      cat("Still didn't work, waiting 10 seconds and trying without borough\n")
      Sys.sleep(10)
      tmp <- try(geoCode(address=as.character(address.key[i,"Address_to_Google_noboro"]),verbose=T))
      if(class(tmp)=="try-error"){
        cat("Yeah, no luck on this one. Moving right along.\n")
        selected_results.list[[i]] <- as.data.frame(cbind(init_address,NA,NA,NA,NA,plan_id)
                                                    ,stringsAsFactors=F)
        # names(selected_results.list[[i]]) <- c("init_address","latitude","longitude","formatted_address","zip","plan_id")
        full_result.list[[i]] <- list("init_addres" = init_address)
      }
    }
  }
  
  if(class(tmp)!="try-error"){
    selected_results.list[[i]] <- as.data.frame(
      cbind(
        do.call("cbind",tmp[c("init_address","latitude","longitude","formatted_address","zip")])
        ,plan_id
      )
      ,stringsAsFactors=F)
    full_result.list[[i]] <- tmp  
  }
  
  colnames(selected_results.list[[i]]) <- c("init_address","latitude","longitude","formatted_address","zip","plan_id")
  cat("Google geocoded address: ")
  cat(selected_results.list[[i]][1,"formatted_address"],"\n")
  if((i%%100==0) | (i%%loop_len==0)){
    cat("Saving backup\n")
    saveRDS(full_result.list
            ,file=paste("AG_full_google_recode_results_",curr.boro,"_",i,".rds",sep="")
    )
    saveRDS(selected_results.list
            ,file=paste("AG_selected_google_recode_results_",curr.boro,"_",i,".rds",sep="")
    )
  }
  random.sleep <- sample(15:20,1)
  if(i%%50==0){
    random.sleep <- sample(35:45,1)
  }
  cat("Finished iteration ",i,", system sleeping for",random.sleep,"seconds\n\n\n")
  Sys.sleep(random.sleep)
}

setwd("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/AG Data/Recoded Addresses/second try")



full_result.list.hold <- full_result.list
# full_result.list <- full_result.list.hold

for(i in 1:length(full_result.list)){
  match.no <- try(which(full_result.list[[i]]$init_address == address.key[,"Address_to_Google"]))
  if(class(match.no)=="try-error"){
    cat("List item not populated, matching on iteration number")
    match.no <- i
  }
  if(length(match.no)>1){
    cat("more than one match on address")
    match.no <- match.no[1]
  }
  full_result.list[[i]]$Original_Address <- address.key[match.no,"Original_Address"]
  full_result.list[[i]]$PLAN_ID_UNIQUE <- address.key[match.no,"PLAN_ID_UNIQUE"]
  cat(i,"\n")
}

geocoded.df <- bind_rows(selected_results.list)


saveRDS(full_result.list
        ,paste("AG_full_google_recode_results_",curr.boro,".rds",sep="")
        )

saveRDS(address.key
        ,paste("address_key_",curr.boro,".rds",sep="")
        )

saveRDS(geocoded.df
        ,paste("geocoded_df_",curr.boro,".rds",sep="")
        )




#######################################################################################
#######################################################################################
#######################################################################################
## Moving on to Brooklyn 
#######################################################################################
#######################################################################################
#######################################################################################



## Checking to see if I've reached the call limit
total.calls <- length(full_result.list)

elapsed_since_start <- (proc.time() - start.time)[3]

if(elapsed_since_start>86000 & total.calls>=2499){
  wait.time <- as.numeric(86400 - elapsed_since_start)
  cat("Reaching call per day limit, waiting",wait.time,"seconds\n")
  Sys.sleep(wait.time)
  total.calls <- 0
  start.time <- proc.time()
}



## make address key 
ag_tmp <- AGdata_bk.df

address.key <- as.data.frame(
  cbind(
    ag_tmp[,"PLAN_STREET"]
    ,trimws(as.character(
      gsub("ST/.*","ST",
           gsub("\\(RESUBMIT*.","",
                gsub(" AKA*.",""
                     ,gsub("STREET.*","STREET",
                           gsub("\\*[^[:blank:]].*","",
                                ag_tmp[,"PLAN_STREET"]
                           )
                     )
                )
           )
      )
    ))
  )
  ,stringsAsFactors=F
)

address.key[,2] <- trimws(
  gsub("CONDOMINIUM","",
       gsub("([[:alnum:]][[:alnum:]] ST\\.).*","\\1"
            ,gsub(" ST   "," ST."
                  ,paste(
                    gsub("/A/K/A*.","",
                         gsub("A/K/A.*","",
                              gsub("AKA.*","",
                                   gsub("63, 65, & 67", "67",
                                        gsub("\\(.*","",
                                             gsub("\\(REAR)","",
                                                  address.key[,2]))
                                        #      )
                                        # )
                                   )
                              )
                         )
                    )
                    ,"       "
                    ,sep=""
                  )
            )
       )
  )
)



zip.vec <- ag_tmp[,"PLAN_ZIP"]
zip.vec[is.na(zip.vec)] <- ""

address.key[,3] <- paste(trimws(address.key[,2])
                         ," "
                         ,trimws(ag_tmp[,"PLAN_BORO"])
                         ," "
                         ,trimws(ag_tmp[,"PLAN_STATE"])
                         ,", "
                         ,zip.vec
                         ,sep="")

address.key[,4] <- paste(trimws(address.key[,2])
                         ," "
                         ,trimws(ag_tmp[,"PLAN_STATE"])
                         ,", "
                         ,zip.vec
                         ,sep="")


address.key[,5] <- paste(trimws(address.key[,2])
                         ," "
                         ,trimws(ag_tmp[,"PLAN_BORO"])
                         ," "
                         ,trimws(ag_tmp[,"PLAN_STATE"])
                         ,sep="")





colnames(address.key) <- c("Original_Address","Corrected_Address","Address_to_Google","Address_to_Google_noboro","Address_to_Google_nozip")

# address.key.hold <- address.key
# address.key <- address.key.hold
address.key <- left_join(address.key
                         ,ag_tmp %>% 
                           filter(!duplicated(PLAN_STREET)) %>% 
                           select(PLAN_STREET,PLAN_ID_UNIQUE,AMND_NO) %>% 
                           dplyr::rename(Original_Address = PLAN_STREET)
                         ,by="Original_Address")

address.key <- address.key %>% 
  filter(!duplicated(Address_to_Google))


curr.boro <- unique(ag_tmp[,"PLAN_BORO"])
if(length(curr.boro)>1){
  curr.boro <- paste("boro_uknown",Sys.Date(),"_")
}


# full_result.list <- list()
# selected_results.list <- list()
full_result.list <- readRDS("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/AG Data/Recoded Addresses/AG_full_google_recode_results_BROOKLYN_850.rds")
selected_results.list <- readRDS("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/AG Data/Recoded Addresses/AG_selected_google_recode_results_BROOKLYN_850.rds")
address.key <- read.csv("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/AG Data/Recoded Addresses/address_key_BROOKLYN.csv",
                        stringsAsFactors=F)

# loop_len <- nrow(address.key)
loop_len <- 1437
setwd("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/AG Data/Recoded Addresses")
start.time <- proc.time()
for(i in 851:loop_len){
  init_address <- address.key[i,"Address_to_Google"]
  plan_id <- address.key[i,"PLAN_ID_UNIQUE"]
  cat("Starting iteration",i,"out of",loop_len,"\nOur address: ",address.key[i,"Original_Address"],"\nAddress sent to Google: ")
  
  tmp <- try(geoCode(address=as.character(address.key[i,"Address_to_Google"]),verbose=T))
  
  ## if it doesn't take, try without the zip code and without borough
  if(class(tmp)=="try-error"){
    cat("Didn't work, waiting 10 seconds and trying without zip code\n")
    Sys.sleep(5)
    tmp <- try(geoCode(address=as.character(address.key[i,"Address_to_Google_nozip"]),verbose=T))
    if(class(tmp)=="try-error"){
      cat("Still didn't work, waiting 10 seconds and trying without borough\n")
      Sys.sleep(5)
      tmp <- try(geoCode(address=as.character(address.key[i,"Address_to_Google_noboro"]),verbose=T))
      if(class(tmp)=="try-error"){
        cat("Yeah, no luck on this one. Moving right along.\n")
        selected_results.list[[i]] <- as.data.frame(cbind(init_address,NA,NA,NA,NA,plan_id)
                                                    ,stringsAsFactors=F)
        # names(selected_results.list[[i]]) <- c("init_address","latitude","longitude","formatted_address","zip","plan_id")
        full_result.list[[i]] <- list("init_addres" = init_address)
      }
    }
  }
  
  if(class(tmp)!="try-error"){
    selected_results.list[[i]] <- as.data.frame(
      cbind(
        do.call("cbind",tmp[c("init_address","latitude","longitude","formatted_address","zip")])
        ,plan_id
      )
      ,stringsAsFactors=F)
    full_result.list[[i]] <- tmp  
  }
  
  colnames(selected_results.list[[i]]) <- c("init_address","latitude","longitude","formatted_address","zip","plan_id")
  cat("Google geocoded address: ")
  cat(selected_results.list[[i]][1,"formatted_address"],"\n")
  if((i%%100==0) | (i%%loop_len==0)){
    cat("Saving backup\n")
    saveRDS(full_result.list
            ,file=paste("AG_full_google_recode_results_",curr.boro,"_",i,".rds",sep="")
    )
    saveRDS(selected_results.list
            ,file=paste("AG_selected_google_recode_results_",curr.boro,"_",i,".rds",sep="")
    )
  }
  random.sleep <- sample(5:10,1)
  if(i%%50==0){
    random.sleep <- sample(10:15,1)
  }
  cat("Finished iteration ",i,", system sleeping for",random.sleep,"seconds\n\n\n")
  Sys.sleep(random.sleep)
}

setwd("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/AG Data/Recoded Addresses")



full_result.list.hold <- full_result.list
# full_result.list <- full_result.list.hold

for(i in 1:length(full_result.list)){
  match.no <- try(which(full_result.list[[i]]$init_address == address.key[,"Address_to_Google"]))
  if(class(match.no)=="try-error"){
    cat("List item not populated, matching on iteration number")
    match.no <- i
  }
  if(length(match.no)>1){
    cat("more than one match on address")
    match.no <- match.no[1]
  }
  full_result.list[[i]]$Original_Address <- address.key[match.no,"Original_Address"]
  full_result.list[[i]]$PLAN_ID_UNIQUE <- address.key[match.no,"PLAN_ID_UNIQUE"]
  cat(i,"\n")
}

geocoded.df <- bind_rows(selected_results.list)


saveRDS(full_result.list
        ,paste("AG_full_google_recode_results_",curr.boro,".rds",sep="")
)

saveRDS(selected_results.list
        ,paste("address_key_",curr.boro,".rds",sep="")
)

saveRDS(geocoded.df
        ,paste("geocoded_df_",curr.boro,".rds",sep="")
)



#######################################################################################
#######################################################################################
#######################################################################################
##
##  Queens
##
#######################################################################################
#######################################################################################
#######################################################################################

## Checking to see if I've reached the call limit
total.calls <- total.calls + length(full_result.list)

elapsed_since_start <- (proc.time() - start.time)[3]

if(elapsed_since_start>86000 & total.calls>=2499){
  wait.time <- as.numeric(86400 - elapsed_since_start)
  cat("Reaching call per day limit, waiting",wait.time,"seconds\n")
  Sys.sleep(wait.time)
  total.calls <- 0
  start.time <- proc.time()
}



## make address key 
ag_tmp <- AGdata_qn.df

address.key <- as.data.frame(
  cbind(
    ag_tmp[,"PLAN_STREET"]
    ,trimws(as.character(
      gsub("ST/.*","ST",
           gsub("\\(RESUBMIT*.","",
                gsub(" AKA*.",""
                     ,gsub("STREET.*","STREET",
                           gsub("\\*[^[:blank:]].*","",
                                ag_tmp[,"PLAN_STREET"]
                           )
                     )
                )
           )
      )
    ))
  )
  ,stringsAsFactors=F
)

address.key[,2] <- trimws(
  gsub("CONDOMINIUM","",
       gsub("([[:alnum:]][[:alnum:]] ST\\.).*","\\1"
            ,gsub(" ST   "," ST."
                  ,paste(
                    gsub("/A/K/A*.","",
                         gsub("A/K/A.*","",
                              gsub("AKA.*","",
                                   gsub("63, 65, & 67", "67",
                                        gsub("\\(.*","",
                                             gsub("\\(REAR)","",
                                                  address.key[,2]))
                                        #      )
                                        # )
                                   )
                              )
                         )
                    )
                    ,"       "
                    ,sep=""
                  )
            )
       )
  )
)



zip.vec <- ag_tmp[,"PLAN_ZIP"]
zip.vec[is.na(zip.vec)] <- ""

address.key[,3] <- paste(trimws(address.key[,2])
                         ," "
                         ,trimws(ag_tmp[,"PLAN_BORO"])
                         ," "
                         ,trimws(ag_tmp[,"PLAN_STATE"])
                         ,", "
                         ,zip.vec
                         ,sep="")

address.key[,4] <- paste(trimws(address.key[,2])
                         ," "
                         ,trimws(ag_tmp[,"PLAN_STATE"])
                         ,", "
                         ,zip.vec
                         ,sep="")


address.key[,5] <- paste(trimws(address.key[,2])
                         ," "
                         ,trimws(ag_tmp[,"PLAN_BORO"])
                         ," "
                         ,trimws(ag_tmp[,"PLAN_STATE"])
                         ,sep="")





colnames(address.key) <- c("Original_Address","Corrected_Address","Address_to_Google","Address_to_Google_noboro","Address_to_Google_nozip")

# address.key.hold <- address.key
# address.key <- address.key.hold
address.key <- left_join(address.key
                         ,ag_tmp %>% 
                           filter(!duplicated(PLAN_STREET)) %>% 
                           select(PLAN_STREET,PLAN_ID_UNIQUE,AMND_NO) %>% 
                           dplyr::rename(Original_Address = PLAN_STREET)
                         ,by="Original_Address")

address.key <- address.key %>% 
  filter(!duplicated(Address_to_Google))


curr.boro <- unique(ag_tmp[,"PLAN_BORO"])
if(length(curr.boro)>1){
  curr.boro <- paste("boro_uknown",Sys.Date(),"_")
}


full_result.list <- list()
selected_results.list <- list()
loop_len <- nrow(address.key)
# loop_len <- 60
setwd("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/AG Data/Recoded Addresses")
start.time <- proc.time()
for(i in 1:loop_len){
  init_address <- address.key[i,"Address_to_Google"]
  plan_id <- address.key[i,"PLAN_ID_UNIQUE"]
  cat("Starting iteration",i,"out of",loop_len,"\nOur address: ",address.key[i,"Original_Address"],"\nAddress sent to Google: ")
  
  tmp <- try(geoCode(address=as.character(address.key[i,"Address_to_Google"]),verbose=T))
  
  ## if it doesn't take, try without the zip code and without borough
  if(class(tmp)=="try-error"){
    cat("Didn't work, waiting 10 seconds and trying without zip code\n")
    Sys.sleep(10)
    tmp <- try(geoCode(address=as.character(address.key[i,"Address_to_Google_nozip"]),verbose=T))
    if(class(tmp)=="try-error"){
      cat("Still didn't work, waiting 10 seconds and trying without borough\n")
      Sys.sleep(10)
      tmp <- try(geoCode(address=as.character(address.key[i,"Address_to_Google_noboro"]),verbose=T))
      if(class(tmp)=="try-error"){
        cat("Yeah, no luck on this one. Moving right along.\n")
        selected_results.list[[i]] <- as.data.frame(cbind(init_address,NA,NA,NA,NA,plan_id)
                                                    ,stringsAsFactors=F)
        # names(selected_results.list[[i]]) <- c("init_address","latitude","longitude","formatted_address","zip","plan_id")
        full_result.list[[i]] <- list("init_addres" = init_address)
      }
    }
  }
  
  if(class(tmp)!="try-error"){
    selected_results.list[[i]] <- as.data.frame(
      cbind(
        do.call("cbind",tmp[c("init_address","latitude","longitude","formatted_address","zip")])
        ,plan_id
      )
      ,stringsAsFactors=F)
    full_result.list[[i]] <- tmp  
  }
  
  colnames(selected_results.list[[i]]) <- c("init_address","latitude","longitude","formatted_address","zip","plan_id")
  cat("Google geocoded address: ")
  cat(selected_results.list[[i]][1,"formatted_address"],"\n")
  if((i%%100==0) | (i%%loop_len==0)){
    cat("Saving backup\n")
    saveRDS(full_result.list
            ,file=paste("AG_full_google_recode_results_",curr.boro,"_",i,".rds",sep="")
    )
    saveRDS(selected_results.list
            ,file=paste("AG_selected_google_recode_results_",curr.boro,"_",i,".rds",sep="")
    )
  }
  random.sleep <- sample(15:20,1)
  if(i%%50==0){
    random.sleep <- sample(35:45,1)
  }
  cat("Finished iteration ",i,", system sleeping for",random.sleep,"seconds\n\n\n")
  Sys.sleep(random.sleep)
}

setwd("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/AG Data/Recoded Addresses")



full_result.list.hold <- full_result.list
# full_result.list <- full_result.list.hold

for(i in 1:length(full_result.list)){
  match.no <- try(which(full_result.list[[i]]$init_address == address.key[,"Address_to_Google"]))
  if(class(match.no)=="try-error"){
    cat("List item not populated, matching on iteration number")
    match.no <- i
  }
  if(length(match.no)>1){
    cat("more than one match on address")
    match.no <- match.no[1]
  }
  full_result.list[[i]]$Original_Address <- address.key[match.no,"Original_Address"]
  full_result.list[[i]]$PLAN_ID_UNIQUE <- address.key[match.no,"PLAN_ID_UNIQUE"]
  cat(i,"\n")
}

geocoded.df <- bind_rows(selected_results.list)


saveRDS(full_result.list
        ,paste("AG_full_google_recode_results_",curr.boro,".rds",sep="")
)

saveRDS(selected_results.list
        ,paste("address_key_",curr.boro,".rds",sep="")
)

saveRDS(geocoded.df
        ,paste("geocoded_df_",curr.boro,".rds",sep="")
)



#######################################################################################
#######################################################################################
#######################################################################################
##
##  Bronx
##
#######################################################################################
#######################################################################################
#######################################################################################

## Checking to see if I've reached the call limit
total.calls <- total.calls + length(full_result.list)

elapsed_since_start <- (proc.time() - start.time)[3]

if(elapsed_since_start>86000 & total.calls>=2499){
  wait.time <- as.numeric(86400 - elapsed_since_start)
  cat("Reaching call per day limit, waiting",wait.time,"seconds\n")
  Sys.sleep(wait.time)
  total.calls <- 0
  start.time <- proc.time()
}


## make address key 
ag_tmp <- AGdata_bx.df

address.key <- as.data.frame(
  cbind(
    ag_tmp[,"PLAN_STREET"]
    ,trimws(as.character(
      gsub("ST/.*","ST",
           gsub("\\(RESUBMIT*.","",
                gsub(" AKA*.",""
                     ,gsub("STREET.*","STREET",
                           gsub("\\*[^[:blank:]].*","",
                                ag_tmp[,"PLAN_STREET"]
                           )
                     )
                )
           )
      )
    ))
  )
  ,stringsAsFactors=F
)

address.key[,2] <- trimws(
  gsub("CONDOMINIUM","",
       gsub("([[:alnum:]][[:alnum:]] ST\\.).*","\\1"
            ,gsub(" ST   "," ST."
                  ,paste(
                    gsub("/A/K/A*.","",
                         gsub("A/K/A.*","",
                              gsub("AKA.*","",
                                   gsub("63, 65, & 67", "67",
                                        gsub("\\(.*","",
                                             gsub("\\(REAR)","",
                                                  address.key[,2]))
                                        #      )
                                        # )
                                   )
                              )
                         )
                    )
                    ,"       "
                    ,sep=""
                  )
            )
       )
  )
)



zip.vec <- ag_tmp[,"PLAN_ZIP"]
zip.vec[is.na(zip.vec)] <- ""

address.key[,3] <- paste(trimws(address.key[,2])
                         ," "
                         ,trimws(ag_tmp[,"PLAN_BORO"])
                         ," "
                         ,trimws(ag_tmp[,"PLAN_STATE"])
                         ,", "
                         ,zip.vec
                         ,sep="")

address.key[,4] <- paste(trimws(address.key[,2])
                         ," "
                         ,trimws(ag_tmp[,"PLAN_STATE"])
                         ,", "
                         ,zip.vec
                         ,sep="")


address.key[,5] <- paste(trimws(address.key[,2])
                         ," "
                         ,trimws(ag_tmp[,"PLAN_BORO"])
                         ," "
                         ,trimws(ag_tmp[,"PLAN_STATE"])
                         ,sep="")





colnames(address.key) <- c("Original_Address","Corrected_Address","Address_to_Google","Address_to_Google_noboro","Address_to_Google_nozip")

# address.key.hold <- address.key
# address.key <- address.key.hold
address.key <- left_join(address.key
                         ,ag_tmp %>% 
                           filter(!duplicated(PLAN_STREET)) %>% 
                           select(PLAN_STREET,PLAN_ID_UNIQUE,AMND_NO) %>% 
                           dplyr::rename(Original_Address = PLAN_STREET)
                         ,by="Original_Address")

address.key <- address.key %>% 
  filter(!duplicated(Address_to_Google))


curr.boro <- unique(ag_tmp[,"PLAN_BORO"])
if(length(curr.boro)>1){
  curr.boro <- paste("boro_uknown",Sys.Date(),"_")
}


full_result.list <- list()
selected_results.list <- list()
loop_len <- nrow(address.key)
# loop_len <- 60
setwd("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/AG Data/Recoded Addresses")
start.time <- proc.time()
for(i in 1:loop_len){
  init_address <- address.key[i,"Address_to_Google"]
  plan_id <- address.key[i,"PLAN_ID_UNIQUE"]
  cat("Starting iteration",i,"out of",loop_len,"\nOur address: ",address.key[i,"Original_Address"],"\nAddress sent to Google: ")
  
  tmp <- try(geoCode(address=as.character(address.key[i,"Address_to_Google"]),verbose=T))
  
  ## if it doesn't take, try without the zip code and without borough
  if(class(tmp)=="try-error"){
    cat("Didn't work, waiting 10 seconds and trying without zip code\n")
    Sys.sleep(10)
    tmp <- try(geoCode(address=as.character(address.key[i,"Address_to_Google_nozip"]),verbose=T))
    if(class(tmp)=="try-error"){
      cat("Still didn't work, waiting 10 seconds and trying without borough\n")
      Sys.sleep(10)
      tmp <- try(geoCode(address=as.character(address.key[i,"Address_to_Google_noboro"]),verbose=T))
      if(class(tmp)=="try-error"){
        cat("Yeah, no luck on this one. Moving right along.\n")
        selected_results.list[[i]] <- as.data.frame(cbind(init_address,NA,NA,NA,NA,plan_id)
                                                    ,stringsAsFactors=F)
        # names(selected_results.list[[i]]) <- c("init_address","latitude","longitude","formatted_address","zip","plan_id")
        full_result.list[[i]] <- list("init_addres" = init_address)
      }
    }
  }
  
  if(class(tmp)!="try-error"){
    selected_results.list[[i]] <- as.data.frame(
      cbind(
        do.call("cbind",tmp[c("init_address","latitude","longitude","formatted_address","zip")])
        ,plan_id
      )
      ,stringsAsFactors=F)
    full_result.list[[i]] <- tmp  
  }
  
  colnames(selected_results.list[[i]]) <- c("init_address","latitude","longitude","formatted_address","zip","plan_id")
  cat("Google geocoded address: ")
  cat(selected_results.list[[i]][1,"formatted_address"],"\n")
  if((i%%100==0) | (i%%loop_len==0)){
    cat("Saving backup\n")
    saveRDS(full_result.list
            ,file=paste("AG_full_google_recode_results_",curr.boro,"_",i,".rds",sep="")
    )
    saveRDS(selected_results.list
            ,file=paste("AG_selected_google_recode_results_",curr.boro,"_",i,".rds",sep="")
    )
  }
  random.sleep <- sample(15:20,1)
  if(i%%50==0){
    random.sleep <- sample(35:45,1)
  }
  cat("Finished iteration ",i,", system sleeping for",random.sleep,"seconds\n\n\n")
  Sys.sleep(random.sleep)
}

setwd("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/AG Data/Recoded Addresses")



full_result.list.hold <- full_result.list
# full_result.list <- full_result.list.hold

for(i in 1:length(full_result.list)){
  match.no <- try(which(full_result.list[[i]]$init_address == address.key[,"Address_to_Google"]))
  if(class(match.no)=="try-error"){
    cat("List item not populated, matching on iteration number")
    match.no <- i
  }
  if(length(match.no)>1){
    cat("more than one match on address")
    match.no <- match.no[1]
  }
  full_result.list[[i]]$Original_Address <- address.key[match.no,"Original_Address"]
  full_result.list[[i]]$PLAN_ID_UNIQUE <- address.key[match.no,"PLAN_ID_UNIQUE"]
  cat(i,"\n")
}

geocoded.df <- bind_rows(selected_results.list)


saveRDS(full_result.list
        ,paste("AG_full_google_recode_results_",curr.boro,".rds",sep="")
)

saveRDS(selected_results.list
        ,paste("address_key_",curr.boro,".rds",sep="")
)

saveRDS(geocoded.df
        ,paste("geocoded_df_",curr.boro,".rds",sep="")
)





#######################################################################################
#######################################################################################
#######################################################################################
##
##  Staten Island
##
#######################################################################################
#######################################################################################
#######################################################################################

## Checking to see if I've reached the call limit
total.calls <- total.calls + length(full_result.list)

elapsed_since_start <- (proc.time() - start.time)[3]

if(elapsed_since_start>86000 & total.calls>=2499){
  wait.time <- as.numeric(86400 - elapsed_since_start)
  cat("Reaching call per day limit, waiting",wait.time,"seconds\n")
  Sys.sleep(wait.time)
  total.calls <- 0
  start.time <- proc.time()
}



## make address key 
ag_tmp <- AGdata_si.df

address.key <- as.data.frame(
  cbind(
    ag_tmp[,"PLAN_STREET"]
    ,trimws(as.character(
      gsub("ST/.*","ST",
           gsub("\\(RESUBMIT*.","",
                gsub(" AKA*.",""
                     ,gsub("STREET.*","STREET",
                           gsub("\\*[^[:blank:]].*","",
                                ag_tmp[,"PLAN_STREET"]
                           )
                     )
                )
           )
      )
    ))
  )
  ,stringsAsFactors=F
)

address.key[,2] <- trimws(
  gsub("CONDOMINIUM","",
       gsub("([[:alnum:]][[:alnum:]] ST\\.).*","\\1"
            ,gsub(" ST   "," ST."
                  ,paste(
                    gsub("/A/K/A*.","",
                         gsub("A/K/A.*","",
                              gsub("AKA.*","",
                                   gsub("63, 65, & 67", "67",
                                        gsub("\\(.*","",
                                             gsub("\\(REAR)","",
                                                  address.key[,2]))
                                        #      )
                                        # )
                                   )
                              )
                         )
                    )
                    ,"       "
                    ,sep=""
                  )
            )
       )
  )
)



zip.vec <- ag_tmp[,"PLAN_ZIP"]
zip.vec[is.na(zip.vec)] <- ""

address.key[,3] <- paste(trimws(address.key[,2])
                         ," "
                         ,trimws(ag_tmp[,"PLAN_BORO"])
                         ," "
                         ,trimws(ag_tmp[,"PLAN_STATE"])
                         ,", "
                         ,zip.vec
                         ,sep="")

address.key[,4] <- paste(trimws(address.key[,2])
                         ," "
                         ,trimws(ag_tmp[,"PLAN_STATE"])
                         ,", "
                         ,zip.vec
                         ,sep="")


address.key[,5] <- paste(trimws(address.key[,2])
                         ," "
                         ,trimws(ag_tmp[,"PLAN_BORO"])
                         ," "
                         ,trimws(ag_tmp[,"PLAN_STATE"])
                         ,sep="")





colnames(address.key) <- c("Original_Address","Corrected_Address","Address_to_Google","Address_to_Google_noboro","Address_to_Google_nozip")

# address.key.hold <- address.key
# address.key <- address.key.hold
address.key <- left_join(address.key
                         ,ag_tmp %>% 
                           filter(!duplicated(PLAN_STREET)) %>% 
                           select(PLAN_STREET,PLAN_ID_UNIQUE,AMND_NO) %>% 
                           dplyr::rename(Original_Address = PLAN_STREET)
                         ,by="Original_Address")

address.key <- address.key %>% 
  filter(!duplicated(Address_to_Google))


curr.boro <- unique(ag_tmp[,"PLAN_BORO"])
if(length(curr.boro)>1){
  curr.boro <- paste("boro_uknown",Sys.Date(),"_")
}


full_result.list <- list()
selected_results.list <- list()
loop_len <- nrow(address.key)
# loop_len <- 60
setwd("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/AG Data/Recoded Addresses")
start.time <- proc.time()
for(i in 1:loop_len){
  init_address <- address.key[i,"Address_to_Google"]
  plan_id <- address.key[i,"PLAN_ID_UNIQUE"]
  cat("Starting iteration",i,"out of",loop_len,"\nOur address: ",address.key[i,"Original_Address"],"\nAddress sent to Google: ")
  
  tmp <- try(geoCode(address=as.character(address.key[i,"Address_to_Google"]),verbose=T))
  
  ## if it doesn't take, try without the zip code and without borough
  if(class(tmp)=="try-error"){
    cat("Didn't work, waiting 10 seconds and trying without zip code\n")
    Sys.sleep(5)
    tmp <- try(geoCode(address=as.character(address.key[i,"Address_to_Google_nozip"]),verbose=T))
    if(class(tmp)=="try-error"){
      cat("Still didn't work, waiting 10 seconds and trying without borough\n")
      Sys.sleep(5)
      tmp <- try(geoCode(address=as.character(address.key[i,"Address_to_Google_noboro"]),verbose=T))
      if(class(tmp)=="try-error"){
        cat("Yeah, no luck on this one. Moving right along.\n")
        selected_results.list[[i]] <- as.data.frame(cbind(init_address,NA,NA,NA,NA,plan_id)
                                                    ,stringsAsFactors=F)
        # names(selected_results.list[[i]]) <- c("init_address","latitude","longitude","formatted_address","zip","plan_id")
        full_result.list[[i]] <- list("init_addres" = init_address)
      }
    }
  }
  
  if(class(tmp)!="try-error"){
    selected_results.list[[i]] <- as.data.frame(
      cbind(
        do.call("cbind",tmp[c("init_address","latitude","longitude","formatted_address","zip")])
        ,plan_id
      )
      ,stringsAsFactors=F)
    full_result.list[[i]] <- tmp  
  }
  
  colnames(selected_results.list[[i]]) <- c("init_address","latitude","longitude","formatted_address","zip","plan_id")
  cat("Google geocoded address: ")
  cat(selected_results.list[[i]][1,"formatted_address"],"\n")
  if((i%%100==0) | (i%%loop_len==0)){
    cat("Saving backup\n")
    saveRDS(full_result.list
            ,file=paste("AG_full_google_recode_results_",curr.boro,"_",i,".rds",sep="")
    )
    saveRDS(selected_results.list
            ,file=paste("AG_selected_google_recode_results_",curr.boro,"_",i,".rds",sep="")
    )
  }
  random.sleep <- sample(seq(from=5,to=10,by=.01),1)
  if(i%%15==0){
    random.sleep <- sample(seq(from=15,to=20,by=.01),1)
  }
  cat("Finished iteration ",i,", system sleeping for",random.sleep,"seconds\n\n\n")
  Sys.sleep(random.sleep)
}

setwd("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/AG Data/Recoded Addresses")



full_result.list.hold <- full_result.list
# full_result.list <- full_result.list.hold

for(i in 1:length(full_result.list)){
  match.no <- try(which(full_result.list[[i]]$init_address == address.key[,"Address_to_Google"]))
  if(class(match.no)=="try-error"){
    cat("List item not populated, matching on iteration number")
    match.no <- i
  }
  if(length(match.no)>1){
    cat("more than one match on address")
    match.no <- match.no[1]
  }
  full_result.list[[i]]$Original_Address <- address.key[match.no,"Original_Address"]
  full_result.list[[i]]$PLAN_ID_UNIQUE <- address.key[match.no,"PLAN_ID_UNIQUE"]
  cat(i,"\n")
}

geocoded.df <- bind_rows(selected_results.list)


saveRDS(full_result.list
        ,paste("AG_full_google_recode_results_",curr.boro,".rds",sep="")
)

saveRDS(selected_results.list
        ,paste("address_key_",curr.boro,".rds",sep="")
)

saveRDS(geocoded.df
        ,paste("geocoded_df_",curr.boro,".rds",sep="")
)



############################################################
############################################################
## second pass 
############################################################
############################################################

## make address key 
ag_tmp <- readRDS("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/AG Data/misses_second_pass_AGdata_df.rds") %>% 
  rename(PLAN_ID_UNIQUE=plan_id)

address.key <- as.data.frame(
  cbind(
    ag_tmp[,"PLAN_STREET"]
    ,trimws(as.character(
      gsub("ST/.*","ST",
           gsub("\\(RESUBMIT*.","",
                gsub(" AKA*.",""
                     ,gsub("STREET.*","STREET",
                           gsub("\\*[^[:blank:]].*","",
                                ag_tmp[,"PLAN_STREET"]
                           )
                     )
                )
           )
      )
    ))
  )
  ,stringsAsFactors=F
)

address.key[,2] <- trimws(
  gsub("CONDOMINIUM","",
       gsub("([[:alnum:]][[:alnum:]] ST\\.).*","\\1"
            ,gsub(" ST   "," ST."
                  ,paste(
                    gsub("/A/K/A*.","",
                         gsub("A/K/A.*","",
                              gsub("AKA.*","",
                                   gsub("63, 65, & 67", "67",
                                        gsub("\\(.*","",
                                             gsub("\\(REAR)","",
                                                  address.key[,2]))
                                        #      )
                                        # )
                                   )
                              )
                         )
                    )
                    ,"       "
                    ,sep=""
                  )
            )
       )
  )
)



zip.vec <- ag_tmp[,"zip"]
zip.vec[is.na(zip.vec)] <- ""

address.key[,3] <- paste(trimws(address.key[,2])
                         ," "
                         ,trimws(ag_tmp[,"PLAN_BORO"])
                         ," "
                         ,trimws(ag_tmp[,"PLAN_STATE"])
                         ,", "
                         ,zip.vec
                         ,sep="")

address.key[,4] <- paste(trimws(address.key[,2])
                         ," "
                         ,trimws(ag_tmp[,"PLAN_STATE"])
                         ,", "
                         ,zip.vec
                         ,sep="")


address.key[,5] <- paste(trimws(address.key[,2])
                         ," "
                         ,trimws(ag_tmp[,"PLAN_BORO"])
                         ," "
                         ,trimws(ag_tmp[,"PLAN_STATE"])
                         ,sep="")
colnames(address.key) <- c("Original_Address","Corrected_Address","Address_to_Google","Address_to_Google_noboro","Address_to_Google_nozip")

# address.key.hold <- address.key
# address.key <- address.key.hold
address.key <- left_join(address.key
                         ,ag_tmp %>% 
                           filter(!duplicated(PLAN_STREET)) %>% 
                           select(PLAN_STREET,PLAN_ID_UNIQUE) %>% 
                           dplyr::rename(Original_Address = PLAN_STREET)
                         ,by="Original_Address")

address.key <- address.key %>% 
  filter(!duplicated(Address_to_Google))
  # summarize(sum(duplicated(Address_to_Google)))

curr.boro <- unique(ag_tmp[,"PLAN_BORO"])
if(length(curr.boro)>1){
  curr.boro <- paste("second_pass",Sys.Date(),sep="_")
}


full_result.list <- list()
selected_results.list <- list()
loop_len <- nrow(address.key)
# loop_len <- 60
setwd("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/AG Data/Recoded Addresses")
start.time <- proc.time()
for(i in 1:loop_len){
  init_address <- address.key[i,"Address_to_Google"]
  plan_id <- address.key[i,"PLAN_ID_UNIQUE"]
  cat("Starting iteration",i,"out of",loop_len,"\nOur address: ",address.key[i,"Original_Address"],"\nAddress sent to Google: ")
  
  tmp <- try(geoCode(address=as.character(address.key[i,"Address_to_Google"]),verbose=T))
  
  ## if it doesn't take, try without the zip code and without borough
  if(class(tmp)=="try-error"){
    cat("Didn't work, waiting 10 seconds and trying without zip code\n")
    Sys.sleep(5)
    tmp <- try(geoCode(address=as.character(address.key[i,"Address_to_Google_nozip"]),verbose=T))
    if(class(tmp)=="try-error"){
      cat("Still didn't work, waiting 10 seconds and trying without borough\n")
      Sys.sleep(5)
      tmp <- try(geoCode(address=as.character(address.key[i,"Address_to_Google_noboro"]),verbose=T))
      if(class(tmp)=="try-error"){
        cat("Yeah, no luck on this one. Moving right along.\n")
        selected_results.list[[i]] <- as.data.frame(cbind(init_address,NA,NA,NA,NA,plan_id)
                                                    ,stringsAsFactors=F)
        # names(selected_results.list[[i]]) <- c("init_address","latitude","longitude","formatted_address","zip","plan_id")
        full_result.list[[i]] <- list("init_addres" = init_address)
      }
    }
  }
  
  if(class(tmp)!="try-error"){
    selected_results.list[[i]] <- as.data.frame(
      cbind(
        do.call("cbind",tmp[c("init_address","latitude","longitude","formatted_address","zip")])
        ,plan_id
      )
      ,stringsAsFactors=F)
    full_result.list[[i]] <- tmp  
  }
  
  colnames(selected_results.list[[i]]) <- c("init_address","latitude","longitude","formatted_address","zip","plan_id")
  cat("Google geocoded address: ")
  cat(selected_results.list[[i]][1,"formatted_address"],"\n")
  if((i%%100==0) | (i%%loop_len==0)){
    cat("Saving backup\n")
    saveRDS(full_result.list
            ,file=paste("AG_full_google_recode_results_",curr.boro,"_",i,".rds",sep="")
    )
    saveRDS(selected_results.list
            ,file=paste("AG_selected_google_recode_results_",curr.boro,"_",i,".rds",sep="")
    )
  }
  random.sleep <- sample(seq(from=5,to=7,by=.01),1)
  if(i%%15==0){
    random.sleep <- sample(seq(from=8,to=10,by=.01),1)
  }
  cat("Finished iteration ",i,", system sleeping for",random.sleep,"seconds\n\n\n")
  Sys.sleep(random.sleep)
}

geocoded.df <- bind_rows(selected_results.list)

tmp.out <- left_join(
  geocoded.df
  ,AGdata.df %>% 
    select(PLAN_STREET,PLAN_NAME,PLAN_ZIP_OLD,PLAN_ID_UNIQUE,PLAN_BORO,PLAN_STATE,PLAN_DATE_EFFECTIVE) %>% 
    rename(plan_id = PLAN_ID_UNIQUE)
  ,by="plan_id"
)

third_pass_coded <- tmp.out %>% 
  filter(!is.na(formatted_address)
  )

third_pass_misses <- tmp.out %>% 
  filter(!is.na(PLAN_DATE_EFFECTIVE) & 
           (is.na(init_address) | is.na(formatted_address))
  ) %>% 
  select(PLAN_STREET,PLAN_NAME,PLAN_ZIP_OLD,plan_id,PLAN_BORO,PLAN_STATE,PLAN_DATE_EFFECTIVE,init_address,latitude,longitude,formatted_address,zip)


setwd("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/AG Data")
write.csv(third_pass_misses,"second_pass_misses.csv",row.names=F)
write.csv(third_pass_coded,"second_pass_coded.csv",row.names=F)





## trying a third time 
third_pass_manual <- read.csv("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/AG Data/third pass manual update.csv"
                              ,stringsAsFactors=F
)

ag_tmp <- third_pass_manual


address.key <- as.data.frame(
  cbind(
    ag_tmp[,"PLAN_STREET"]
    ,trimws(as.character(
      gsub("ST/.*","ST",
           gsub("\\(RESUBMIT*.","",
                gsub(" AKA*.",""
                     ,gsub("STREET.*","STREET",
                           gsub("\\*[^[:blank:]].*","",
                                ag_tmp[,"PLAN_STREET"]
                           )
                     )
                )
           )
      )
    ))
  )
  ,stringsAsFactors=F
)

address.key[,2] <- trimws(
  gsub("CONDOMINIUM","",
       gsub("([[:alnum:]][[:alnum:]] ST\\.).*","\\1"
            ,gsub(" ST   "," ST."
                  ,paste(
                    gsub("/A/K/A*.","",
                         gsub("A/K/A.*","",
                              gsub("AKA.*","",
                                   gsub("63, 65, & 67", "67",
                                        gsub("\\(.*","",
                                             gsub("\\(REAR)","",
                                                  address.key[,2]))
                                        #      )
                                        # )
                                   )
                              )
                         )
                    )
                    ,"       "
                    ,sep=""
                  )
            )
       )
  )
)

address.key <- as.data.frame(
  cbind(ag_tmp[,"PLAN_STREET"]
        ,ag_tmp[,"PLAN_STREET"]
  )
  ,stringsAsFactors=F
)

zip.vec <- ag_tmp[,"PLAN_ZIP_OLD"]
zip.vec[is.na(zip.vec)] <- ""

address.key[,3] <- paste(trimws(address.key[,2])
                         ," "
                         ,trimws(ag_tmp[,"PLAN_BORO"])
                         ," "
                         ,trimws(ag_tmp[,"PLAN_STATE"])
                         ,", "
                         ,zip.vec
                         ,sep="")

address.key[,4] <- paste(trimws(address.key[,2])
                         ," "
                         ,trimws(ag_tmp[,"PLAN_STATE"])
                         ,", "
                         ,zip.vec
                         ,sep="")


address.key[,5] <- paste(trimws(address.key[,2])
                         ," "
                         ,trimws(ag_tmp[,"PLAN_BORO"])
                         ," "
                         ,trimws(ag_tmp[,"PLAN_STATE"])
                         ,sep="")
colnames(address.key) <- c("Original_Address","Corrected_Address","Address_to_Google","Address_to_Google_noboro","Address_to_Google_nozip")

ag_tmp <- ag_tmp %>% 
  rename(PLAN_ID_UNIQUE = plan_id)

address.key <- left_join(address.key
                         ,ag_tmp %>% 
                           filter(!duplicated(PLAN_STREET)) %>% 
                           select(PLAN_STREET,PLAN_ID_UNIQUE) %>% 
                           dplyr::rename(Original_Address = PLAN_STREET)
                         ,by="Original_Address")

address.key <- address.key %>% 
  filter(!duplicated(Address_to_Google))
# summarize(sum(duplicated(Address_to_Google)))

curr.boro <- unique(ag_tmp[,"PLAN_BORO"])
if(length(curr.boro)>1){
  curr.boro <- paste("third_pass",Sys.Date(),sep="_")
}


full_result.list <- list()
selected_results.list <- list()
loop_len <- nrow(address.key)
# loop_len <- 60
setwd("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/AG Data/Recoded Addresses")
start.time <- proc.time()
for(i in 1:loop_len){
  init_address <- address.key[i,"Address_to_Google"]
  plan_id <- address.key[i,"PLAN_ID_UNIQUE"]
  cat("Starting iteration",i,"out of",loop_len,"\nOur address: ",address.key[i,"Original_Address"],"\nAddress sent to Google: ")
  
  tmp <- try(geoCode(address=as.character(address.key[i,"Address_to_Google"]),verbose=T))
  
  ## if it doesn't take, try without the zip code and without borough
  if(class(tmp)=="try-error"){
    cat("Didn't work, waiting 10 seconds and trying without zip code\n")
    Sys.sleep(5)
    tmp <- try(geoCode(address=as.character(address.key[i,"Address_to_Google_nozip"]),verbose=T))
    if(class(tmp)=="try-error"){
      cat("Still didn't work, waiting 10 seconds and trying without borough\n")
      Sys.sleep(5)
      tmp <- try(geoCode(address=as.character(address.key[i,"Address_to_Google_noboro"]),verbose=T))
      if(class(tmp)=="try-error"){
        cat("Yeah, no luck on this one. Moving right along.\n")
        selected_results.list[[i]] <- as.data.frame(cbind(init_address,NA,NA,NA,NA,plan_id)
                                                    ,stringsAsFactors=F)
        # names(selected_results.list[[i]]) <- c("init_address","latitude","longitude","formatted_address","zip","plan_id")
        full_result.list[[i]] <- list("init_addres" = init_address)
      }
    }
  }
  
  if(class(tmp)!="try-error"){
    selected_results.list[[i]] <- as.data.frame(
      cbind(
        do.call("cbind",tmp[c("init_address","latitude","longitude","formatted_address","zip")])
        ,plan_id
      )
      ,stringsAsFactors=F)
    full_result.list[[i]] <- tmp  
  }
  
  colnames(selected_results.list[[i]]) <- c("init_address","latitude","longitude","formatted_address","zip","plan_id")
  cat("Google geocoded address: ")
  cat(selected_results.list[[i]][1,"formatted_address"],"\n")
  if((i%%100==0) | (i%%loop_len==0)){
    cat("Saving backup\n")
    saveRDS(full_result.list
            ,file=paste("AG_full_google_recode_results_",curr.boro,"_",i,".rds",sep="")
    )
    saveRDS(selected_results.list
            ,file=paste("AG_selected_google_recode_results_",curr.boro,"_",i,".rds",sep="")
    )
  }
  random.sleep <- sample(seq(from=5,to=7,by=.01),1)
  if(i%%15==0){
    random.sleep <- sample(seq(from=8,to=10,by=.01),1)
  }
  cat("Finished iteration ",i,", system sleeping for",random.sleep,"seconds\n\n\n")
  Sys.sleep(random.sleep)
}

geocoded.df <- bind_rows(selected_results.list)




## Final pass 
fourth_pass_manual <- read.csv("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/AG Data/fourth_manual_update.csv"
                              ,stringsAsFactors=F
)

ag_tmp <- fourth_pass_manual

address.key <- as.data.frame(
  cbind(ag_tmp[,"PLAN_STREET"]
        ,ag_tmp[,"PLAN_STREET"]
  )
  ,stringsAsFactors=F
)

zip.vec <- ag_tmp[,"PLAN_ZIP_OLD"]
zip.vec[is.na(zip.vec)] <- ""

address.key[,3] <- paste(trimws(address.key[,2])
                         ,", "
                         ,trimws(ag_tmp[,"PLAN_BORO"])
                         ,", "
                         ,trimws(ag_tmp[,"PLAN_STATE"])
                         ,", "
                         ,zip.vec
                         ,sep="")

address.key[,4] <- paste(trimws(address.key[,2])
                         ,", "
                         ,trimws(ag_tmp[,"PLAN_STATE"])
                         ,", "
                         ,zip.vec
                         ,sep="")


address.key[,5] <- paste(trimws(address.key[,2])
                         ,", "
                         ,trimws(ag_tmp[,"PLAN_BORO"])
                         ,", "
                         ,trimws(ag_tmp[,"PLAN_STATE"])
                         ,sep="")

colnames(address.key) <- c("Original_Address","Corrected_Address","Address_to_Google","Address_to_Google_noboro","Address_to_Google_nozip")

ag_tmp <- ag_tmp %>% 
  rename(PLAN_ID_UNIQUE = plan_id)

address.key <- left_join(address.key
                         ,ag_tmp %>% 
                           filter(!duplicated(PLAN_STREET)) %>% 
                           select(PLAN_STREET,PLAN_ID_UNIQUE) %>% 
                           dplyr::rename(Original_Address = PLAN_STREET)
                         ,by="Original_Address")

address.key <- address.key %>% 
  filter(!duplicated(Address_to_Google))
# summarize(sum(duplicated(Address_to_Google)))

curr.boro <- unique(ag_tmp[,"PLAN_BORO"])
if(length(curr.boro)>1){
  curr.boro <- paste("final_pass",Sys.Date(),sep="_")
}


full_result.list <- list()
selected_results.list <- list()
loop_len <- nrow(address.key)
setwd("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/AG Data/Recoded Addresses")
start.time <- proc.time()
for(i in 1:loop_len){
  init_address <- address.key[i,"Address_to_Google"]
  plan_id <- address.key[i,"PLAN_ID_UNIQUE"]
  cat("Starting iteration",i,"out of",loop_len,"\nOur address: ",address.key[i,"Original_Address"],"\nAddress sent to Google: ")
  
  tmp <- try(geoCode(address=as.character(address.key[i,"Address_to_Google"]),verbose=T))
  
  ## if it doesn't take, try without the zip code and without borough
  if(class(tmp)=="try-error"){
    cat("Didn't work, waiting 10 seconds and trying without zip code\n")
    Sys.sleep(5)
    tmp <- try(geoCode(address=as.character(address.key[i,"Address_to_Google_nozip"]),verbose=T))
    if(class(tmp)=="try-error"){
      cat("Still didn't work, waiting 10 seconds and trying without borough\n")
      Sys.sleep(5)
      tmp <- try(geoCode(address=as.character(address.key[i,"Address_to_Google_noboro"]),verbose=T))
      if(class(tmp)=="try-error"){
        cat("Yeah, no luck on this one. Moving right along.\n")
        selected_results.list[[i]] <- as.data.frame(cbind(init_address,NA,NA,NA,NA,plan_id)
                                                    ,stringsAsFactors=F)
        # names(selected_results.list[[i]]) <- c("init_address","latitude","longitude","formatted_address","zip","plan_id")
        full_result.list[[i]] <- list("init_addres" = init_address)
      }
    }
  }
  
  if(class(tmp)!="try-error"){
    selected_results.list[[i]] <- as.data.frame(
      cbind(
        do.call("cbind",tmp[c("init_address","latitude","longitude","formatted_address","zip")])
        ,plan_id
      )
      ,stringsAsFactors=F)
    full_result.list[[i]] <- tmp  
  }
  
  colnames(selected_results.list[[i]]) <- c("init_address","latitude","longitude","formatted_address","zip","plan_id")
  cat("Google geocoded address: ")
  cat(selected_results.list[[i]][1,"formatted_address"],"\n")
  if((i%%100==0) | (i%%loop_len==0)){
    cat("Saving backup\n")
    saveRDS(full_result.list
            ,file=paste("AG_full_google_recode_results_",curr.boro,"_",i,".rds",sep="")
    )
    saveRDS(selected_results.list
            ,file=paste("AG_selected_google_recode_results_",curr.boro,"_",i,".rds",sep="")
    )
  }
  random.sleep <- sample(seq(from=5,to=7,by=.01),1)
  if(i%%15==0){
    random.sleep <- sample(seq(from=8,to=10,by=.01),1)
  }
  cat("Finished iteration ",i,", system sleeping for",random.sleep,"seconds\n\n\n")
  Sys.sleep(random.sleep)
}

geocoded.df <- bind_rows(selected_results.list)

View(geocoded.df %>% 
  arrange(is.na(latitude)) %>% 
    filter(!duplicated(plan_id)))

getwd()
setwd("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/AG Data")
write.csv(geocoded.df,"final_pass.csv",row.names=F)
saveRDS(selected_results.list,"final_pass_selected_results_list.rds")
saveRDS(selected_results.list,"final_pass_full_results_list.rds")
