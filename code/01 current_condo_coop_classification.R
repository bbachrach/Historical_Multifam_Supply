
##############################################################################################################
##
## This script uses names of filenames of downloaded pdfs, PAD and pluto to create a key linking PDFs to pluto
## PAD is necessary because some PDFs are condo BBLs which cannot be directly linked to pluto
## The key will then be used in scraping the PDFs
##
## PDF formats differ slightly by property type and year
## Knowing the basic characteristics of the property within the PDF should greatly simplify the scrape
## Instead of a very long list of conditional statements, nested functions can be used on each type of PDF
##
## As an ancillary benefit, this process also revealed some BBLs we would be interested in but the
## Download script did not capture (likely due to the BBLs chosen being based on the rawdata.csv file)
## There were slightly north of 1,000 BBLs which would be of interest (elevator building, office, 
## something with more than 25 units) which the download script did not get
## This script also compiles a dataframe of those BBLs which can be passed to the downloader script for 
## a third run
##
##
##############################################################################################################

# setwd("/Users/billbachrach/Desktop/NPV_all")
# setwd("/Users/billbachrach/Desktop/DOF PDFs")
setwd("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/taxbills")

library(dplyr)
library(parallel)
library(lubridate)
library(stringr)
library(pdftools)
library(doParallel)
library(sf)
library(geojsonio)
options(scipen=999)

source("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/useful functions/HWE_FUNCTIONS.r")
source("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/useful functions/-.R")
source("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/useful functions/useful minor functions.R")
source("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/useful functions/hwe colors.R")


# Read in PAD and Pluto ---------------------------------------------------

pluto.all <- readRDS("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Project Data/PLUTO/pluto_lean_compressed_2003_2017.rds")

pluto <- pluto.all %>% 
  filter(!(is.na(BldgClass) | is.na(Block) | is.na(Lot)))%>%
  select(Borough,Block,Lot,ZipCode,Address,BldgClass,BBL
         ,BldgArea,ComArea,ResArea,OfficeArea,RetailArea,GarageArea,StrgeArea
         ,FactryArea,OtherArea,NumBldgs,NumFloors,UnitsRes,UnitsTotal,YearBuilt
         ,CondoNo,XCoord,YCoord,Year,lat,lon) %>% 
  filter(Year >= 2008) %>% 
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
    ,BBL= paste(borocode
                ,as.numeric(Block)
                ,as.numeric(Lot)
                ,sep="_"
    )
    ,BBL_bill = BBL
    ,CondoNo= ifelse(CondoNo==0,
                     0,
                     paste(substr(BBL,start=1,stop=1),CondoNo,sep="_")
    )
  ) %>% 
  # select(-Block_char,-Lot_char) %>%
  ## some BBLs contain empty fields so aren't of use to us, dropping those out
  filter(!(is.na(BBL) | grepl("[[:alpha:]]",BBL))) %>%
  group_by(Year) %>%
  filter(!duplicated(BBL)) %>%
  ungroup() %>%
  mutate(Id.pluto = as.character(1:n()))


pad.df <- readRDS("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Project Data/PAD/PAD_with_Condo_BBLs_expanded_2006_2017.rds") %>%
  mutate(Year = as.character(Year)
         ,Id.pad = as.character(1:n())
         )



# Join PAD/PLUTO ----------------------------------------------------------

cl <- makeCluster(round(detectCores()*.75))
registerDoParallel(cl)

year.vec <- 2009:2017

tmp.list <- foreach(i = 1:length(year.vec)
                    , .packages = c("dplyr")) %dopar% {
                      year.curr <- year.vec[i]
                      pad.tmp <- pad.df %>% filter(Year==year.curr)
                      pluto.tmp <- pluto %>% 
                        filter(Year <= year.curr) %>%
                        arrange(desc(Year)) %>%
                        filter(!duplicated(BBL_bill))
                      
                      out <- pad.tmp %>%
                        select(BBL_bill,BBL,Year,coopnum,condonum,Id.pad) %>%
                        left_join(pluto.tmp %>%
                                    select(Borough,BBL_bill,BldgClass,CondoNo,lat,lon,Year,Id.pluto) %>%
                                    rename(Year.pluto = Year)
                                  ,by="BBL_bill"
                        )
                      return(out)
                    }

stopCluster(cl)

pad_pluto.df <- bind_rows(tmp.list)

## where there are missing rows find most recent year to join with 
pad_pluto.miss <- pad_pluto.df %>%
  filter(is.na(Id.pluto)) %>%
  select(-Borough,-BldgClass,-CondoNo,-lat,-lon,-Year.pluto,-Id.pluto) %>%
  semi_join(pluto
            ,by="BBL_bill")

pad_pluto.miss <- pad_pluto.miss %>%
  left_join(pluto %>%
              arrange(desc(Year)) %>%
              filter(!duplicated(BBL_bill)) %>%
              select(Borough,BBL_bill,BldgClass,CondoNo,lat,lon,Year,Id.pluto) %>%
              rename(Year.pluto = Year)
            ,by="BBL_bill"
  )

# pad_pluto.df.hold <- pad_pluto.df

pad_pluto.df <- bind_rows(
  pad_pluto.df %>%
    anti_join(pad_pluto.miss
              ,by="Id.pad")
  ,pad_pluto.miss
) %>%
  rename(Year.pad = Year)

# saveRDS(pad_pluto.df,"/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/PAD/PAD_pluto_join 20180410_1355.rds")




# Create NPV Key ----------------------------------------------------------

npv.files <- list.files(path="/Users/billbachrach/Desktop/DOF PDFs/NPV_all")

setwd("/Users/billbachrach/Desktop/DOF PDFs/NPV_all")
npv.sizes <- file.size(npv.files)

npv_tmp.key <- gsub("[^[:digit:]]","",
                    gsub("NPV.*","",npv.files)
) %>%
  as.data.frame() %>%
  setNames("bbl") %>%
  mutate(bbl = as.character(bbl)
         ,BBL = paste(
           as.numeric(str_sub(bbl,start=1,end=1))
           ,as.numeric(str_sub(bbl,start=2,end=-5))
           ,as.numeric(str_sub(bbl,start=-4,end=-1))
           ,sep="_"
         )
  )

npv_tmp.key[,"doc_year"] <- gsub("[^[:digit:]]",""
                                 ,gsub("_.*",""
                                       ,gsub(".*NPV","",npv.files)
                                 )
)

npv_tmp.key[,"file_size"] <- npv.sizes
npv_tmp.key[,"filename"] <- npv.files

npv_tmp.key <- npv_tmp.key %>%
  filter(file_size > 2500) %>%
  mutate(Year = as.character(as.numeric(doc_year)-1)
         ,BBL_docyear = paste(BBL,doc_year,sep="_")
         ) %>%
  filter(Year >= 2009)
  

# Join NPV key with pad/pluto ---------------------------------------------

cl <- makeCluster(detectCores()/2)
registerDoParallel(cl)

year.vec <- 2009:2017

tmp.list <- foreach(i = 1:length(year.vec)
                    , .packages = c("dplyr")) %dopar% {
                      # for(i in 1:length(year.vec)){
                      year.curr <- year.vec[i]
                      npv.tmp <- npv_tmp.key %>% filter(Year==year.curr)
                      
                      pad_pluto.tmp <- pad_pluto.df %>% 
                        filter(Year.pad <= year.curr) %>%
                        arrange(desc(Year.pad)) %>%
                        filter(!duplicated(BBL))
                      
                      out <- npv.tmp %>%
                        left_join(pad_pluto.tmp
                                  ,by="BBL"
                        )
                      return(out)
                    }

stopCluster(cl)

npv_join.key <- bind_rows(tmp.list)
npv_join.key.hold <- npv_join.key

npv_join.miss <- npv_join.key %>%
  filter(is.na(BBL_bill))

npv_join.miss <- npv_join.miss %>%
  select(colnames(npv_tmp.key)) %>%
  left_join(
    pad_pluto.df %>%
      arrange(desc(Year.pad)) %>%
      filter(!duplicated(BBL))
    ,by="BBL"
  )

npv_join.key <- bind_rows(
  npv_join.key %>%
    anti_join(npv_join.miss
              ,by="BBL_docyear")
  ,npv_join.miss
)


rm(pad.df,tmp.list,pad_pluto.df.hold,npv_join.key.hold,pad_pluto.notin,pluto.tmp,out,cl,pluto.all)
gc()
# save.image("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Refresh/data/key_create_workspace 20180430_1236.RData")


# Scrape PDF building classes ---------------------------------------------
## NOTE: this takes quite a while 

source("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Git/DOF_taxbills_scrape/NPV Scrape Individual Parameters/NPV Scrape Functions.R")
setwd("/Users/billbachrach/Desktop/DOF PDFs/NPV_all")

npv.key_small <- npv_join.key %>%
  select(filename,doc_year,BldgClass,BBL,Id.pad)

df.breaks <- c(seq(from=1,to=nrow(npv.key_small),by=50000),nrow(npv.key_small))
BldgClass_outer.list <- list()

start.time <- Sys.time()

start <- 2
ptm.outer <- proc.time()
for(i in start:length(df.breaks)){
  ptm <- proc.time()
  
  tmp.df <- npv.key_small[df.breaks[i-1]:df.breaks[i],]
  len <- nrow(tmp.df)
  
  cl <- makeCluster(round(detectCores()*.5),type="FORK")
  
  
  BldgClass_outer.list[[i-1]] <- parLapply(cl,1:len, function(x){
    out <- list()
    
    out$BBL <- tmp.df[x,"bbl"]
    out$Id.pad <- tmp.df[x,"Id.pad"]
    out$doc_year <- tmp.df[x,"doc_year"]
    out$filename <- tmp.df[x,"filename"]
    out$BldgClass <- tmp.df[x,"BldgClass"]

    tmp.text <- try(suppressWarnings(pdf_text(tmp.df[x,"filename"])),silent=T)
    
    # out$raw <- tmp.text
    
    if(class(tmp.text)=="try-error"){
      out$data <- rep(NA,7)
    }else{
      bldg_class <- bldg_class.fun(tmp.text = tmp.text, doc_year = tmp.df[x,"doc_year"])
      res_units <- units.fun(tmp.text = tmp.text, doc_year = tmp.df[x,"doc_year"])
      
      ## this is wrong, shoudl be ,tmp.df[x,"bbl]
      ## sorted out in the next lapply but should be corrected
      tmp.out <- cbind(bldg_class
                       ,res_units
                       ,tmp.df[x,"BBL"]
                       ,tmp.df[x,"Id.pad"]
                       ,tmp.df[x,"doc_year"]
                       ,tmp.df[x,"BldgClass"]
                       ,tmp.df[x,"filename"]
      )
      out$data <- tmp.out
    }
    
    rm(tmp.text)
    return(out)
  }
  )
  stopCluster(cl)
  gc()
  cat("Time in loop ", i-1,"out of",length(df.breaks)-1,":",(proc.time() - ptm)[3],"seconds\n")
}
BldgClass_full.time <- proc.time() - ptm.outer


BldgClass_df.list <- list()

start <- 1
for(i in start:length(BldgClass_outer.list)){
  cat("starting iteration ",i,"\n")
  tmp.list <- BldgClass_outer.list[[i]]
  names(tmp.list) <- unlist(lapply(tmp.list, function(x) x$filename))
  
  tmp.list.drops <- which(unlist(lapply(tmp.list, function(x) class(x$data)!="matrix")))
  
  tmp.list[tmp.list.drops] <- NULL
  
  cat("Unlisting\n")
  cl <- makeCluster(detectCores()-2,type="FORK")
  tmp.list <- parLapply(cl,tmp.list, function(x) x$data %>% as.data.frame(.,stringsAsFactors=F))
  stopCluster(cl)

  cat("Binding\n")
  tmp.df <- bind_rows(tmp.list)[,1:7] %>%
    setNames(c("BldgClass","UnitsRes","BBL","Id.pad","doc_year","BldgClass.pluto","filename")) %>%
    mutate(bbl =   gsub("[^[:digit:]]",""
                        ,gsub("NPV.*","",filename)
    ))
  
  BldgClass_df.list[[i]] <- tmp.df
  cat("Finished iteration ",i,"\n")
}


BldgClass.df <- bind_rows(BldgClass_df.list) %>%
  mutate(BBL_Year = paste(BBL,(as.numeric(doc_year)-1),sep="_")) %>%
  filter(nchar(filename)==nchar("1013971550.NPV.2014_01_15.pdf") 
         & !duplicated(paste(BBL_Year,BldgClass,sep="_")))

# saveRDS(BldgClass.df,"/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Refresh/data/BuildingClass_df 20180430_1541.rds")
# BldgClass.df <- readRDS("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Refresh/data/BuildingClass_df 20180430_1541.rds")

## Join with PAD/PLUTO
pad_pluto.df.hold <- pad_pluto.df

pad_pluto.df <- pad_pluto.df.hold %>%
  mutate(BBL_Year = paste(BBL,Year.pad,sep="_")) %>%
  left_join(BldgClass.df %>%
              mutate(UnitsRes_Internal = as.numeric(UnitsRes)) %>%
              select(BldgClass,BBL_Year,UnitsRes_Internal) %>%
              rename(BldgClass_pdf = BldgClass)
            ,by="BBL_Year"
            )

pp_2017.df <- pad_pluto.df %>%
  filter(Year.pad == 2017)

pp_miss.df <- pp_2017.df %>%
  filter(is.na(BldgClass_pdf))

## where building class is missing look for the most recent observation and add it in
out_df.list <- list()
year.vec <- 2016:2014
for(i in 1:length(year.vec)){
  curr.year <- year.vec[i]
  
  tmp.df <- pad_pluto.df %>%
    filter(Year.pad == curr.year & !is.na(BldgClass_pdf))
  
  out.df <-  tmp.df %>%
    semi_join(
      pp_miss.df
      ,by="BBL"
    )

  out_df.list[[i]] <- out.df
}

pp_found.df <- bind_rows(out_df.list) %>%
  filter(!duplicated(BBL))

pp_2017.df <- pp_2017.df %>%
  left_join(pp_found.df %>%
              select(BBL,BldgClass_pdf,UnitsRes_Internal) %>%
              rename(BldgClass_pdf.aux = BldgClass_pdf
                     ,UnitsRes_Internal.aux = UnitsRes_Internal
              )
            ,by="BBL"
  ) %>%
  mutate(BldgClass_pdf = ifelse(is.na(BldgClass_pdf) & !is.na(BldgClass_pdf.aux)
                                ,BldgClass_pdf.aux
                                ,BldgClass_pdf
  )
  ,UnitsRes_Internal = ifelse(is.na(UnitsRes_Internal) & !is.na(UnitsRes_Internal.aux)
                              ,UnitsRes_Internal.aux
                              ,UnitsRes_Internal
  )
  ) %>%
  select(-BldgClass_pdf.aux,-UnitsRes_Internal.aux) %>%
  rename(BldgClass_bill = BldgClass)


# Classifying -------------------------------------------------------------

## identify the most prevalent building classes at condo level
pp_group.df <- pp_2017.df %>%
  filter(!is.na(BldgClass_pdf)) %>%
  group_by(BBL_bill,BldgClass_pdf) %>%
  summarize(count = n()
            ,units = sum(UnitsRes_Internal,na.rm=T)
            ) %>%
  ungroup() %>%
  group_by(BBL_bill) %>%
  arrange(desc(units)) %>%
  mutate(order = 1:n()) %>%
  ungroup() %>%
  rename(BBL = BBL_bill)

## read in pluto 2017 (think I already have this but whatever)
condocoop.df <- readRDS("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Project Data/PLUTO/pluto_lean_compressed_2003_2017.rds") %>%
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
  )


# condocoop.df.hold <- condocoop.df

condocoop.df <- condocoop.df %>%
  left_join(pp_group.df %>%
              filter(order == 1) %>%
              select(BBL,BldgClass_pdf) %>%
              rename(BldgClass_Internal.1 = BldgClass_pdf)
            ,by="BBL"
            ) %>%
  left_join(pp_group.df %>%
              filter(order == 2) %>%
              select(BBL,BldgClass_pdf) %>%
              rename(BldgClass_Internal.2 = BldgClass_pdf)
            ,by="BBL"
  ) %>%
  left_join(pp_group.df %>%
              filter(order == 3) %>%
              select(BBL,BldgClass_pdf) %>%
              rename(BldgClass_Internal.3 = BldgClass_pdf)
            ,by="BBL"
  )


## Bldg Classes at the condo level 
# tmp.classes <- c(
#   condocoop.df %>% pull(BldgClass_Internal.1)
#   ,condocoop.df %>% pull(BldgClass_Internal.2)
#   ,condocoop.df %>% pull(BldgClass_Internal.3)
# )
# 
# tmp.classes <- tmp.classes[!duplicated(tmp.classes)]
# tmp.classes <- tmp.classes[order(tmp.classes)]

small.classes <- c("A0","A1","A2","A3","A4","A5","A6","A7","A9","B1","B2","B3","B9","S1","S2")
rental.classes <- c("C0","C1","C2","C3","C4","C5","C7","C9","D1","D2","D3","D5","D6","D7","D8","D9","RR","S4","S5","S9")
condo.classes <- c("R0","R1","R2","R3","R4","R6","R8")
coop.classes <- c("C6","C8","D0","D4","R9")
other.classes <- c("CM","R5","R7","RB","RG","RH","RK","RP","RS","RT","RA","RW")


condocoop.df <- condocoop.df %>%
  # filter(UnitsRes>0) %>%
  mutate(Type = ifelse((BldgClass %in% condo.classes | BldgClass_Internal.1 %in% condo.classes)
                       ,"condo"
                       ,ifelse((BldgClass %in% coop.classes | BldgClass_Internal.1 %in% coop.classes)
                               ,"coop"
                               ,ifelse(((BldgClass %in% rental.classes | BldgClass_Internal.1 %in% rental.classes) & UnitsRes>=3)
                                       ,"rental"
                                       ,ifelse(BldgClass %in% small.classes | BldgClass_Internal.1 %in% small.classes
                                               ,"small"
                                               ,"other"
                                       )
                               )
                       )
  )
  ,Type = ifelse(BldgClass %in% small.classes
                 ,"small"
                 ,Type
  )
  ) %>%
  select(BBL,Address,BldgClass,BldgClass_Internal.1,BldgClass_Internal.2,BldgClass_Internal.3,ZipCode,lon,lat,Type)


# Attach Neighborhood -----------------------------------------------------

## read in pediacities shapefile
pediashape.url <- "http://data.beta.nyc//dataset/0ff93d2d-90ba-457c-9f7e-39e47bf2ac5f/resource/35dd04fb-81b3-479b-a074-a27a37888ce7/download/d085e2f8d0b54d4590b1e7d1f35594c1pediacitiesnycneighborhoods.geojson"

pedia.map <- geojson_read(as.location(pediashape.url),
                          method="local",
                          what="sp")

pedia.map <- st_as_sf(pedia.map,crs=4326)

## can only geolocate on those with lat/lon
tmp.sf <- st_as_sf(condocoop.df %>% 
                     filter(!is.na(lat)) %>%
                     select(BBL,lon,lat)
                   ,coords = c("lon", "lat"), crs = 4326)

## spatial join and then re-join with resi.out
tmp.sf <- st_join(tmp.sf
                  ,pedia.map %>%
                    rename(Neighborhood = neighborhood) %>%
                    mutate(BOROUGH.PEDIA = toupper(as.character(borough)))) %>% 
  select(BBL,Neighborhood)


condocoop.df <- left_join(condocoop.df
                      ,tmp.sf %>%
                        select(BBL,Neighborhood)
                      ,by="BBL"
) %>%
  select(-geometry)


saveRDS(condocoop.df,"/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Refresh/data/condocoop_df 20180430_1615.rds")

# condocoop.df %>%
#   summarize(count = sum(UnitsRes>=3)
#             ,unit.count = sum(UnitsRes[UnitsRes>=3])
# 
#             ,condo.count = sum(Type == "condo")
#             ,condo_unit.count = sum(UnitsRes[Type=="condo"])
# 
#             ,coop.count = sum(Type == "coop")
#             ,coop_unit.count = sum(UnitsRes[Type=="coop"])
# 
#             ,rental.count = sum(Type == "rental")
#             ,rental_unit.count = sum(UnitsRes[Type=="rental"])
# 
#             # ,na.count = sum(is.na(Type))
#             # ,na_unit.count = sum(UnitsRes[is.na(Type)])
#   )




