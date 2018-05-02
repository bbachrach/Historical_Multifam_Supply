Historical Multifamily Supply
================

This collection of scripts generates historical multifamily stock from 1980-current.

> The process is broken down into 4 scripts

-   [Current Condo-Coop Classification](#current-condo-coop-classification)
    -   The output of this script is a dataframe which identifies buildings as condos or coops. It utilizes DOF Notice of Property Value documents, PAD and Pluto to determine building class at the condo BBL level.
    -   [Read in and munge PLUTO and PAD](#read-in-and-munge-pluto-and-pad)
    -   [Create NOPV Key](#create-nopv-key)
    -   [Scrape PDF building classes](#scrape-pdf-building-classes)
    -   [Join NOPV data and PDF/PAD](#join-nopv-data-and-pdfpad)
    -   [Classifying](#classifying)
    -   [Attach Neighborhood](#attach-neighborhood)
-   [AG Data Address Cleaning](#ag-data-address-cleaning)
    -   This script takes the data given to us by the attorney general's office and links it to BBL. It then creates a dataframe with conversion date for billing level BBLs which can be linked to pluto. It operates primarily by calling the NYC GeoClient API. It should be noted that this process is heavily reliant on someone manually correcting some addresses and eventually finding the BBL of a small number of addresses which cannot be processed programatically.
    -   [Read in and Munge AG data and PLUTO](#read-in-and-munge-ag-data-and-pluto)
    -   [make address key](#make-address-key)
    -   [call to NYC Geoclient](#call-to-nyc-geoclient)
    -   [Find missing observations and run through a second time](#find-missing-observations-and-run-through-a-second-time)
    -   [Run through the names which were fixed manually](#run-through-the-names-which-were-fixed-manually)
    -   [Read in Final Manual step and create output](#read-in-final-manual-step-and-create-output)
-   [Pluto Augment Create](#pluto-augment-create)
    -   This script takes the condo coop classifications and cleansed AG data to create a pluto dataframe that includes fields for whether a building is condo/coop and (if applicable) the date it was converted. It further creates dataframes which indicate how many units were converted by year and geographic area
    -   [Readin Munge Join](#readin-munge-join)
    -   [Add TCO data to pluto](#add-tco-data-to-pluto)
    -   [Join with Neighborhood](#join-with-neighborhood)
    -   [Conversions dataframe](#conversions-dataframe)
-   [Back in Time](#back-in-time)
    -   The final script takes the pluto augmented dataframe and steps backward through time to determine housing stock by year and area
    -   [Step backward through time](#step-backward-through-time)
    -   [Calculate the number of condos and coops for a given year](#calculate-the-number-of-condos-and-coops-for-a-given-year)
    -   [Combine and save to disk](#combine-and-save-to-disk)
    -   [Excel instructions](#excel-instructions)

Current Condo-Coop Classification
=================================

The first script in the process identifies which buildings contain condo units or coop units. While building classes are provided by PLUTO these are at the billing BBL level. Many "condo" buildings actually are instances wherein ground floor has been condoed off for retail or a single penthouse has been condoed off while the bulk of units remain rentals. A related wrinkle is that a coop can exist within a condo for similar reasons.

To deal with this the script creates a dataframe of building classes by BBL using the DOF Notice of Property Value documents. This is then linked to billing BBL using PAD. Where a 75xx billing BBL actually contains co-ops there will be a small number of documents one of which indicates a co-op building the others indicating commercial, storage, etc. The same is true for billing BBLs that contain rental units. In the instances where a billing BBL actually contains condos we see many documents which indicate some form of condo.

``` r
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
```

Read in and munge PLUTO and PAD
-------------------------------

Read in pluto, restrict to 2008 or later, create HWE standardized BBL. Borocode is created from Borough rather than stripping the first number from existing BBL as some of the existing BBLs have NA values.

``` r
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
  select(-Block_char,-Lot_char) %>%
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
```

Join PAD and PLUTO
------------------

For some billing BBLs in PAD there is not a matching PLUTO record. My best guess is that datasets are released at slightly different times. It's also possible they are compiled in different ways be different teams and simply have discrepancies. To join on both BBL and Year, for each individual year PLUTO is pared back to versions of that year and earlier and duplicate BBLs are dropped. This increases coverage. PAD and PLUTO are joined in this manner list-wise.

``` r
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
```

Even with this process there are still some observations which cannot be linked on both BBL and Year. For these observations simply selecting the most recent PLUTO for which the BBL is available.

``` r
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
```

Create NOPV Key
---------------

List all Notice of Property Value PDF names. Using these names create a dataframe that includes pertinent information (BBL, Year, file size). Remove any filenames with sizes under 2500 bytes. Anything below this size appears to be null.

``` r
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
```

Using the same method as before to join on on both BBL and Year. Note that this is more pertinent here than with the PAD-PLUTO join as NPV documents are definitely created at a different point in time from PLUTO.

``` r
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
```

Bind and then use the same method as before where missing observations are linked with anything possible. Removing some unnecessary objects from the workspace to free up memory.

``` r
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
```

Scrape PDF building classes
---------------------------

This portion takes quite a while to run and is memory intensive.

First, setting working directory and taking only the necessary variables from npv.key.

``` r
source("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Git/DOF_taxbills_scrape/NPV Scrape Individual Parameters/NPV Scrape Functions.R")
setwd("/Users/billbachrach/Desktop/DOF PDFs/NPV_all")

npv.key_small <- npv_join.key %>%
  select(filename,doc_year,BldgClass,BBL,Id.pad)
```

Break job into manageable chunks, create empty list and set the iterator start value to 2 (the for loop references i-1)

``` r
df.breaks <- c(seq(from=1,to=nrow(npv.key_small),by=50000),nrow(npv.key_small))
BldgClass_outer.list <- list()

start <- 2
```

In the outer portion of the loop, create a temporary dataframe consisting of a 100k (or other specified value) row chunk of npv.key and create cluster

``` r
ptm.outer <- proc.time()
for(i in start:length(df.breaks)){
  ptm <- proc.time()
  
  tmp.df <- npv.key_small[df.breaks[i-1]:df.breaks[i],]
  len <- nrow(tmp.df)
  
  cl <- makeCluster(round(detectCores()*.5),type="FORK")
```

In the inner portion of the loop, set variables from the npv dataframe and then call scrape text from the PDF filename. Use the building class function to pull building class out of the resulting text and the units function to pull the number of residential units listed in each PDF. Bind into a format that can be turned into a larger dataframe later.

Note that within each list item BBL, PAD ID and other information as stored as its own list item alongside redundant data. This is for troubleshooting purposes and not strictly necessary in production format.

``` r
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
```

Some of what would logically be in the prior loop is taken out and done separately. Because the prior loop is so time consuming error handling was simpler if as much as possible was taken out and done separately.

Drop out items which are null (data in the list item is not in appropriate matrix format). Make each list item a dataframe. Bind and create DOF format BBL variable.

``` r
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
```

Bind into single dataframe and save to disk.

Because the NOPV documents describe the property in the as of the very beginning of the year a variable to join on is created which is document year lagged 1. Observations which are duplicates or where the filename is not in the appropriate format are dropped.

``` r
BldgClass.df <- bind_rows(BldgClass_df.list) %>%
  mutate(BBL_Year = paste(BBL,(as.numeric(doc_year)-1),sep="_")) %>%
  filter(nchar(filename)==nchar("1013971550.NPV.2014_01_15.pdf") 
         & !duplicated(paste(BBL_Year,BldgClass,sep="_")))

# saveRDS(BldgClass.df,"/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Refresh/data/BuildingClass_df 20180430_1541.rds")
# BldgClass.df <- readRDS("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Refresh/data/BuildingClass_df 20180430_1541.rds")
```

Join NOPV data and PDF/PAD
--------------------------

Join for year 2017. Note that I'm taking the number of units attached to each pdf.

``` r
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
```

Where there a match wasn't found to join on see if there is a match 2014 or newer. Give preference to newer versions.

``` r
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
```

Add back into the pp\_2017.df dataframe

``` r
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
```

Classifying
-----------

For each billing BBL determine which internal building class contains the most residential units

``` r
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
```

Read PLUTO back in

``` r
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
```

Join wth pp\_group creating a variable indicating the first, second and third most prevalent building classes at the condo level (currently we are only using the most prevalent class)

``` r
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
```

Look at the building classes at the condo level to determine in which category each should go

``` r
# tmp.classes <- c(
#   condocoop.df %>% pull(BldgClass_Internal.1)
#   ,condocoop.df %>% pull(BldgClass_Internal.2)
#   ,condocoop.df %>% pull(BldgClass_Internal.3)
# )
# 
# tmp.classes <- tmp.classes[!duplicated(tmp.classes)]
# tmp.classes <- tmp.classes[order(tmp.classes)]
```

Assign building classes to "small" (single or two family homes), "rental", "condo", "coop" or "other" categories.

``` r
small.classes <- c("A0","A1","A2","A3","A4","A5","A6","A7","A9","B1","B2","B3","B9","S1","S2")
rental.classes <- c("C0","C1","C2","C3","C4","C5","C7","C9","D1","D2","D3","D5","D6","D7","D8","D9","RR","S4","S5","S9")
condo.classes <- c("R0","R1","R2","R3","R4","R6","R8")
coop.classes <- c("C6","C8","D0","D4","R9")
other.classes <- c("CM","R5","R7","RB","RG","RH","RK","RP","RS","RT","RA","RW")
```

Assign to observations within pluto 2017

``` r
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
```

Attach Neighborhood
===================

Use customary method with pediacities shapefile

``` r
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
```

Save to disk

``` r
saveRDS(condocoop.df,"/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Refresh/data/condocoop_df 20180430_1615.rds")
```

AG Data Address Cleaning
========================

Read in and Munge AG data and PLUTO
-----------------------------------

Read in version of ag data to keep raw for troubleshooting

``` r
saveRDS(condocoop.df,"/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Refresh/data/condocoop_df 20180430_1615.rds")
```

``` r
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
```

``` r
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
```

Set ZipCode to NA where it doesn't contain a digit, select only residential conversions

``` r
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
```

make address key
----------------

Start with cursory cleaning of street names. As an improvement on the prior version, I do this within dplyr syntax instead of nested vector operations. Much easier to see. However, I did try to keep the order of operations as close to the original as possible to avoid creating more effort in the manual address cleansing stage.

Create the base key with the plan id, name and address. First, fix "the Saints". After that there area host of other corrections. Most of the regex statements apply to issues that are prevalent. Some are one offs that I just happened to throw in in the previous iteration and figured why not keep them.

``` r
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
```

Put AG data into the address key (again, this was done kind of backwards originally), drop out duplicates and put into a format which is appropriate for feeding to the API.

``` r
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
```

call to NYC Geoclient
---------------------

Function to create URLs with zipcode and borough.

``` r
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
```

Create vector of URLs and put into the address key. URLs can be used as foreign key to join on later in the process.

``` r
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
```

Set start and end for the foreach loop and create cluster.

``` r
start <- 1
end <- length(tmp.vec)

cl <- makeCluster(round(detectCores()*.75))
registerDoParallel(cl)
```

Call API for all URLs in the vector

``` r
ptm <- proc.time()
initial_output.list <- foreach(i = start:end
                               , .packages = c("httr")) %dopar% {
                                 out <- try(GET(url = tmp.vec[i]))
                                 return(out)
                               }
stopCluster(cl)
api_call.time <- proc.time() - ptm
```

Extract content from the JSON objects within the list created by calling the API

``` r
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
```

Set items which are not dataframes to null and bind list into a single dataframe

``` r
tmp_out.list[which(unlist(
  lapply(tmp_out.list, function(x) class(x))
) != "data.frame")] <- NULL

tmp.out <- bind_rows(tmp_out.list)
```

Find missing observations and run through a second time
=======================================================

Find missing observations

``` r
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
```

Run missing observations through the API to make sure we got everything possible

``` r
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
```

Bind, add to tmp.out and find all observations which were not coded

``` r
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
```

Select the AG data which was not able to be geolocated by the API and write to disk for the addresses to be manually cleansed

NOTE: Prior experience with this showed that the returns on attempting to geolocate algorithmically diminish steeply. Even with 500 someodd addresses to clean it's faster to do so manually than to write a script that calls a second API.

``` r
notcoded.df <- notcoded.df %>%
  left_join(
    ag_tmp %>%
      select(PLAN_NAME,PLAN_ID_UNIQUE)
    ,by="PLAN_ID_UNIQUE"
  )

notcoded.df <- notcoded.df %>%
  select(PLAN_ID_UNIQUE,Original_Address,PLAN_NAME,Corrected_Address,borough,zip)

write.csv(notcoded.df
          ,"/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Refresh/data/initial_notcoded_condo_coop_conversions.csv"
          ,row.names=F
          ,na=""
)
```

Run through the names which were fixed manually
===============================================

Read in the addresses which were cleaned manually. Create a vector of URLs and add into the new address key.

``` r
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
```

Initialize cluster

``` r
start <-1
end <- nrow(manual_coded.df)

cl <- makeCluster(round(detectCores()/2))
registerDoParallel(cl)
```

Call API

``` r
ptm <- proc.time()
second_output.list <- foreach(i = start:end
                              , .packages = c("httr")) %dopar% {
                                out <- try(GET(url = manual_coded.df[i,"url"]))
                                return(out)
                              }
stopCluster(cl)
second_api_call.time <- proc.time() - ptm
```

Get contents from items returned

``` r
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
```

Bind and create dataframe of the portions of the address key which cannot be geocoded even using the cleaned addresses

``` r
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
```

Write to disk to find the BBLs manually.

``` r
write.csv(notcoded_secondary.df
          ,"/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Refresh/data/initial_secondary_notcoded_condo_coop_conversions.csv"
          ,row.names=F
          ,na=""
)
```

I've found the best primary means of finding BBLs for addresses is using NYC's zoning and land use (ZoLa) interface. However, for some observations no address is given or incorrect addresses are given. By using a combination of plan names, the DOB's Building Information Systems website and Google I was able to recover a good amount of these. However, there a handful of addresses for which there is simply not enough information.

ZoLa URL: <https://zola.planning.nyc.gov/about#9.72/40.7125/-73.733>

Read in Final Manual step and create output
-------------------------------------------

``` r
mancoded_secondary.df <- read.csv("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Refresh/data/secondary_notcoded_condo_coop_conversions manual_code 20180420_1134.csv"
                                  ,stringsAsFactors=F)
```

Pull address key for observations which were programmatically coded

``` r
tmp.out <- tmp.out %>%
  left_join(
    address.key %>%
      select(PLAN_ID_UNIQUE,url)
    ,by="url"
  ) %>%
  filter(!is.na(bbl))
```

The formatting issue for the majority of AG Addresses which could not be coded was that multiple addresses were in a single line. In a few circumstances these are multiple BBLs being converted at the same time but more often than not they are multiple street addresses for the same BBL. It was my assessment that in blowing out the addresses the more common occurance was that more errant BBLs were included and unfortunately there is no way to know for certain. Moreover, becuase the number of units stated in the AG documents are for all addresses included we only want to include one instance from each plan Id. To deal with this I group on plan Id and select the BBL which appears the greatest number of times.

``` r
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
```

Bind the programmatically geocoded data with the manally coded data and create HWE standardized format BBL.

``` r
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
```

Add in the final manually coded addresses

``` r
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
```

Creating a variable for the year in which the conversion was done.

A number of plans do not include the date the plan was effected. I initially suspected that these plans may have never been enacted but after carefully looking through the data I found that this is not necessarily indicative of whether a plan was enacted or not. Of the plans I inspected some were abandoned but not in sizable numbers and even those with an effective date may have been abandoned.

I further found that for observations with a PLAN\_DATE\_EFFECTIVE value the average time in between when a plan was reviewed and when a plan was enacted was approximately 340 days. Though there was a good deal of variance it appeared stochastic. So to deal with missing PLAN\_DATE\_EFFECTIVE values I simply add 1 to the year in which the plan was reviewed.

``` r
AGdata.df <- AGdata.df %>%
  mutate(EFFECTIVE_YEAR = ifelse(!is.na(PLAN_DATE_EFFECTIVE)
                                 ,year(PLAN_DATE_EFFECTIVE)
                                 ,year(PLAN_DATE_REVIEWED) + 1
  )
  )
```

Creating final output and saving to disk.

Where conversion type is COOPERATIVE/CONDOMINIUM my best guess is that these are coops within a condo building. I set these to COOPERATIVE because their condo status is only a technicality.

``` r
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
```

Pluto Augment Create
====================

Readin Munge Join
-----------------

Read in the condo/coop classifier dataframe from the first script and pluto. Put both BBL and APPBBL (the previous BBL for the property) into HWE standardized format.

``` r
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
```

A number of rows in pluto have improperly formatted values in the YearBuilt field or a value of 0. To try and fill some of these out I take a slice of all pluto versions 2014 forward, remove observations with an improperly formatted YearBuilt field, then duplicated BBLs and attempt to join 2017 pluto with it.

``` r
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
```

Do the same in an attempt to recover more latitude and longitude coordinates. For this one it's assumed that BBLs are not re-used and thus remain geographically stationary. Operating on this assumption it is not necessary to restrict the auxiliary pluto dataset to recent versions.

``` r
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
```

Join pluto with the condo/coop classification dataframe to create the pluto augmented dataframe. Redundant, but setting all A and B class buildings to "small" type.

``` r
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
```

I'm not 100% sure what's hand coded or why in this excel file from the original script. It appears YearBuilt and Type. Reading in and adding to pluto augmented

``` r
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
```

Add TCO data to pluto
---------------------

For most observations to get delivery date we just add 2 to the YearBuit date. However, for buildings delivered 2012 or later it's possible to get the actual TCO date

Get TCO data from API

``` r
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
```

Buildings typically have multiple TCOs. Some TCOs are issued to buildings that are not new build. Restricting the TCO data to just new build projects and setting TCO date to the first TCO date the building has within the dataset.

``` r
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
```

Where BBLs have not matched to a TCO it's plausible that in the TCO dataset the BBL listed is actually the building's prior BBL. An example may be a building that was declared condo after it was constructed. Attempting to join on APPBBL (previous BBL) instead.

``` r
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
```

Attempting to join on Address

``` r
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
) 
```

For observations which we cannot get a TCO date for just add 2 to YearBuilt. Remove observations for which we can't get some form of TCO date because those BBLs are dead to us.

``` r
pluto.aug <- pluto.aug %>%
  mutate(TCO.real = year(C_O_ISSUE_DATE)
         ,TCO.impute = ifelse(
           nchar(YearBuilt)==4
           ,YearBuilt + 2
           ,NA
         )
         ,TCO = ifelse(is.na(TCO.real)
                       ,TCO.impute
                       ## If TCO is implausibly far away from YearBuilt, just use YearBuilt
                       ,ifelse((TCO.real - YearBuilt>4)
                               ,ifelse(!is.na(TCO.impute)
                                       ,TCO.impute
                                       ,TCO.real
                               )
                               ,TCO.real
                       )
         )
  ) %>%
  ## if YearBuilt is screwed and we don't have actual TCO, drop the observation
  filter((YearBuilt !=0 & nchar(YearBuilt)==4) | !is.na(TCO.real))

pluto.aug <- pluto.aug %>% mutate(Borough = ifelse(Borough=="MN","MANHATTAN",Borough)
                                  ,Borough = ifelse(Borough=="BX","BRONX",Borough)
                                  ,Borough = ifelse(Borough=="BK","BROOKLYN",Borough)
                                  ,Borough = ifelse(Borough=="QN","QUEENS",Borough)
                                  ,Borough = ifelse(Borough=="SI","STATEN ISLAND",Borough)
)
```

Join with Neighborhood
----------------------

For observations which we cannot get a TCO date for just add 2 to YearBuilt. Remove observations for which we can't get some form of TCO date because those BBLs are dead to us.

``` r
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
```

Conversions dataframe
---------------------

In the next script we take out units by YearBuilt and add back in the number of units converted to condo/coop in that area and year. If we include buildings which cannot be joined with pluto we would be overcounting. Prior to tabulating the number of units converted in a given year the AG data is joined with pluto. The conversion dataframe is then tabulated by conversion year in our pluto augmented dataframe. This way the output is internally accurate.

Read in output from the AG Data Address Cleaning script and join with pluto

``` r
AGdata.df <- readRDS("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Refresh/data/condo_coop_conversions_df 20180427_1306.rds")
```

Join AG data with Pluto. Where conversion year is missing, use APPDate if the APPBBL is not a condo and the TCO is less than or equal to the APPDate.
If conversion year is really close to TCO, just set it to TCO.
If the conversion is within 5 years of the TCO we're saying that the building was originally intended to be condos. In those circumstances creating a variable DECLARE\_NEWBUILD

``` r
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
```

The conversions dataframes are no longer necessary but since the code looked so nice I'm keeping it in.

``` r
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
```

Back in Time
============

This is the core concept of our historical multifamily supply analysis. Within a given geographic area (1) calculate the number of units which are condo, coop, rental respectively for the base year (2) remove all units TCO'd in the current year (3) calculate the number of units which are condo, coop, rental (4) add back in to rentals the number of units which were converted to condo or coop in that year (5) repeat for next year and continue back to 1980.

Read in the previously created pluto augmented dataframe

``` r
pluto.aug <- readRDS("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Refresh/data/pluto_augmented 20180501_1332.rds")
```

Read in the proportion of condos being rented out. File is in format of one observation per HVS year. After reading in file blow out so that there is a value for every year 1980-2017. Years on which there was not an HVS wave recieve the value from the previous HVS wave

``` r
condo_rental.df <- read.csv("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/HVS/condo rental metric small.csv"
                            ,stringsAsFactors=F)

## blow out to contain every year, not just the HVS years
tmp.df <- as.data.frame(
  cbind(1980:2017
        ,rep(NA,38)
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
```

Names of boroughs and neighborhoods and vector of years to be used in apply loops. Note that the years progress backwards.

``` r
Borough.levs.conversion <- pluto.aug %>%
  filter(!is.na(Borough) & !duplicated(Borough)) %>%
  pull(Borough)

Neighborhood.levs.conversion <- pluto.aug %>%
  mutate(AREA = as.character(Neighborhood)) %>%
  filter(!is.na(AREA) & !duplicated(AREA)) %>%
  arrange(AREA) %>%
  pull(AREA)

year.levs <- (year(Sys.Date())-1):1980
```

Step backward through time
--------------------------

### Neighborhood

Initiate cluster and start outer parallel loop which iterates over neighborhood

``` r
cl <- makeCluster(detectCores()-2,type="FORK")

tmp.derp <- parLapply(cl,Neighborhood.levs.conversion, function(z){
```

Filter pluto augmented to only area of interest

``` r
tmp.df.outer <- pluto.aug %>% filter(Neighborhood==z)
```

Start inner loop for year

``` r
  tmp.out <- lapply(1:length(year.levs), function(x){
    cat(year.levs[x]," ")
```

#### Calculate the number of condos and coops for a given year

Filter out the dataframe to only buildings which were condo or coop in a given year

-   Filter to type (condo or coop) of building
-   Temporary certificate of occupancy granted in the year of interest or prior
-   DECLARE\_NEWBUILD true indicating building was a condo from the time it was built OR building was converted prior to the year of interest OR Conversion Year is na indicating the conversion was prior to 1980
-   Take sum of the residential units

``` r
condo <- tmp.df.outer %>%
  filter(Type %in% "condo" & TCO <= year.levs[x] & (DECLARE_NEWBUILD==T | ConversionYear <= year.levs[x] | is.na(ConversionYear))) %>%
  summarize(units = sum(UnitsRes,na.rm=T)) %>%
  pull(units)

coop <- tmp.df.outer %>%
  filter(Type %in% "coop" & TCO <= year.levs[x] & (DECLARE_NEWBUILD==T | ConversionYear <= year.levs[x] | is.na(ConversionYear))) %>%
  summarize(units = sum(UnitsRes,na.rm=T)) %>%
  pull(units)

    owned <- condo + coop
```

Rentals are slightly more difficult

-   Temporary certificate of occupancy granted in the year of interest or prior
-   Type of building is currently rental (we presume a building did not go from condo/coop to rental) OR if a building is currently condo/coop
    -   Conversion Year cannot be NA (which would indicate a conversion prior to 1980)
    -   Conversion Year must be after the year of interest
    -   DECLARE\_NEWBUILD must be false indicating the building was not a condo or coop at the time it was built
-   Take sum of residential units

``` r
rentals <- tmp.df.outer %>%
  filter(TCO <= year.levs[x] & 
           (Type %in% "rental" |
              (Type %in% c("condo","coop") & !is.na(ConversionYear) & ConversionYear > year.levs[x] & DECLARE_NEWBUILD==F))
  ) %>%
  summarize(units = sum(UnitsRes,na.rm=T)) %>%
  pull(units)
```

For small type buildings simply filter on the TCO being before or during the year of interest, filter on Type and take the sum of residential units

``` r
small <- tmp.df.outer %>%
  filter(TCO <= year.levs[x] &
           Type %in% "small") %>%
  summarize(units = sum(UnitsRes,na.rm=T)) %>%
  pull(units)
```

End inner loop by returning a vector with (1) year (2) area (3) number of rentals (4) number of coops+condos (5) condos (6) coops (7) small type

``` r
    out <- c(year.levs[x],z,rentals,owned,condo,coop,small)
    return(out)
  }
  )
```

Bind list and set column names. Built in error handling ensuring values are numeric. A vestige of prior versions had an occasional negative value. Keeping the error handling ensuring that negative values are 0.

``` r
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
      ,Condo = ifelse(as.numeric(Condo)<0
                      ,0
                      ,as.numeric(Condo))
      ,Coop = ifelse(as.numeric(Coop)<0
                     ,0
                     ,as.numeric(Coop))
      ,Small = ifelse(as.numeric(Small)<0
                      ,0
                      ,as.numeric(Small))
      ,Total = Rentals + Condo + Coop + Small
    )
```

Return dataframe, end loop and stop cluster

``` r
  cat("\n")
  return(out.df)
}
)

stopCluster(cl)
```

Bind list
Join with the condo rental proportion dataframe
Apply said proportions to get the condo/coop rentals and adjusted condos and coops
Note that for coops we just say about 15% are rented out, this proved to be generally accurate

Rentals.adj are the base level rentals + condo rentals and coop rentals

``` r
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

out.df.nbrhd <- out.df
```

### Borough

This is the same process as neighborhood just using borough as the area

``` r
cl <- makeCluster(detectCores()-2,type="FORK")

tmp.derp <- parLapply(cl,Borough.levs.conversion, function(z){
  tmp.df.outer <- pluto.aug %>% filter(Borough==z)
  
  
  tmp.out <- lapply(1:length(year.levs), function(x){
    cat(year.levs[x]," ")
    condo <- tmp.df.outer %>%
      filter(Type %in% "condo" & TCO <= year.levs[x] & (DECLARE_NEWBUILD==T | ConversionYear <= year.levs[x] | is.na(ConversionYear))) %>%
      summarize(units = sum(UnitsRes,na.rm=T)) %>%
      pull(units)
    
    coop <- tmp.df.outer %>%
      filter(Type %in% "coop" & TCO <= year.levs[x] & (DECLARE_NEWBUILD==T | ConversionYear <= year.levs[x] | is.na(ConversionYear))) %>%
      summarize(units = sum(UnitsRes,na.rm=T)) %>%
      pull(units)
    
    owned <- condo + coop
    
    rentals <- tmp.df.outer %>%
      filter(TCO <= year.levs[x] & 
               (Type %in% "rental" |
                  (Type %in% c("condo","coop") & !is.na(ConversionYear) & ConversionYear > year.levs[x] & DECLARE_NEWBUILD==F))
      ) %>%
      summarize(units = sum(UnitsRes,na.rm=T)) %>%
      pull(units)
    
    small <- tmp.df.outer %>%
      filter(TCO <= year.levs[x] &
               Type %in% "small") %>%
      summarize(units = sum(UnitsRes,na.rm=T)) %>%
      pull(units)
    
    out <- c(year.levs[x],z,rentals,owned,condo,coop,small)
    return(out)
  }
  )
  
  out.df <- as.data.frame(do.call("rbind",tmp.out)
                          ,stringsAsFactors=F)
  
  colnames(out.df) <- c("Year","Borough","Rentals","Owned","Condo","Coop","Small")
  
  out.df <- out.df %>% 
    mutate(
      Rentals = ifelse(as.numeric(Rentals)<0
                       ,0
                       ,as.numeric(Rentals))
      ,Owned = ifelse(as.numeric(Owned)<0
                      ,0
                      ,as.numeric(Owned))
      ,Condo = ifelse(as.numeric(Condo)<0
                      ,0
                      ,as.numeric(Condo))
      ,Coop = ifelse(as.numeric(Coop)<0
                     ,0
                     ,as.numeric(Coop))
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

## bind list
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

out.df.boro <- out.df
```

### NYC

Process for NYC is basically the same but just with the outer loop being eliminated

``` r
tmp.df.outer <- pluto.aug

tmp.out <- lapply(1:length(year.levs), function(x){
  cat(year.levs[x]," ")
  condo <- tmp.df.outer %>%
    filter(Type %in% "condo" & TCO <= year.levs[x] 
           & (DECLARE_NEWBUILD==T | ConversionYear <= year.levs[x] | is.na(ConversionYear))
           ) %>%
    summarize(units = sum(UnitsRes,na.rm=T)) %>%
    pull(units)
  
  coop <- tmp.df.outer %>%
    filter(Type %in% "coop" & TCO <= year.levs[x] 
           & (DECLARE_NEWBUILD==T | ConversionYear <= year.levs[x] | is.na(ConversionYear))
           ) %>%
    summarize(units = sum(UnitsRes,na.rm=T)) %>%
    pull(units)
  
  owned <- condo + coop
  
  rentals <- tmp.df.outer %>%
    filter(TCO <= year.levs[x] & 
             (Type %in% "rental" |
                (Type %in% c("condo","coop") & !is.na(ConversionYear) & ConversionYear > year.levs[x] & DECLARE_NEWBUILD==F))
    ) %>%
    summarize(units = sum(UnitsRes,na.rm=T)) %>%
    pull(units)
  
  small <- tmp.df.outer %>%
    filter(TCO <= year.levs[x] &
             Type %in% "small") %>%
    summarize(units = sum(UnitsRes,na.rm=T)) %>%
    pull(units)
  
  z <- "NYC"
  
  out <- c(year.levs[x],z,rentals,owned,condo,coop,small)
  return(out)
}
)

out.df <- as.data.frame(do.call("rbind",tmp.out)
                        ,stringsAsFactors=F)

colnames(out.df) <- c("Year","AREA","Rentals","Owned","Condo","Coop","Small")

out.df <- out.df %>% 
  mutate(
    Rentals = ifelse(as.numeric(Rentals)<0
                     ,0
                     ,as.numeric(Rentals))
    ,Owned = ifelse(as.numeric(Owned)<0
                    ,0
                    ,as.numeric(Owned))
    ,Condo = ifelse(as.numeric(Condo)<0
                    ,0
                    ,as.numeric(Condo))
    ,Coop = ifelse(as.numeric(Coop)<0
                   ,0
                   ,as.numeric(Coop))
    ,Small = ifelse(as.numeric(Small)<0
                    ,0
                    ,as.numeric(Small))
    ,Total = Rentals + Condo + Coop + Small
  )

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
```

Combine and save to disk
------------------------

``` r
out.df <- bind_rows(out.df.nyc,out.df.boro,out.df.nbrhd) %>% 
  select(Year,AREA,AreaType,Rentals,Condo,Coop,Small,Total,Rentals.adj,Coop.rentals,Condo.rentals,Condo.adj,Coop.adj,Owned.cc) %>% 
  mutate(Rentals.adj = round(Rentals.adj)
         ,Coop.rentals = round(Coop.rentals)
         ,Condo.rentals = round(Condo.rentals)
         ,Coop.adj = round(Coop.adj)
         ,Condo.adj = round(Condo.adj)
         ,Owned.cc = round(Owned.cc)
  )


setwd("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Refresh/data")
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
```

Excel instructions
------------------

After saving to disk a pivot table is created with year along the rows and AREA as a filter. Values for rentals, adjusted rentals, etc are columns.
