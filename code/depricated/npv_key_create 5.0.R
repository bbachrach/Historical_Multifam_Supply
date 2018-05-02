
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


# npv_2018.files <- list.files(path="/Users/billbachrach/Desktop/DOF PDFs/NPV2018")
# setwd("/Users/billbachrach/Desktop/DOF PDFs/NPV2018")
# npv_2018.sizes <- file.size(npv_2018.files)
# 
# npv_2018.tocopy <- npv_2018.files[npv_2018.sizes > 1000]
# 
# old.names <- paste(
#   "/Users/billbachrach/Desktop/DOF PDFs/NPV2018/"
#   ,npv_2018.tocopy
#   ,sep=""
# )
# 
# new.names <- paste(
#   "/Users/billbachrach/Desktop/DOF PDFs/NPV_all/"
#   ,npv_2018.tocopy
#   ,sep=""
# )
# 
# file.copy(
#   old.names
#   ,new.names
#   ,overwrite=T
# )

npv.files <- list.files(path="/Users/billbachrach/Desktop/DOF PDFs/NPV_all")

setwd("/Users/billbachrach/Desktop/DOF PDFs/NPV_all")
npv.sizes <- file.size(npv.files)




# Read in PAD and Pluto ---------------------------------------------------

pluto.all <- readRDS("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Project Data/PLUTO/pluto_lean_compressed_2003_2017.rds")

pluto.all %>%
  summarize(
    count = n()
    ,na_Borough.count = sum(is.na(Borough))
    ,na_BldgClass.count = sum(is.na(BldgClass))
    ,na_BBL.count = sum(is.na(BBL))
    ,na_Block.count = sum(is.na(Block))
    ,na_Lot.count = sum(is.na(Lot))
  )

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
    # borocode = str_sub(BBL,start=1,end=1)
    ## turn block and lot into character format with the appropriate number of leading zeros
    ,Block_char = str_sub(
      paste("0000"
            ,as.numeric(Block)
            ,sep=""
      )
      ,start=-5
      ,end=-1
    )
    ,Lot_char = str_sub(
      paste("0000"
            ,as.numeric(Lot)
            ,sep=""
      )
      ,start=-4
      ,end=-1
    )
    ## DOF 10 character format uses leading zeros 
    ,bbl = paste(
      borocode
      ,Block_char
      ,Lot_char
      ,sep=""
    )
    ,bbl_bill = as.character(bbl)
    ## HWE format transforms them back into numeric (clunky, I know)
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
  # filter(Year >= 2008) %>%
  mutate(Id.pluto = as.character(1:n()))


pad.df <- readRDS("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Project Data/PAD/PAD_with_Condo_BBLs_expanded_2006_2017.rds") %>%
  mutate(Year = as.character(Year)
         ,Id.pad = as.character(1:n())
         )


# Try by starting with pad/pluto join -------------------------------------

# derp <- pad.df %>%
#   select(BBL_bill,BBL,Year,coopnum,condonum) %>%
#   filter(Year >= 2009) %>%
#   mutate(BBL_Year = paste(BBL_bill,Year,sep="_")) %>%
#   left_join(pluto.all %>%
#               select(Borough,BBL_bill,BldgClass,CondoNo,lat,lon,Year) %>%
#               mutate(BBL_Year = paste(BBL_bill,Year,sep="_")) %>%
#               rename(BBL_bill.pluto = BBL_bill)
#             ,by="BBL_Year"
#   )


cl <- makeCluster(round(detectCores()*.75))
registerDoParallel(cl)

year.vec <- 2009:2017

tmp.list <- foreach(i = 1:length(year.vec)
                    , .packages = c("dplyr")) %dopar% {
                      # for(i in 1:length(year.vec)){
                      year.curr <- year.vec[i]
                      pad.tmp <- pad.df %>% filter(Year==year.curr)
                      pluto.tmp <- pluto %>% 
                        filter(Year <= year.curr) %>%
                        arrange(desc(Year)) %>%
                        filter(!duplicated(BBL_bill))
                      
                      out <- pad.tmp %>%
                        # tmp.list[[i]] <- pad.df %>%
                        select(BBL_bill,BBL,Year,coopnum,condonum,Id.pad) %>%
                        left_join(pluto.tmp %>%
                                    select(Borough,BBL_bill,BldgClass,CondoNo,lat,lon,Year,Id.pluto) %>%
                                    rename(Year.pluto = Year)
                                  ,by="BBL_bill"
                        )
                      return(out)
                    }

stopCluster(cl)

# pad.notin <- pad.df %>%
#   anti_join(
#     pluto
#     ,by="BBL_bill"
#   )

pad_pluto.df <- bind_rows(tmp.list)

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

pad_pluto.df.hold <- pad_pluto.df

pad_pluto.df <- bind_rows(
  pad_pluto.df %>%
    anti_join(pad_pluto.miss
              ,by="Id.pad")
  ,pad_pluto.miss
) %>%
  rename(Year.pad = Year)

saveRDS(pad_pluto.df,"/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/PAD/PAD_pluto_join 20180410_1355.rds")

# pad_pluto.df %>%
#   summarize(count = n()
#             ,na.pluto = sum(is.na(Id.pluto))
#             )
# derp <- pad_pluto.df %>%
#        filter(is.na(Id.pluto))
# 
# derp %>% 
#   semi_join(pluto
#             ,by="BBL_bill") %>%
#   summarize(count = n())
# 
# sel <- sample(1:nrow(pad_pluto.df),2500,replace=F)
# View(pad_pluto.df[sel,])
# 
# sel <- sample(1:nrow(out),2500,replace=F)
# View(out[sel,])
# 
# colnames(pad.df)
# colnames(pluto.all)

# create key --------------------------------------------------------------

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


pad_pluto.notin <- pad_pluto.df %>%
  anti_join(
    npv_join.key
    ,by="Id.pad"
  )



# Joining the inverse direction -------------------------------------------

tmp.df <- pad_pluto.df %>%
  left_join(
    npv_join.key %>%
      select(BBL,doc_year,filename,Id.pad)
    ,by="Id.pad"
  ) %>%
  rename(BldgClass.pluto = BldgClass) %>%
  mutate(BldgClass_pluto.broad = str_sub(BldgClass.pluto,start=1,end=1))

saveRDS(tmp.df,"/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Refresh/data/initial_bbl_key 20180410_1438.rds")

object_sizes.fun()
rm(pad.df,tmp.list,pad_pluto.df.hold,npv_join.key.hold,pad_pluto.notin,pluto.tmp,out,cl)
gc()
save.image("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Refresh/data/key_create_workspace 20180410_1445.RData")



# Scrape PDF building classes ---------------------------------------------

source("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Git/DOF_taxbills_scrape/NPV Scrape Individual Parameters/NPV Scrape Functions.R")
setwd("/Users/billbachrach/Desktop/DOF PDFs/NPV_all")

npv.key_small <- npv_join.key %>%
  select(filename,doc_year,BldgClass,BBL,Id.pad)

# filenames <- list.files(pattern=".pdf")

df.breaks <- c(seq(from=1,to=nrow(npv.key_small),by=100000),nrow(npv.key_small))
BldgClass_outer.list <- list()

start.time <- Sys.time()

start <- i
ptm.outer <- proc.time()
for(i in start:length(df.breaks)){
  ptm <- proc.time()
  
  tmp.df <- npv.key_small[df.breaks[i-1]:df.breaks[i],]
  len <- nrow(tmp.df)
  
  cl <- makeCluster(round(detectCores()*.75),type="FORK")
  
  
  BldgClass_outer.list[[i-1]] <- parLapply(cl,1:len, function(x){
    out <- list()
    
    out$BBL <- tmp.df[x,"bbl"]
    out$Id.pad <- tmp.df[x,"Id.pad"]
    out$doc_year <- tmp.df[x,"doc_year"]
    out$filename <- tmp.df[x,"filename"]
    out$BldgClass <- tmp.df[x,"BldgClass"]

    tmp.text <- try(suppressWarnings(pdf_text(tmp.df[x,"filename"])),silent=T)
    
    if(class(tmp.text)=="try-error"){
      out$data <- rep(NA,6)
    }else{
      bldg_class <- bldg_class.fun(tmp.text = tmp.text, doc_year = tmp.df[x,"doc_year"])
      
      ## this is wrong, shoudl be ,tmp.df[x,"bbl]
      ## sorted out in the next lapply but should be corrected
      tmp.out <- cbind(bldg_class
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

for(i in start:length(BldgClass_outer.list)){
  cat("starting iteration ",i,"\n")
  tmp.list <- BldgClass_outer.list[[i]]
  names(tmp.list) <- unlist(lapply(tmp.list, function(x) x$filename))
  
  tmp.list.drops <- which(unlist(lapply(tmp.list, function(x) class(x$data)!="matrix")))
  
  tmp.list[not_complete] <- NULL
  
  cat("Unlisting\n")
  cl <- makeCluster(detectCores()-2,type="FORK")
  tmp.list <- parLapply(cl,tmp.list, function(x) x$data %>% as.data.frame(.,stringsAsFactors=F))
  stopCluster(cl)

  cat("Binding\n")
  tmp.df <- bind_rows(tmp.list)[,1:6] %>%
    setNames(c("BldgClass","BBL","Id.pad","doc_year","BldgClass.pluto","filename")) %>%
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

saveRDS(BldgClass.df,"/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Refresh/data/BuildingClass_df 20180410_1754.rds")


# Joining with pad_pluto --------------------------------------------------

pad_pluto.df.hold <- pad_pluto.df

pad_pluto.df <- pad_pluto.df %>%
  mutate(BBL_Year = paste(BBL,Year.pad,sep="_")) %>%
  left_join(BldgClass.df %>%
              select(BldgClass,BBL_Year) %>%
              rename(BldgClass_pdf = BldgClass)
            ,by="BBL_Year"
            )


# Exploring ---------------------------------------------------------------

pad_pluto.df %>%
  filter(Year.pad == 2017) %>%
  summarize(count = n()
            ,bldgclass_pdf_miss.count = sum(is.na(BldgClass_pdf))
            ,bldgclass_pluto_miss.count = sum(is.na(BldgClass))
  )


pp_2017.df <- pad_pluto.df %>%
  filter(Year.pad == 2017)

pp_miss.df <- pp_2017.df %>%
  filter(is.na(BldgClass_pdf))

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

  pp_miss.df <- pp_miss.df %>%
    anti_join(
      out.df
      ,by="BBL"
    )
  
  out_df.list[[i]] <- out.df
}

pp_found.df <- bind_rows(out_df.list)

pp_2017.df <- pp_2017.df %>%
  left_join(pp_found.df %>%
              select(BBL,BldgClass_pdf) %>%
              rename(BldgClass_pdf.aux = BldgClass_pdf)
            ,by="BBL"
  ) %>%
  mutate(BldgClass_pdf = ifelse(is.na(BldgClass_pdf) & !is.na(BldgClass_pdf.aux)
                                ,BldgClass_pdf.aux
                                ,BldgClass_pdf
  )
  ) %>%
  select(-BldgClass_pdf.aux)

pp_2017.df <- pp_2017.df %>%
  rename(BldgClass_bill = BldgClass)


pp_group.df <- pp_2017.df %>%
  filter(!is.na(BldgClass_pdf)) %>%
  group_by(BBL_bill,BldgClass_pdf) %>%
  summarize(count = n()) %>%
  ungroup() %>%
  group_by(BBL_bill) %>%
  arrange(desc(count)) %>%
  mutate(order = 1:n()) %>%
  ungroup() %>%
  rename(BBL = BBL_bill)


# Join with pluto 2017 ----------------------------------------------------

pluto_2017 <- readRDS("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Project Data/PLUTO/pluto_lean_compressed_2003_2017.rds") %>%
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


pluto_2017.hold <- pluto_2017

pluto_2017 <- pluto_2017 %>%
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


tmp.classes <- c(
  pluto_2017 %>% pull(BldgClass_Internal.1)
  ,pluto_2017 %>% pull(BldgClass_Internal.2)
  ,pluto_2017 %>% pull(BldgClass_Internal.3)
)

tmp.classes <- tmp.classes[!duplicated(tmp.classes)]
tmp.classes <- tmp.classes[order(tmp.classes)]


small.classes <- c("A0","A1","A2","A3","A4","A5","A6","A7","A9","B1","B2","B3","B9","S1","S2")
rental.classes <- c("C0","C1","C2","C3","C4","C5","C7","C9","D1","D2","D3","D5","D6","D7","D8","D9","RR","S4","S5","S9")
condo.classes <- c("R1","R2","R3","R4","R6","R8")
coop.classes <- c("C6","C8","D0","D4","R9")
other.classes <- c("CM","R5","R7","RB","RG","RH","RK","RP","RS","RT","RA","RW")


pp_group.df %>%
  ungroup() %>%
  group_by(BBL_bill) %>%
  summarize(count = n()) %>%
  ungroup() %>%
  summarize(min.count = min(count)
            ,perc_0.25.count = quantile(count,probs=.25)
            ,mean.count = mean(count)
            ,med.count = median(count)
            ,perc_0.75.count = quantile(count,probs=.75)
            ,max.count = max(count)
            
            ,count_2more = sum(count>2)
            ,prop_2more = count_2more/n()
            ,count_3more = sum(count>3)
            ,prop_3more = count_3more/n()
            )

# Classifying -------------------------------------------------------------

small.classes <- c("A0","A1","A2","A3","A4","A5","A6","A7","A9","B1","B2","B3","B9","S1","S2")
rental.classes <- c("C0","C1","C2","C3","C4","C5","C7","C9","D1","D2","D3","D5","D6","D7","D8","D9","RR","S4","S5","S9")
condo.classes <- c("R0","R1","R2","R3","R4","R6","R8")
coop.classes <- c("C6","C8","D0","D4","R9")
other.classes <- c("CM","R5","R7","RB","RG","RH","RK","RP","RS","RT","RA","RW")


condocoop.df <- pluto_2017 %>%
  # filter(UnitsRes>0) %>%
  mutate(Type = ifelse((BldgClass %in% condo.classes | BldgClass_Internal.1 %in% condo.classes)
                       ,"condo"
                       ,ifelse((BldgClass %in% coop.classes | BldgClass_Internal.1 %in% coop.classes)
                               ,"coop"
                               ,ifelse(((BldgClass %in% rental.classes | BldgClass_Internal.1 %in% rental.classes) & UnitsRes>=3)
                                       ,"rental"
                                       ,ifelse((BldgClass %in% small.classes | BldgClass_Internal.1 %in% small.classes | UnitsRes < 3)
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
  select(BBL,Address,BldgClass,BldgClass_Internal.1,ZipCode,lon,lat,Type)


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



saveRDS(condocoop.df,"/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Refresh/data/condocoop_df 20180418_1500.rds")
View(condocoop.df[1:1000,])

condocoop.df %>%
  summarize(count = sum(UnitsRes>=3)
            ,unit.count = sum(UnitsRes[UnitsRes>=3])
            
            ,condo.count = sum(Type == "condo")
            ,condo_unit.count = sum(UnitsRes[Type=="condo"])
            
            ,coop.count = sum(Type == "coop")
            ,coop_unit.count = sum(UnitsRes[Type=="coop"])
            
            ,rental.count = sum(Type == "rental")
            ,rental_unit.count = sum(UnitsRes[Type=="rental"])
            
            # ,na.count = sum(is.na(Type))
            # ,na_unit.count = sum(UnitsRes[is.na(Type)])
  )

# condocoop_old.df <- readRDS("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/UWS condo prop/Data/condo_coop_plutojoined_df.rds")

condocoop_old.df %>%
  summarize(count = sum(UnitsRes>=3,na.rm=T)
            ,unit.count = sum(UnitsRes[UnitsRes>=3],na.rm=T)
            
            ,condo.count = sum(Type == "condo")
            ,condo_unit.count = sum(UnitsRes[Type=="condo"])
            
            ,coop.count = sum(Type == "coop")
            ,coop_unit.count = sum(UnitsRes[Type=="coop"],na.rm=T)
            
            ,rental.count = sum(Type == "all" & UnitsRes>=3)
            ,rental_unit.count = sum(UnitsRes[Type=="all" & UnitsRes>=3])
  )




coop.classes <- c(
  "C6"
  ,"C8"
  ,"D0"
  ,"D4"
  ,"R9"
)

condo.classes <- c(
  "R0"
  ,"R1"
  ,"R3"
  ,"R4"
  ,"R6"
  ,"R7"
)

rental.classes <- c(
  "C0"
  ,"C1"
  ,"C2"
  ,"C3"
  ,"C4"
  ,"C5"
  ,"C7"
  ,"C9"
  ,"D1"
  ,"D2"
  ,"D3"
  ,"D5"
  ,"D6"
  ,"D7"
  ,"D8"
  ,"D9"
  ,"RR"
)

classi.df <- pp_2017.df %>%
  filter(!duplicated(BBL)) %>%
  group_by(BBL_bill) %>%
  summarize(
    count = n()
    ,condo.count = sum(BldgClass_pdf %in% condo.classes)
    ,coop.count = sum(BldgClass_pdf %in% coop.classes)
    ,rental.count = sum(BldgClass_pdf %in% rental.classes)
  ) %>%
  left_join(pp_2017.df %>%
              filter(!duplicated(BBL_bill)) %>%
              select(-BldgClass_pdf)
            # %>%
            #   select(BBL_bill,BldgClass_bill,condoflag,lot4dig.flag,UnitsRes,UnitsTotal,YearBuilt)
  ) %>%
  mutate(BldgClass_bill.broad = str_sub(BldgClass_bill,start=1,end=1)
         ,randomid = sample(1:n(),n(),replace=F)
  )


condocoop.df <- readRDS("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/UWS condo prop/Data/condo_coop_plutojoined_df.rds")


object_sizes.fun()

save.image("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Refresh/data/key_create_workspace 20180418_1503.RData")
# load("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Refresh/data/key_create_workspace 20180416_1903.RData")

# pp_2017.df %>%
#   group_by(BldgClass) %>%
#   summarize(count = n()
#             ,bldgclass_pdf_miss.count = sum(is.na(BldgClass_pdf))
#             # ,bldgclass_pluto_miss.count = sum(is.na(BldgClass))
#   )

# colnames(pp_found.df)
# View(
#   pp_2017.df %>%
#   # pad_pluto.df %>%
#   #   filter(Year.pad == 2017) %>%
#     group_by(BldgClass) %>%
#     summarize(
#       count = n()
#       ,bldgclass_pdf_miss.count = sum(is.na(BldgClass_pdf))
#       ,bldgclass_miss.prop = bldgclass_pdf_miss.count/count
#     ) %>%
#     arrange(desc(bldgclass_pdf_miss.count))
# )