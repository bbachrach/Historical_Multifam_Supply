
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
# load("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/taxbills/Data/Workspaces/gsf_scrape_workspace_20170911_1133.RData")

library(dplyr)
library(parallel)
library(lubridate)
library(stringr)
library(pdftools)
options(scipen=999)

source("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/useful functions/HWE_FUNCTIONS.r")
source("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/useful functions/-.R")
source("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/useful functions/useful minor functions.R")
source("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/useful functions/hwe colors.R")



# Read in and mutate pluto dataframe containing all versions ---------------------------------

## NOTE: it's necessary to create a bbl in the format used in filenames and by DOF rather than our easily read format
## BBL refers to our internal format bbl refers to the 10 character DOF format
## I put in fields for billing BBL and bbl here which are the same as regular ol' BBL to make linking with PAD easier
## some can be dropped out in the future to make the code more lean

pluto.all <- readRDS("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Project Data/PLUTO/pluto_all_compressed_2003_2016.rds")
pluto.all.hold <- pluto.all

pluto.all <- pluto.all.hold %>% 
  filter(!(is.na(Borough) | is.na(Block) | is.na(Lot) | is.na(BldgClass)))%>% 
  select(Borough,Block,Lot,ZipCode,Address,BldgClass
         ,BldgArea,ComArea,ResArea,OfficeArea,RetailArea,GarageArea,StrgeArea
         ,FactryArea,OtherArea,AreaSource,NumBldgs,NumFloors,UnitsRes,UnitsTotal,YearBuilt
         ,CondoNo,XCoord,YCoord,Year,Version,DOBDate,lat,lon,neighborhood) %>% 
  filter(Year >= 2010) %>% 
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
                                                ,NA
                                              )
                                      )
                              )
                      )
    )
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
  ungroup()


# saveRDS(pluto.all,"/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/taxbills/pluto_all.rds")
# pluto.all <- readRDS("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/taxbills/pluto_all.rds") %>%
#   filter(!(is.na(BBL) | grepl("[[:alpha:]]",BBL)))


## use code chunk (1) to inspect the initial pluto dataframe


# Read in and mutate PAD --------------------------------------------------

pad_expand <- readRDS("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Project Data/PAD/PAD_Condo_BBLs_expanded_v002.rds")
pad_expand.hold <- pad_expand

## NOTE: found out "new_lot" is the expanded field, use that to get the expanded BBLs
## ALSO NOTE: found out "new_lot" is numeric and thus needs to be manipulated before putting into bbl

pad_expand <- pad_expand.hold %>% 
  filter(is.na(billlot) 
         | nchar(billlot)==4 
         & as.numeric(billlot) > 0
         & !(!is.na(condoflag)
             & is.na(billlot)
         )
  ) %>% 
  mutate(
    lot = as.character(str_sub(
      paste("0000"
            ,gsub(".*_","",
                  as.character(lot)
            )
            ,sep=""
      )
      ,start=-4
      ,end=-1
    )
    )
    ,new_lot = as.character(str_sub(
      paste("0000"
            ,gsub(".*_","",
                  as.character(new_lot)
            )
            ,sep=""
      )
      ,start=-4
      ,end=-1
    )
    )
    # ,billlot = ifelse(!is.na(billlot)
    ,billlot = ifelse(!is.na(billboro)
                      ,as.character(str_sub(
                        paste("0000"
                              ,gsub(".*_","",
                                    as.character(billlot)
                              )
                              ,sep=""
                        )
                        ,start=-4
                        ,end=-1
                      )
                      )
                      ,NA
    )
    ,bbl_bill = ifelse(!is.na(billboro)
                       ,paste(
                         billboro
                         ,billblock
                         ,billlot
                         ,sep=""
                       )
                       ,NA
    )
    ,bbl = paste(
      boro
      ,block
      ,new_lot
      ,sep=""
    )
    ,BBL_bill = ifelse(!is.na(billboro)
                       ,paste(
                         as.numeric(billboro)
                         ,as.numeric(billblock)
                         ,as.numeric(billlot)
                         ,sep="_"
                       )
                       ,NA
    )
    ,BBL = paste(
      as.numeric(boro)
      ,as.numeric(block)
      ,as.numeric(new_lot)
      ,sep="_"
    )
  )

# saveRDS(pad_expand,"/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/taxbills/pad_expand.rds")
# pad_expand <- readRDS("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/taxbills/pad_expand.rds")

## use code chunk (2) to inspect initial PAD dataframe and code chunk (3) to inspect the munged version
## separate diagnostics were immensely helpful in troubleshootin

## Only include bbls from pad that have billing lots associated with them
## Idea is to link the condo bbls with their billing bbl so we don't need PAD rows that aren't condos
pad_expand.onlybill <- pad_expand %>% 
  filter(grepl("[[:digit:]]",BBL_bill) & !is.na(BBL_bill)) %>%
  mutate(Year = 2016) %>%
  select(bbl_bill,bbl,BBL_bill,BBL,condoflag,numaddr,Year)

pad_expand.onlybill <- semi_join(
  pad_expand %>% 
    filter(grepl("[[:digit:]]",BBL_bill) & !is.na(BBL_bill)) %>%
    mutate(Year = 2016) %>%
    select(bbl_bill,bbl,BBL_bill,BBL,condoflag,numaddr,Year)
  ,pluto.all %>% 
    arrange(desc(Year)) %>% 
    filter(!duplicated(BBL_bill))
  ,by="BBL_bill"
)

## use code chunks (4) to verify this worked correctly

## Joining PAD with pluto and then appending to pluto

pad_expand.joined <- left_join(
  pad_expand.onlybill %>% 
    select(-Year)
  ,pluto.all %>% 
    filter(!is.na(BBL_bill)) %>% 
    arrange(desc(Year)) %>% 
    filter(!duplicated(BBL_bill)) %>% 
    select(-bbl,-bbl_bill,-BBL)
  ,by="BBL_bill"
)

pluto.all.hold <- pluto.all
# pluto.all <- pluto.all.hold
pluto.all <- bind_rows(
  pad_expand.joined
  ,pluto.all.hold
) %>% 
  mutate(doc_year = as.character(Year))

## use code chunk (@5) to inspect the appended pluto dataframe



# Dataframe with names and information from PDFs --------------------------

npv.files <- list.files(path="/Users/billbachrach/Desktop/DOF PDFs/NPV_all")
# npv.files <- readRDS("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/taxbills/local_npv_filenames.rds")

npv.bbls <- gsub("[^[:digit:]]",""
                 ,gsub("NPV.*","",npv.files)
)
npv.dates <- gsub("[^[:digit:]]",""
                  ,gsub(".*NPV","",npv.files)
)

npv.key <- as.data.frame(cbind(
  npv.bbls
  ,npv.dates
  ,npv.files
)
,stringsAsFactors=F
)

colnames(npv.key) <- c("bbl","doc_date","filename")

npv.key <- npv.key %>% 
  mutate(doc_date = parse_date_time(doc_date
                                    ,"Y!m!d!")
         ,doc_year = as.character(year(doc_date))
  )

## use code chunk (@6) to verify. the filename dataframe is pretty simple so it never caused any problems

# saveRDS(npv.key,"/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/taxbills/npv_key.rds")
# save.image("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/taxbills/npv_key_create_base_workspace.RData")
# npv.key.saved <- readRDS("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/taxbills/Data/npv_key_create/npv_key_20170831_1513.rds")

# Join filename df with pluto ---------------------------------------------

derp <- semi_join(
  pluto.all
  ,pluto.all %>% 
    filter(
      duplicated(paste(bbl,doc_year,by="_"))
    ) %>% 
    select(bbl,doc_year)
  ,by=c("bbl","doc_year")
) %>% 
  filter(is.na(condoflag))


pluto.all <- anti_join(pluto.all
                       ,derp %>% 
                         select(bbl,doc_year,condoflag)
                       ,by=c("bbl","doc_year","condoflag"))

# View(
#   as.data.frame(derp %>% 
#     select(BBL,BBL_bill,doc_year,BldgClass,Version,UnitsRes,everything()) %>% 
#       arrange(BBL,desc(doc_year))
#     ,stringsAsFactors=F)[1:1000,]
# )

## all fields in pluto
npv.key.joined <- left_join(
  npv.key
  ,pluto.all %>% 
    arrange(desc(Year))
    # filter(!duplicated(paste(bbl,doc_year,by="_"))) %>% 
    # arrange(is.na(condoflag)) %>% 
  ,by = c("bbl","doc_year")
) %>% 
  filter(!is.na(BldgClass))

npv.key.joined2 <- left_join(
  npv.key
  ,pluto.all %>% 
    filter(!is.na(BldgClass)) %>% 
    arrange(desc(doc_year)) %>% 
    filter(!duplicated(bbl)) %>% 
    select(-doc_year)
  ,by = "bbl"
)

# npv.key.joined.hold <- npv.key.joined
# npv.key.joined <- npv.key.joined.hold

npv.key.joined <- bind_rows(
  npv.key.joined
  ,anti_join(npv.key.joined2
             ,npv.key.joined
             ,by=c("bbl","doc_year")
  )
) %>% 
  mutate(
  BldgClass.broad = str_sub(BldgClass,start=1,end=1)
  ,lot4dig.flag = str_sub(bbl,start=-4,end=-4) != 0
)


## just the fields that might be helpful
npv.key.joined.slim <- npv.key.joined %>%
  select(filename,doc_year,doc_date,BldgClass.broad,BldgClass,lot4dig.flag,condoflag
         ,bbl,bbl_bill,BBL,BBL_bill,CondoNo
         ,Borough,Address,ZipCode,XCoord,YCoord,lat,lon
         ,UnitsRes,UnitsTotal,NumBldgs,YearBuilt,BldgArea,ResArea
         ,Year,Version
         ) %>% 
  rename(pluto_version_year = Year
         ,pluto_version = Version)

## save to disk
# saveRDS(npv.key.joined
#         ,paste("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/taxbills/Data/npv_key_create/npv_key_full_"
#                ,format(
#                  Sys.time()
#                  ,"%Y%m%d_%H%M"
#                )
#                ,".rds"
#                ,sep=""
#         )
# )
# 

saveRDS(npv.key.joined.slim
        ,paste("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/taxbills/Data/npv_key_create/npv_key_"
               ,format(
                 Sys.time()
                 ,"%Y%m%d_%H%M"
               )
               ,".rds"
               ,sep=""
        )
)

# saveRDS(npv.key.joined.slim,"/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/taxbills/Data/npv_key_create/npv_key_20170919_2000.rds")

# Get building classes in pdfs --------------------------------------------
# list.files(path="/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/taxbills/Data/npv_key_create/"
#            ,pattern="npv_key_")

npv.key <- readRDS("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/taxbills/Data/npv_key_create/npv_key_20170919_2025.rds")

npv.key_small <- npv.key[,c("bbl","doc_year","filename","BldgClass")]
# npv.key_small <- npv.key.joined.slim[,c("bbl","doc_year","filename","BldgClass")]

## at this point it would be prudent to remove evertying except for npv.key and npv.key_small, bout to use a lot of memory
## honestly tho, should probably drop npv.key and then bring it back in later for the join, this is going to crush memory resources
rm(list=ls()[!ls() %in% "npv.key_small"])
gc()

source("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/taxbills/Code/NPV Scrape Functions v2.3.4.R")
setwd("/Users/billbachrach/Desktop/DOF PDFs/NPV_all")

filenames <- list.files(pattern=".pdf")

df.breaks <- c(seq(from=1,to=nrow(npv.key_small),by=100000),nrow(npv.key_small))
BldgClass_outer.list <- list()

start.time <- Sys.time()

ptm.outer <- proc.time()
for(i in 2:length(df.breaks)){
  ptm <- proc.time()
  
  tmp.df <- npv.key_small[df.breaks[i-1]:df.breaks[i],]
  len <- nrow(tmp.df)
  
  cl <- makeCluster(detectCores()-1,type="FORK")
  
  
  BldgClass_outer.list[[i-1]] <- parLapply(cl,1:len, function(x){
    out <- list()
    
    out$BBL <- tmp.df[x,"bbl"]
    out$doc_year <- tmp.df[x,"doc_year"]
    out$filename <- tmp.df[x,"filename"]
    out$BldgClass <- tmp.df[x,"BldgClass"]
    
    # tmp.text <- try(suppressWarnings(pdf_text(tmp.df[x,"filename"])),silent=T)
    tmp.text <- try(suppressWarnings(pdf_text(tmp.df[x,"filename"])),silent=T)
    
    if(class(tmp.text)=="try-error"){
      out$data <- rep(NA,5)
    }else{
      # gross_inc <- gross_inc.fun(tmp.text)
      # 
      # if(tmp.df[x,"BldgClass"] %in% condo.int){
      #   rent <- rent.fun(tmp.text)
      # }else{
      #   rent <- c(NA,NA)
      # }   
      
      bldg_class <- bldg_class.fun(tmp.text = tmp.text, doc_year = tmp.df[x,"doc_year"])
      
      ## this is wrong, shoudl be ,tmp.df[x,"bbl]
      ## sorted out in the next lapply but should be corrected
      tmp.out <- cbind(bldg_class
                       ,tmp.df[x,"BBL"]
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


BldgClass_df.list <- lapply(BldgClass_outer.list, function(tmp.list){
  names(tmp.list) <- unlist(lapply(tmp.list, function(x) x$filename))
  
  tmp.list.lengths <- unlist(lapply(tmp.list, function(x) length(x$data)))
  not_complete <- which(tmp.list.lengths!=4)
  # length(not_complete)
  
  tmp.list[not_complete] <- NULL
  
  tmp.bbls <- unlist(lapply(tmp.list, function(x) x$BBL))
  
  tmp.df <- as.data.frame(do.call("rbind",
                                  lapply(tmp.list, function(x) x$data)
  )
  ,stringsAsFactors=F
  )
  
  colnames(tmp.df) <- c("BldgClass","doc_year","BldgClass.pluto","filename")
  tmp.df[,"bbl"] <- tmp.bbls
  
  return(tmp.df)
}
)

BldgClass.df <- do.call("rbind",BldgClass_df.list)


npv.key <- readRDS("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/taxbills/Data/npv_key_create/npv_key_20170919_2025.rds")
# npv.key <- readRDS("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/taxbills/Data/npv_key_create/npv_key_20170911_1904.rds")
npv.key.hold <- npv.key
# npv.key <- npv.key.hold
# colnames(npv.key)[
#   colnames(npv.key) %in%
#     colnames(BldgClass.df)
#   ]

npv.key <- left_join(npv.key
                     ,BldgClass.df %>% 
                       filter(!duplicated(filename)) %>%
                       select(BldgClass,filename) %>% 
                       rename(BldgClass_pdf = BldgClass)
                     ,by="filename") %>%
  rename(BldgClass_bill = BldgClass)

## save them fuckers to disk
saveRDS(npv.key
        ,paste("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/taxbills/Data/npv_key_create/npv_key_"
               ,format(
                 Sys.time()
                 ,"%Y%m%d_%H%M"
               )
               ,".rds"
               ,sep=""
        )
)


# Create dataframe of BBLs we want but don’t have -------------------------

pluto.restr <- pluto.all %>% 
  filter(Year >= 2009 & !duplicated(bbl)) %>% 
  mutate(BldgClass.broad = str_sub(BldgClass,start=1,end=1)) %>%
  filter(BldgClass.broad %in% c("D","O","H","C","S") | 
           BldgClass %in% c("RR","RM"))

pluto.restr.notin <- anti_join(
  pluto.restr
  ,npv.key
  ,by="bbl"
)

NPVsweep3.bbls <- pluto.restr.notin %>% 
  select(bbl,bbl_bill,BBL,BBL_bill,condoflag,Address,CondoNo,BldgClass.broad,BldgClass,UnitsRes)


saveRDS(
  NPVsweep3.bbls
  ,paste(
    "/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/taxbills/Data/Target BBls/NPV_Sweep3_"
    ,format(
      Sys.time()
      ,"%Y%m%d_%H%M"
    )
    ,".rds"
    ,sep=""
  )
)



### EOF


# Code chunks used for testing integrity of dataframes --------------------

##########
## (@1) Initial inspection of the larger pluto dataframe
# pluto.all %>%
#   summarize(
#     na_BBL = sum(is.na(BBL) | grepl("[[:alpha:]]",BBL))
#     ,na_BBL_bill = sum(is.na(BBL_bill) | grepl("[[:alpha:]]",BBL_bill))
#     ,na_bbl = sum(is.na(bbl) | grepl("[[:alpha:]]",bbl))
#     ,na_bbl_bill = sum(is.na(bbl_bill) | grepl("[[:alpha:]]",bbl_bill))
#     ,na_Year = sum(is.na(Year))
# ,na_lot = sum(is.na(Lot))
# ,na_numer_block = sum(is.na(as.numeric(Block)))
# ,na_numer_lot = sum(is.na(as.numeric(Lot)))                          
# ,max_char_block = max(nchar(as.character(Block)),na.rm=T)
# ,max_char_lot = max(nchar(as.character(Lot)),na.rm=T)
# )

# pluto.all %>% 
#   summarize(
#     na_boro = sum(is.na(Borough))
#     ,na_block = sum(is.na(Block))
#     ,na_lot = sum(is.na(Lot))
#     ,na_numer_block = sum(is.na(as.numeric(Block)))
#     ,na_numer_lot = sum(is.na(as.numeric(Lot)))                          
#     ,max_char_block = max(nchar(as.character(Block)),na.rm=T)
#     ,max_char_lot = max(nchar(as.character(Lot)),na.rm=T)
#   )

##########
## (@2) checking basic characteristics of initial PAD file
# View(
#   t(
#     pad_expand.hold %>%
#       summarize(
#         count = n()
#         
#         ,condo_nobill = sum(
#           !is.na(condoflag)
#           & is.na(billlot)
#         )
#         
#         ,BORO_SECTION = "BORO SECTION" 
#         
#         ,boro.class = class(boro)
#         ,na.boro = sum(is.na(boro))
#         
#         ,new_lot.class = class(new_lot)
#         ,na.new_lot = sum(is.na(new_lot))
#         ,minchar.new_lot = min(nchar(new_lot))
#         ,maxchar.new_lot = max(nchar(new_lot))
#         ,wrongchar.new_lot = sum(nchar(new_lot)!=4,na.rm=T)
#         
#         ,block.class = class(block)
#         ,na.block = sum(is.na(block))
#         ,minchar.block = min(nchar(block))
#         ,maxchar.block = max(nchar(block))
#         ,wrongchar.block = sum(nchar(block)!=5,na.rm=T)
#         
#         ,billboro.class = class(billboro)
#         ,na.billboro = sum(is.na(billboro))
#         
#         ,billlot.class = class(billlot)
#         ,na.billlot = sum(is.na(billlot),na.rm=T)
#         ,minchar.billlot = min(nchar(billlot),na.rm=T)
#         ,maxchar.billlot = max(nchar(billlot),na.rm=T)
#         ,wrongchar.billlot = sum(nchar(billlot)!=4,na.rm=T)
#         
#         ,billblock.class = class(billblock)
#         ,na.billblock = sum(is.na(billblock),na.rm=T)
#         ,minchar.billblock = min(nchar(billblock),na.rm=T)
#         ,maxchar.billblock = max(nchar(billblock),na.rm=T)
#         ,wrongchar.billblock = sum(nchar(billblock)!=5,na.rm=T)
#         
#         ,fucked.new_bbl = sum(
#           !grepl("[[:digit:]]",new_bbl)
#           | is.na(new_bbl)
#           | (grepl("[^[:digit:]]",new_bbl) & !grepl("_",new_bbl))
#         )
#       )
#   )
# )

# View(
#   t(
#     pad_expand.hold %>% 
#       summarize_each(funs(class))
#   )
# )

# View(
#   pad_expand.hold %>% 
#     filter(!is.na(billlot) &
#              nchar(billlot)!=4
#     )
# )

# View(
#   pad_expand.hold %>%
#     filter(!is.na(condoflag)
#             & is.na(billlot)
#             )
# )

##########
## (3) integrity checking for pad_expand after manipulation
# View(
#   t(
#     pad_expand %>%
#       summarize(
#         count = n()
#         # ,bbl.class = class(bbl)
#         ,fucked.BBL_bill = sum(
#           (!grepl("[[:digit:]]",BBL_bill)
#            | (grepl("[^[:digit:]]",BBL_bill) & !grepl("_",BBL_bill)))
#           & !is.na(BBL_bill)
#         )
#         ,fucked.BBL = sum(
#           !grepl("[[:digit:]]",BBL)
#           | is.na(BBL)
#           | (grepl("[^[:digit:]]",BBL) & !grepl("_",BBL))
#         )
#         ,fucked.bbl_bill = sum(
#           (grepl("[^[:digit:]]",bbl_bill)
#            | nchar(bbl_bill) != 10)
#           & !is.na(bbl_bill)
#         )
#         ,fucked.bbl = sum(
#           grepl("[^[:digit:]]",bbl)
#           | nchar(bbl) != 10
#           | is.na(bbl)
#         )
#         ,minchar.bbl = min(nchar(bbl))
#         ,maxchar.bbl = max(nchar(bbl))
#         ,wrongchar.bbl = sum(nchar(bbl)!=10,na.rm=T)
# 
#         ,minchar.bbl_bill = min(nchar(bbl_bill),na.rm=T)
#         ,maxchar.bbl_bill = max(nchar(bbl_bill),na.rm=T)
#         ,wrongchar.bbl_bill = sum(nchar(bbl_bill)!=10,na.rm=T)
# 
#         ,minchar.billblock = min(nchar(billblock),na.rm=T)
#         ,maxchar.billblock = max(nchar(billblock),na.rm=T)
#         ,wrongchar.billblock = sum(nchar(billblock)!=5,na.rm=T)
# 
#         ,minchar.billlot = min(nchar(billlot),na.rm=T)
#         ,maxchar.billlot = max(nchar(billlot),na.rm=T)
#         ,wrongchar.billlot = sum(nchar(billlot)!=4,na.rm=T)
# 
#         ,notmatch.BBL = sum(new_bbl != BBL)
#       )
#   )
# )

# View(
#   t(
#     pad_expand %>% 
#       summarize_each(funs(class))
#   )
# )

##########
## (@4) Inspecting output from restricting PAD to only condo BBLs

# pad_expand.onlybill %>% 
#   summarize(sum(is.na(BBL_bill)))

# peob.notin %>% 
#   summarize(BBL_bill.unique = sum(!duplicated(BBL_bill))
#             ,BBL.unique = sum(!duplicated(BBL))
#   )

# sel <- sample(1:nrow(peob.notin),1000,replace=F)
# View(peob.notin[sel,])


######
## (@5) checking integrity of appended pluto dataframe
# View(
#   t(
#     pluto.all %>%
#       summarize(
#         count = n()
# 
#         ,BBL_SECTION = "---------------------"
#         ,BBL.class = class(BBL)
#         ,BBL.unique = sum(!duplicated(BBL))
#         ,BBL.na = sum(is.na(BBL))
#         ,BBL.fucked= sum(
#           !grepl("[[:digit:]]",BBL)
#           | is.na(BBL)
#           | (grepl("[^[:digit:]]",BBL) & !grepl("_",BBL))
#         )
# 
#         ,BBL_bill_SECTION = "---------------------"
#         ,BBL_bill.class = class(BBL_bill)
#         ,BBL_bill.unique = sum(!duplicated(BBL_bill))
#         ,BBL_bill.na = sum(is.na(BBL_bill))
#         ,BBL_bill.fucked= sum(
#           !grepl("[[:digit:]]",BBL_bill)
#           | is.na(BBL_bill)
#           | (grepl("[^[:digit:]]",BBL_bill) & !grepl("_",BBL_bill))
#         )
# 
#         ,bbl_SECTION = "---------------------"
#         ,bbl.class = class(bbl)
#         ,bbl.unique = sum(!duplicated(bbl))
#         ,bbl.na = sum(is.na(bbl))
#         ,fucked.bbl = sum(
#           grepl("[^[:digit:]]",bbl)
#           | nchar(bbl) != 10
#           | is.na(bbl)
#         )
# 
#         ,bbl_bill_SECTION = "---------------------"
#         ,bbl_bill.class = class(bbl_bill)
#         ,bbl_bill.unique = sum(!duplicated(bbl_bill))
#         ,bbl_bill.na = sum(is.na(bbl_bill))
#         ,bbl_bill.fucked = sum(
#           (grepl("[^[:digit:]]",bbl_bill)
#            | nchar(bbl_bill) != 10)
#           & !is.na(bbl_bill)
#         )
# 
#         ,BldgClass_SECTION = "---------------------"
#         ,BldgClass.class = class(BldgClass)
#         ,BldgClass.unique = sum(!duplicated(BldgClass))
#         ,BldgClass.na = sum(is.na(BldgClass))
#         ,BldgClass.fucked = sum(
#           nchar(BldgClass) != 2
#           | is.na(BldgClass)
#         )
# 
# 
#         ,Year_SECTION = "---------------------"
#         ,Year.class = class(Year)
#         ,Year.unique = sum(!duplicated(Year))
#         ,Year.na = sum(is.na(Year))
#         ,Year.min = ifelse(class(Year)!="character"
#                            ,min(Year,na.rm=T)
#                            ,NA)
#         ,Year.max = ifelse(class(Year)!="character"
#                            ,max(Year,na.rm=T)
#                            ,NA)
#       )
#   )
# )

##(@6)  inspecting NPV Key
# View(
#   t(
#     npv.key %>%
#       summarize(
#         count = n()
# 
#         ,bbl_SECTION = "--------------"
#         ,unique.bbls = sum(!duplicated(bbl))
#         ,bbl.na = sum(is.na(bbl))
#         ,bbl.max.nchar = max(nchar(bbl))
#         ,bbl.min.nchar = min(nchar(bbl))
#         ,bbl.condos = sum(
#           !duplicated(bbl) &
#             str_sub(bbl,start=-4,end=-4) != 0
#         )
#         ,bbl.75lot = sum(
#           !duplicated(bbl) &
#             str_sub(bbl,start=-4,end=-4) == 75
#         )
#         ,bbl.wrong_boro = sum(
#           !duplicated(bbl) &
#             !str_sub(bbl,start=1,end=1) %in% c(1,2,3,4,5)
#         )
#         ,bbl.wrong_checkdigit = sum(
#           str_sub(bbl,start=2,end=2) != 0
#         )
# 
#         ,filename_SECTION = "--------------------"
#         ,filename.dupes = sum(duplicated(filename))
#         ,filename.na = sum(is.na(filename))
#         ,filename.maxchar = max(nchar(filename))
#         ,filename.minchar = min(nchar(filename))
#       )
#   )
# )

# View(
#   t(
#     npv.key %>% 
#       group_by(doc_year) %>% 
#       summarize(
#         count = n()
#         
#         ,bbl_SECTION = "--------------"
#         ,unique.bbls = sum(!duplicated(bbl))
#         ,bbl.na = sum(is.na(bbl))
#         ,bbl.condos = sum(
#           !duplicated(bbl) &
#             str_sub(bbl,start=-4,end=-4) != 0
#         )
#         ,bbl.75lot = sum(
#           !duplicated(bbl) &
#             str_sub(bbl,start=-4,end=-4) == 75
#         )
#         ,bbl.wrong_boro = sum(
#           !duplicated(bbl) &
#             !str_sub(bbl,start=1,end=1) %in% c(1,2,3,4,5)
#         )
#         ,bbl.wrong_checkdigit = sum(
#           str_sub(bbl,start=2,end=2) != 0
#         )
#         
#         ,filename_SECTION = "--------------------"
#         ,filename.dupes = sum(duplicated(filename))
#         ,filename.na = sum(is.na(filename))
#         ,filename.maxchar = max(nchar(filename))
#         ,filename.minchar = min(nchar(filename))
#       )
#   )
# )


## (@7) inspecting final product, npv.key.joined
# View(
#   t(
#     npv.key.joined %>%
#       # group_by(doc_year) %>%
#       arrange(is.na(BldgClass)) %>%
#       summarize(
#         count = n()
#         ,bbl.unique = sum(!duplicated(bbl))
# 
#         # ,bbl_SECTION = "--------------"
#         # ,unique.bbls = sum(!duplicated(bbl))
#         # ,bbl.na = sum(is.na(bbl))
#         # ,bbl.condos = sum(
#         #   !duplicated(bbl) &
#         #     str_sub(bbl,start=-4,end=-4) != 0
#         # )
#         # ,bbl.75lot = sum(
#         #   !duplicated(bbl) &
#         #     str_sub(bbl,start=-4,end=-4) == 75
#         # )
#         # ,bbl.wrong_boro = sum(
#         #   !duplicated(bbl) &
#         #     !str_sub(bbl,start=1,end=1) %in% c(1,2,3,4,5)
#         # )
#         # ,bbl.wrong_checkdigit = sum(
#         #   str_sub(bbl,start=2,end=2) != 0
#         # )
#         #
#         # ,filename_SECTION = "--------------------"
#         # ,filename.dupes = sum(duplicated(filename))
#         # ,filename.na = sum(is.na(filename))
#         # ,filename.maxchar = max(nchar(filename))
#         # ,filename.minchar = min(nchar(filename))
# 
#         ,BldgClass_SECTION = "--------------------"
#         ,BldgClass.na = sum(!duplicated(bbl) &
#                             is.na(BldgClass)
#                             )
#         ,BldgClass.na.prop = BldgClass.na / bbl.unique
#       )
#   )
# )
# 
# ## looking into the BBLs that don't link with pluto
# tmp <- npv.key.joined %>%
#   arrange(desc(doc_year),is.na(BldgClass)) %>%
#   filter(!duplicated(bbl) &
#            is.na(BldgClass)
#   )
# 
# tmp %>%
#   mutate(borocode = str_sub(bbl,start=1,end=1)) %>%
#   group_by(borocode) %>%
#   summarize(
#     count = n()
#     ,condo_bbls = sum(
#       str_sub(bbl,start=-4,end=-4) != 0
#     )
#     ,condo_bbls.prop = condo_bbls/count
#     ,lot75_bbls = sum(
#       str_sub(bbl,start=-4,end=-3) == 75
#     )
#     ,lot75_bbls.prop = lot75_bbls/count
#     ,nonzero_dig2 = sum(
#       str_sub(bbl,start=2,end=2) != 0
#     )
#     ,nonzero_dig2.prop = nonzero_dig2/count
#     ,nonzero_condo = sum(
#       (str_sub(bbl,start=2,end=2) != 0)
#       & (str_sub(bbl,start=-4,end=-4) != 0)
#     )
#     ,nonzero_condo.prop = nonzero_condo/count
#   )
# 
# # npv.key.joined %>%
# #   filter(is.na(BldgClass)) %>% 
# #   arrange(desc(Year))
# 
# # npv.key.joined %>% 
# #   summarize(bldgclass.na = sum(is.na(BldgClass))
# #             ,bldgclass.na.prop = bldgclass.na/n()
# #             )
# 
# # View(npv.key.joined[1:100,])
# 
# # View(
# #   as.data.frame(
# #     npv.key.joined %>%
# #       filter(is.na(BldgClass)) %>%
# #       arrange(desc(Year))
# #     ,stringsAsFactors = F
# #   )[1:100,]
# # )
# 
# # tmp %>% 
# #   group_by(doc_year) %>% 
# #   summarize(count = n())


## series of joins to test where dataframes aren't linking properly
# peob.notin <- anti_join(
#   pad_expand.onlybill
#   ,pluto.all
#   ,by="BBL_bill"
# )
# npv.notin <- anti_join(
#   npv.key
#   ,pluto.all
#   ,by="bbl"
# )
# 
# npv.bldgclass.na <- npv.key.joined %>%
#   filter(is.na(BldgClass))
# 
# pluto.nodupes <- pluto.all %>% 
#   arrange(desc(Year)) %>% 
#   filter(!duplicated(bbl))
# 
# 
# npv_key.notin <- anti_join(
#   npv.key
#   ,pluto.all
#   ,by="bbl"
# )
# 
# pluto.notin <- anti_join(
#   pluto.all %>% 
#     arrange(desc(Year)) %>% 
#     filter(!duplicated(bbl))
#   ,npv.key
#   ,by="bbl"
# )
# 
# pluto.notin <- anti_join(pluto.all %>% 
#                            filter(!duplicated(BBL))
#                          ,npv.key
#                          ,by="bbl"
# )
# 
# pluto.in <- semi_join(pluto.all %>% 
#                         filter(!duplicated(BBL)) %>% 
#                         mutate(bbl = as.character(BBL))
#                       ,npv.key
#                       ,by="bbl"
# )

## saving interim dataframes to disk
save.image(
  paste(
    "/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/taxbills/Data/Workspaces/npv_key_workspace "
    ,format(
      Sys.time()
      ,"%Y%m%d_%H%M"
    )
    ,".RData"
    ,sep=""
  )
)

saveRDS(pad_expand.hold
  ,paste(
    "/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/taxbills/Data/pad_expand_hold "
    ,format(
      Sys.time()
      ,"%Y%m%d_%H%M"
    )
    ,".rds"
    ,sep=""
  )
)

saveRDS(pad_expand.joined
        ,paste(
          "/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/taxbills/Data/npv_key_create/pad_expand_joined "
          ,format(
            Sys.time()
            ,"%Y%m%d_%H%M"
          )
          ,".rds"
          ,sep=""
        )
)

saveRDS(pad_expand.onlybill
        ,paste(
          "/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/taxbills/Data/npv_key_create/pad_expand_only_billlots "
          ,format(
            Sys.time()
            ,"%Y%m%d_%H%M"
          )
          ,".rds"
          ,sep=""
        )
)

saveRDS(pluto.all.hold
        ,paste(
          "/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/taxbills/Data/npv_key_create/pluto_all_hold "
          ,format(
            Sys.time()
            ,"%Y%m%d_%H%M"
          )
          ,".rds"
          ,sep=""
        )
)

saveRDS(pluto.all
        ,paste(
          "/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/taxbills/Data/npv_key_create/pluto_all "
          ,format(
            Sys.time()
            ,"%Y%m%d_%H%M"
          )
          ,".rds"
          ,sep=""
        )
)