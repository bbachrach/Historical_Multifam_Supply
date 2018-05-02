library(parallel)
library(data.table)
library(dplyr)
library(stringr)

options(scipen=999)

## cool functions Tim made
source("/Users/billbachrach/Dropbox (hodgeswardelliott)/Team_NYC/Bill Bachrach/useful functions/HWE_FUNCTIONS.r")
## cool thing time made to copy to clipboard 
source("/Users/billbachrach/Dropbox (hodgeswardelliott)/Team_NYC/Bill Bachrach/useful functions/-.R")
## save workspace image with standardized format and date/time stamp
# source("/Users/billbachrach/Dropbox (hodgeswardelliott)/Team_NYC/Bill Bachrach/useful functions/ws_image_save.R")
## get sizes for all objects in workspace 
# source("/Users/billbachrach/Dropbox (hodgeswardelliott)/Team_NYC/Bill Bachrach/useful functions/workspace sizes.R")
# source("/Users/billbachrach/Dropbox (hodgeswardelliott)/Team_NYC/Bill Bachrach/useful functions/itertell.R")
# source("/Users/billbachrach/Dropbox (hodgeswardelliott)/Team_NYC/Bill Bachrach/useful functions/numbers2words.R")


###############################################
### LOAD PREVIOUS VERSION OF THE FILE 
# load("/Users/billbachrach/Dropbox (hodgeswardelliott)/Team_NYC/Bill Bachrach/ad_hoc/UWS condo prop/Data/ACRIS_condo_coop_change/condo_coop_ACRIS_timeseries.RData")
### If loading previous file can skip down beyond the munging part



#########################################################################################################################################################
## First portion identifies examples where a deed is transferred to an entity title "Owner's Corp" or something along those lines


# tudorplace_mast_deed <- grep("FT_1600000214060",rp_mast_deeds.df[,"DOCUMENT_ID"])
# tudorplace_parties_deed <- grep("FT_1600000214060",rp_parties.df[,"DOCUMENT_ID"])
# tudorplace_refer_deeds <- grep("FT_1600000214060",rp_refer.df[,"DOCUMENT_ID"])
# tudorplace_legal_deed <-  grep("FT_1600000214060",rp_legal.df[,"DOCUMENT_ID"])

# tudorplace_mast_deed
# tudorplace_parties_deed
# tudorplace_refer_deeds
# tudorplace_legal_deed


## Get the acris data 
# rp_mast_deeds.df <- readRDS("/Users/billbachrach/Dropbox (hodgeswardelliott)/Team_NYC/Bill Bachrach/ad_hoc/UWS condo prop/Data/ACRIS_condo_coop_change/ACRIS_rp_master.rds")
# rp_legal.df <- readRDS("/Users/billbachrach/Dropbox (hodgeswardelliott)/Team_NYC/Bill Bachrach/ad_hoc/UWS condo prop/Data/ACRIS_condo_coop_change/ACRIS_rp_legal.rds")
# rp_parties.df <- readRDS("/Users/billbachrach/Dropbox (hodgeswardelliott)/Team_NYC/Bill Bachrach/ad_hoc/UWS condo prop/Data/ACRIS_condo_coop_change/ACRIS_rp_parties.rds")

## NOTE: the next few steps are highly redundant and inefficient

## From the ACRIS real properties parties file, select those where second party has "owner's corp" in it
owner.vec <- (grepl("owners",rp_parties.df[,"NAME"],ignore.case=T) |
                grepl("owner's",rp_parties.df[,"NAME"],ignore.case=T)) & 
  (grepl("CORP",rp_parties.df[,"NAME"],ignore.case=T) | 
     grepl("CP",rp_parties.df[,"NAME"],ignore.case=T) | 
     grepl("CRP",rp_parties.df[,"NAME"],ignore.case=T) | 
     grepl("CO",rp_parties.df[,"NAME"],ignore.case=T)) & 
  rp_parties.df[,"PARTY_TYPE"] == 2



## select the document ids from that file 
tmp.docids <- rp_parties.df[owner.vec,"DOCUMENT_ID"]
## restrict the rp_parties.df dataframe to just those observations
tmp.parties <- rp_parties.df[owner.vec,]
## restrict the master file to those which have the appropriate document ids
tmp.df <- rp_mast_deeds.df %>% filter(DOCUMENT_ID %in% tmp.docids & 
                                        DOC_TYPE == "DEED")


# tmp.df.hold <- tmp.df
# tmp.df <- tmp.df.hold
# tmp.parties.hold <- tmp.parties
# tmp.parties <- tmp.parties.hold

## some of the parties df has some shit we're not interested in, take those out
## also restrict to document ids in the master file 
tmp.parties <- tmp.parties %>% 
  filter(!grepl("homeowner",NAME,ignore.case=T) | 
           grepl("service",NAME,ignore.case=T) | 
           grepl("home owner",NAME,ignore.case=T)) %>%
  filter(!duplicated(paste(DOCUMENT_ID,NAME))) %>%
  filter(!(duplicated(DOCUMENT_ID) & ADDRESS_1=="")) %>%
  filter(!duplicated(DOCUMENT_ID)) %>%
  filter(DOCUMENT_ID %in% tmp.df[,"DOCUMENT_ID"])

# tmp.parties <- tmp.parties %>% filter(grepl("owners",NAME,ignore.case=T) | 
#                               grepl("owner's",NAME,ignore.case=T))

## restrict the master file to the document ids in the newly restricted parties file
tmp.df <- tmp.df %>% filter(DOCUMENT_ID %in% tmp.parties[,"DOCUMENT_ID"])

## combine the two 
tmp.df <- left_join(tmp.df, (tmp.parties %>% select(DOCUMENT_ID,NAME)) 
)

## the legal file has BBL in it
## take the legal file and restrict it to just the document ids in the dataframe we created
tmp.legal <- rp_legal.df %>% filter(DOCUMENT_ID %in% tmp.df[,"DOCUMENT_ID"])
# tmp.legal.hold <- tmp.legal

## create BBL
tmp.legal <- tmp.legal %>% mutate(BBL = paste(BOROUGH,BLOCK,LOT,sep="_"))


## not sure how the duplicate removal in the next two code snippets works, need to come back to this
# drops.legal <- duplicated(paste(tmp.legal[,"DOCUMENT_ID"]
#                      ,tmp.legal[,"BOROUGH"]
#                      ,tmp.legal[,"BLOCK"]
#                      ,tmp.legal[,"LOT"],sep="_"))


alldupes.fun <- function(df,col){
  cat("returns boolean T/F vector of which observations belong to duplicated set")
  return(
    df[,col] %in% 
      df[duplicated(df[,col]),col]
  )
}

tmp.legal <- tmp.legal[!drops.legal,]

## join the legal file with the prior dataframe
tmp.df <- left_join((tmp.legal %>% select(DOCUMENT_ID,BBL,BOROUGH,BLOCK,LOT
                                          ,PROPERTY_TYPE,STREET_NUMBER,STREET_NAME,UNIT))
                    ,(tmp.df %>% select(DOCUMENT_ID,NAME,REEL_YEAR,DOC_DATE
                                        ,BOROUGH,DOC_TYPE,RECORDED_FILED,TRANSFERRED))
)

## and then this is our dataframe of coop conversions
owner.df <- tmp.df

# sum(!duplicated(owner.df[,"BBL"]))
# 
# round(
#   summary(factor(owner.df[,"REEL_YEAR"]))/nrow(owner.df)
#   ,digits=3
# )

setwd("/Users/billbachrach/Dropbox (hodgeswardelliott)/Team_NYC/Bill Bachrach/ad_hoc/UWS condo prop/Data/ACRIS_condo_coop_change")

# saveRDS(owner.df,"coop_by_ownercorp_flag.rds")
# write.csv(owner.df,"coop_by_ownercorp_flag.csv",
#           row.names=F)


##################################################
## Condos

## put the acris data back in
rm(rp_parties.df)
gc()
rp_mast_deeds.df <- readRDS("/Users/billbachrach/Dropbox (hodgeswardelliott)/Team_NYC/Bill Bachrach/ad_hoc/UWS condo prop/Data/ACRIS_condo_coop_change/ACRIS_rp_master.rds")
rp_legal.df <- readRDS("/Users/billbachrach/Dropbox (hodgeswardelliott)/Team_NYC/Bill Bachrach/ad_hoc/UWS condo prop/Data/ACRIS_condo_coop_change/ACRIS_rp_legal.rds")

# summary(factor(str_sub((rp_mast_deeds.df %>% filter(DOC_TYPE=="CDEC"))[,"DOC_DATE"],start=1,end=4)))
# doctypes.80 <- as.data.frame(summary(factor((rp_mast_deeds.df %>% filter(REEL_YEAR=="1980"))[,"DOC_TYPE"])))
# doctypes.90 <- as.data.frame(summary(factor((rp_mast_deeds.df %>% filter(REEL_YEAR=="1990"))[,"DOC_TYPE"])))


# getwd()
setwd("/Users/billbachrach/Dropbox (hodgeswardelliott)/Team_NYC/Bill Bachrach/ad_hoc/UWS condo prop/Data")
# write.csv(doctypes.80, file="doctypes_1980.csv",row.names=T)
# write.csv(doctypes.90, file="doctypes_1990.csv",row.names=T)


## restrict to just master files where doc type is condo conversion decleration
tmp.mast.cdec <- rp_mast_deeds.df %>% filter(DOC_TYPE=="CDEC")
## to get BBL restrict the legal file to just document ids in the tmp.mast.cdec DataFrame
tmp.legal <- rp_legal.df %>% 
  filter(DOCUMENT_ID %in% tmp.mast.cdec[,"DOCUMENT_ID"]) %>% 
  mutate(BBL = paste(BOROUGH,BLOCK,LOT,sep="_"),
         FULL_ADDRESS.LEGAL = paste(trimws(STREET_NUMBER),trimws(STREET_NAME),sep=" ")
  )

# sum(!duplicated(
#   paste(tmp.legal[,"STREET_NUMBER"]
#         ,tmp.legal[,"STREET_NAME"]
#         ,sep="_")
# )
# )

## address to BBL key could be useful, making it just in case
address_bbl.key <- tmp.legal %>% 
  mutate(BBL = paste(BOROUGH, BLOCK, LOT,sep="_")) %>% 
  select(DOCUMENT_ID,BBL,STREET_NUMBER
         ,STREET_NAME,UNIT,BOROUGH,BLOCK,LOT
  )

# saveRDS(address_bbl.key
#         ,"address_bbl_key_for_ACRIS_condoconversion_doctype"
# )

# write.csv(
#   address_bbl.key
#   ,"address_bbl_key_for_ACRIS_condoconversion_doctype.csv"
#   ,row.names=F
# )

# saveRDS(rp_legal.df
#         ,"ACRIS_rp_legal"
# )

# saveRDS(rp_mast_deeds.df
#         ,"ACRIS_rp_master"
# )

# saveRDS(rp_parties.df
#         ,"ACRIS_rp_parties"
# )

# save.image("condo_coop_ACRIS_timeseries.RData")


# sum(
#   tmp.mast.cdec[,"DOCUMENT_ID"] %in% 
#     # tmp.legal[,"DOCUMENT_ID"]
#   rp_legal.df[,"DOCUMENT_ID"]
# )


## Create the cdec dataframe with BBL in it
# cdec_bbl.df.hold <- cdec_bbl.df
cdec_bbl.df <- inner_join(tmp.legal %>% 
                            select(DOCUMENT_ID, BBL
                                   ,FULL_ADDRESS.LEGAL, STREET_NUMBER, STREET_NAME, UNIT, BOROUGH, BLOCK, LOT
                                   ,PARTIAL_LOT
                            )
                          ,
                          (tmp.mast.cdec %>% 
                             select(DOCUMENT_ID, DOC_DATE, REEL_YEAR, RECORDED_FILED
                                    ,DOC_TYPE, DOC_AMOUNT, TRANSFERRED
                             )
                          )
)



## so, unfortunately neither BBL nor Address link directly to pluto
## so we need to get from Address to pluto BBL 
## this involves using the both the adr and bbl Property Address Directory from Bytes of the Big Apple

# sum(
#   !duplicated(
#     paste(cdec_bbl.df[,"FULL_ADDRESS.LEGAL"]
#           ,cdec_bbl.df[,"REEL_YEAR"]
#           ,sep="_")
#   )
# )

docid_address_dupes <- as.data.frame(cdec_bbl.df %>% 
                                       group_by(DOCUMENT_ID) %>% 
                                       summarize(num_addresses = length(unique(FULL_ADDRESS.LEGAL))) %>% 
                                       arrange(desc(num_addresses)) %>% 
                                       filter(num_addresses>1) 
                                     # %>%
                                     #   summarize(sum(num_addresses))
                                     ,stringsAsFactors=F
)


cdec_add_dupes <- cdec_bbl.df %>% filter(DOCUMENT_ID %in% docid_address_dupes[,"DOCUMENT_ID"])


# length(unique(cdec_bbl.df[,"FULL_ADDRESS.LEGAL"]))

# colnames(pluto)
# sum(unique(cdec_add_dupes[,"FULL_ADDRESS.LEGAL"]) %in% pluto[,"Address"])
# length(unique(cdec_add_dupes[,"FULL_ADDRESS.LEGAL"]))


# sum(unique(cdec_add_dupes[,"BBL"]) %in% pluto[,"BBL"])
# length(unique(cdec_add_dupes[,"BBL"]))

###########################################################################################################
## idea right now is to standardize the ACRIS BBLS from condo conversions to pluto bbls

boba.bbl <- fread("/Users/billbachrach/Dropbox (hodgeswardelliott)/Team_NYC/Bill Bachrach/Data Sources/BOBA/bobabbl.txt",
                  colClasses=rep("character",23),
                  na.strings="    ",
                  stringsAsFactors=F,
                  # nrows=4,
                  data.table=F)

# boba.bbl.hold <- boba.bbl
# boba.bbl <- boba.bbl.hold
boba.bbl <- boba.bbl %>%
  mutate(
    billboro = as.numeric(billboro)
    ,billblock=as.numeric(billblock)
    ,billlot=as.numeric(billlot)
    ,BBL.bill = paste(billboro,billblock,billlot,sep="_")
    ,boro = as.numeric(boro)
    ,block=as.numeric(block)
    ,lot=as.numeric(lot)
    ,BBL = paste(boro,block,lot,sep="_")
    ,loboro = as.numeric(loboro)
    ,loblock=as.numeric(loblock)
    ,lolot=as.numeric(lolot)
    ,BBL.lo = paste(loboro,loblock,lolot,sep="_")
    ,hiboro = as.numeric(hiboro)
    ,hiblock=as.numeric(hiblock)
    ,hilot=as.numeric(hilot)
    ,BBL.hi = paste(hiboro,hiblock,hilot,sep="_")
  )



## munging boba property address file (address not bbl) to usable format
boba.adr <- fread("/Users/billbachrach/Dropbox (hodgeswardelliott)/Team_NYC/Bill Bachrach/Data Sources/BOBA/bobaadr.txt",
                  colClasses=rep("character",23),
                  na.strings="    ",
                  stringsAsFactors=F,
                  # nrows=4,
                  data.table=F) %>%
  mutate(lhnd = trimws(lhnd)
         ,hhnd = trimws(hhnd))

boba.adr <- left_join(boba.adr,boba.bbl[,c("BBL","BBL.bill","billboro","billblock","billlot")],by="BBL")

boba.adr[,"unique_id"] <- as.character(1:nrow(boba.adr))

boba.adr.expand <- boba.adr %>% filter(lhnd != hhnd)
boba.adr.nonexpand <- boba.adr %>% filter(lhnd == hhnd)


# View(boba.adr.expand[1:1000,])

# sum(grepl("[[:alpha:]]",str_sub(boba.adr.expand[,"lhnd"],start=-4,end=-2)))
# sum(grepl("[^[:digit:]]",str_sub(boba.adr.expand[,"lhnd"],start=-4,end=-1)))
# sum(grepl("[^[:digit:]]",trimws(boba.adr.expand[,"lhnd"])))

# tmp.bae <- boba.adr.expand[grepl("[^[:digit:]]",trimws(boba.adr.expand[,"lhnd"])),]
# sel <- sample(1:nrow(tmp.bae),1000,replace=F)
# View(tmp.bae[sel,])

# restr.queens <- grepl("-",boba.adr.expand[,"lhnd"]) | grepl("-",boba.adr.expand[,"hhnd"])
# restr.alpha <- grepl("[[:alpha:]]",boba.adr.expand[,"lhnd"]) | grepl("[[:alpha:]]",boba.adr.expand[,"hhnd"])
# restr.3alpha <- grepl("[[:alpha:]]",str_sub(boba.adr.expand[,"lhnd"],start=-3,end=-3)) | grepl("[[:alpha:]]",str_sub(boba.adr.expand[,"hhnd"],start=-3,end=-3))
# restr.2alpha <- grepl("[[:alpha:]]",str_sub(boba.adr.expand[,"lhnd"],start=-2,end=-2)) | grepl("[[:alpha:]]",str_sub(boba.adr.expand[,"hhnd"],start=-2,end=-2))


########################################################################################################################
## Starting the grueling, thankless task of cleaning and matching the addresses


## anything that is garage or rear... drop that fucking shit 
restr.alpha_drops <- grepl("[[:alpha:]]",str_sub(boba.adr.expand[,"lhnd"],start=-4,end=-2)) | 
  grepl("[[:alpha:]]",str_sub(boba.adr.expand[,"hhnd"],start=-4,end=-2)
  )

## anything that is fucking 1/2 drop that fucking shit 
restr.fraction_drops <- (grepl("/",boba.adr.expand[,"lhnd"]) | 
                           grepl("/",boba.adr.expand[,"hhnd"])
)

boba.adr.expand <- boba.adr.expand[!(restr.alpha_drops | 
                                       restr.fraction_drops),]

## gotta deal with queens and their fucked up addresses
## break out anything that has that queens garbage in it
## NOTE: some street numbers mean from X to Y, should actually subset this by observations that are actually in queens
restr.queens <- grepl("-",boba.adr.expand[,"lhnd"]) | grepl("-",boba.adr.expand[,"hhnd"])
restr.alpha <- grepl("[[:alpha:]]",boba.adr.expand[,"lhnd"]) | grepl("[[:alpha:]]",boba.adr.expand[,"hhnd"])

bae.queens <- boba.adr.expand[restr.queens,]
bae.nonqueens <- boba.adr.expand[!restr.queens,]

# tmp.bae <- boba.adr.expand[restr.queens | restr.alpha,]
# dim(tmp.bae)
# sel <- sample(1:nrow(tmp.bae),1000,replace=F)

# View(boba.adr.expand[restr.queens & restr.alpha,])

# bae.queens <- boba.adr.expand[restr.queens,]
# sel <- sample(1:nrow(bae.queens),1000,replace=F)
# View(bae.queens[sel,])

# class(boba.adr.expand[,"lhnd"])

# View(boba.adr.expand[
# grep("[[:alpha:]]",str_sub(boba.adr.expand[,"lhnd"],start=-4,end=-1))
# ,])

# gsub("-.*","",bae.queens[,"lhnd"])
# gsub(".*-","",bae.queens[,"lhnd"])
# bae.queens[,"lhnd"]


## idea is to create a every possible address within a range

## split into numbers to right and left of the "-" and remove alpha characters
## put the alpha characters into their own shit 
bae.queens[,"lhnd_q1"] <- gsub("-.*","",bae.queens[,"lhnd"])
bae.queens[,"lhnd_q2"] <- gsub("[[:alpha:]]","",
                               gsub(".*-","",bae.queens[,"lhnd"])
)
bae.queens[,"lhnd_alpha"] <- gsub("[^[:alpha:]]","",bae.queens[,"lhnd"])
bae.queens[,"hhnd_q1"] <- gsub("-.*","",bae.queens[,"hhnd"])
bae.queens[,"hhnd_q2"] <- gsub("[[:alpha:]]","",
                               gsub(".*-","",bae.queens[,"hhnd"])
)
bae.queens[,"hhnd_alpha"] <- gsub("[^[:alpha:]]","",bae.queens[,"hhnd"])

uid.vec <- unique(bae.queens[,"unique_id"])

## next part is memory intensive, let's clear up the workspace to free some memory
## should probably close out external processes that are using memory as well 
rm(rp_mast_deeds.df)
rm(rp_legal.df)
gc()


tmp.out <- mclapply(uid.vec, function(z){
  iter.counter <- which(uid.vec==z)
  itertell.fun(iter.counter,50)
  # cat(x,"out of",length(uid.vec),"\n")
  
  tmp.obs <- bae.queens[bae.queens[,"unique_id"]==z,]
  
  ## check if even or odd
  even.lhnd <- as.numeric(tmp.obs[,"lhnd_q1"]) %% 2 == 0
  even.hhnd <- as.numeric(tmp.obs[,"hhnd_q1"]) %% 2 == 0
  ## create vectors of number from lowest val to highest val
  q1 <- tmp.obs[,"lhnd_q1"] : tmp.obs[,"hhnd_q1"]
  ## if both low and high are even/odd keep only even/odd numbers
  if(even.lhnd==even.hhnd){
    q1 <- q1[q1%%2 == as.numeric(tmp.obs[,"lhnd_q1"]) %% 2]
  } else{
    q1 <- q1
  }
  
  ## add a leading 0 if there's only 1 character
  q1 <- ifelse(nchar(as.character(q1))==1,paste(0,q1,sep=""),q1)
  
  ## same as above
  even.lhnd <- as.numeric(tmp.obs[,"lhnd_q2"]) %% 2 == 0
  even.hhnd <- as.numeric(tmp.obs[,"hhnd_q2"]) %% 2 == 0
  q2 <- tmp.obs[,"lhnd_q2"] : tmp.obs[,"hhnd_q2"]
  if(even.lhnd==even.hhnd){
    q2 <- q2[q2%%2 == as.numeric(tmp.obs[,"lhnd_q2"]) %% 2]
  } else{
    q2 <- q2
  }
  
  q2 <- ifelse(nchar(as.character(q2))==1,paste(0,q2,sep=""),q2)
  
  ## somthing similar to above
  if(grepl("[[:alpha:]]",tmp.obs[,"hhnd_alpha"])){
    if(grepl("[[:alpha:]]",tmp.obs[,"lhnd_alpha"])){
      start.letter <- tmp.obs[,"lhnd_alpha"]
    } else {
      start.letter <- "A"
    }
    alpha.vec <- LETTERS[which(LETTERS[]==start.letter):
                           which(LETTERS[]==tmp.obs[,"hhnd_alpha"])]
  } else {
    ## or if there are no alpha characters leave it as nothing
    alpha.vec <- ""
  }
  
  ## put all them fuckers together
  tmp.out <- trimws(unlist(lapply(alpha.vec, function(y)
    paste(
      unlist(lapply(q2, function(x)
        paste(q1
              ,x
              ,sep="-")))
      ,y
      ,sep="")
  )))
  
  ## and then put them into a matrix with original address and the iterated address
  out <- cbind(z,tmp.out)
  return(out)
}
## for parallelization, can remove if doing it in serial
,mc.cores=4
)

## put into a single dataframe and name something sensible
tmp.outdf <- as.data.frame(do.call("rbind",tmp.out)
                           ,stringsAsFactors=F
)
colnames(tmp.outdf) <- c("unique_id","STREET_NUMBER")

## make sure the unique id is character as it is in the original dataframe
bae.queens[,"unique_id"] <- as.character(bae.queens[,"unique_id"])
bae.queens <- left_join(tmp.outdf,bae.queens)


## now doing the same thing for the non-queens addresses
bae.nonqueens[,"lhnd_alpha"] <- gsub("[^[:alpha:]]","",bae.nonqueens[,"lhnd"])
bae.nonqueens[,"hhnd_alpha"] <- gsub("[^[:alpha:]]","",bae.nonqueens[,"hhnd"])

bae.nonqueens[,"lhnd"] <- gsub("[[:alpha:]]","",bae.nonqueens[,"lhnd"])
bae.nonqueens[,"hhnd"] <- gsub("[[:alpha:]]","",bae.nonqueens[,"hhnd"])

uid.vec <- unique(bae.nonqueens[,"unique_id"])

# tmp.out_nq <- lapply(uid.vec, function(z){
tmp.out_nq <- mclapply(uid.vec, function(z){
  iter.counter <- which(uid.vec==z)
  itertell.fun(iter.counter,1)
  
  tmp.obs <- bae.nonqueens[bae.nonqueens[,"unique_id"]==z,]
  even.lhnd <- as.numeric(tmp.obs[,"lhnd"]) %% 2 == 0
  even.hhnd <- as.numeric(tmp.obs[,"hhnd"]) %% 2 == 0
  q1 <- tmp.obs[,"lhnd"] : tmp.obs[,"hhnd"]
  if(even.lhnd==even.hhnd){
    q1 <- q1[q1%%2 == as.numeric(tmp.obs[,"lhnd"]) %% 2]
  } else{
    q1 <- q1
  }
  
  q1 <- ifelse(nchar(as.character(q1))==1,paste(0,q1,sep=""),q1)
  
  if(grepl("[[:alpha:]]",tmp.obs[,"hhnd_alpha"])){
    if(grepl("[[:alpha:]]",tmp.obs[,"lhnd_alpha"])){
      start.letter <- tmp.obs[,"lhnd_alpha"]
    } else {
      start.letter <- "A"
    }
    alpha.vec <- LETTERS[which(LETTERS[]==start.letter):
                           which(LETTERS[]==tmp.obs[,"hhnd_alpha"])]
  } else {
    alpha.vec <- ""
  }
  
  tmp.out <- trimws(unlist(lapply(alpha.vec, function(y)
    paste(q1
          ,y
          ,sep="")
  )))
  
  
  out <- cbind(z,tmp.out)
  return(out)
}
,mc.cores=4
)

tmp.outdf_nq <- as.data.frame(do.call("rbind",tmp.out_nq)
                              ,stringsAsFactors=F
)

colnames(tmp.outdf_nq) <- c("unique_id","STREET_NUMBER")

bae.nonqueens.hold <- bae.nonqueens
bae.nonqueens[,"unique_id"] <- as.character(bae.nonqueens[,"unique_id"])
bae.nonqueens <- left_join(tmp.outdf_nq,bae.nonqueens,by="unique_id")

## save them motherfuckers should you so choose
# setwd("/Users/billbachrach/Dropbox (hodgeswardelliott)/Team_NYC/Bill Bachrach/ad_hoc/UWS condo prop/Data/ACRIS_condo_coop_change")
# saveRDS(bae.queens,"queens_adr_expansion.rds")
# saveRDS(bae.nonqueens,"nonqueens_adr_expansion.rds")

## extra column names in the queens dataframe, keep only those in the non-queens dataframe
bae.queens <- bae.queens[,
                         colnames(bae.queens)[colnames(bae.queens) %in% colnames(bae.nonqueens)]
                         ]

## put the queens and non-queens dataframes together
bae <- bind_rows(bae.queens,bae.nonqueens)

## create the STREET_NUMBER variable in the portion that did not need to be expanded
boba.adr.nonexpand <- boba.adr.nonexpand %>% mutate(
  STREET_NUMBER = lhnd
)


## make the unique id character (just in case it's not) and bind the two together 
boba.adr.nonexpand[,"unique_id"] <- as.character(boba.adr.nonexpand[,"unique_id"])
new.df <- bind_rows(bae,boba.adr.nonexpand)

####################################################################################################
## NOW... onto the real drudgery 



## white space is kind of fucked up so I do a lot of trimming and adding
## particularly in the PAD file there's a lot of padding, along with double spaces and shit
new.df[,"STREET_NUMBER"] <- trimws(new.df[,"STREET_NUMBER"])
## remove the double padding fucking garbage
new.df[,"STREET_NAME"] <- trimws(gsub("   "," ",
                                      gsub("  "," ",
                                           gsub("   "," ",trimws(new.df[,"stname"]))
                                      )
)
)

## creating the full address variable, which I wind up not using 
new.df[,"FULL_ADDRESS"] <- paste(trimws(new.df[,"STREET_NUMBER"]),trimws(new.df[,"STREET_NAME"]),sep=" ")
# save.image("boba_address_expansion.RData")
## not sure what this line does, but keeping it anyway
new.df[,"STREET_NUMBER.noalpha"] <- trimws(new.df[,"STREET_NUMBER"])


saveRDS(new.df,"PLUTO")

# creating the full address for the cdec/ACRIS data 
cdec_bbl.df[,"FULL_ADDRESS.LEGAL"] <- paste(trimws(cdec_bbl.df[,"STREET_NUMBER"])
                                            ,trimws(cdec_bbl.df[,"STREET_NAME"])
                                            ,sep=" "
)

# sel <- sample(1:nrow(new.df),1000,replace=F)
# View(new.df[sel,])

# sel <- sample(1:nrow(cdec_bbl.df),1000,replace=F)
# View(cdec_bbl.df[sel,])

# sum(cdec_bbl.df[,"FULL_ADDRESS.LEGAL"] %in% new.df[,"FULL_ADDRESS"])
# sum(
#   trimws(cdec_bbl.df[,"STREET_NAME"]) %in% trimws(new.df[,"stname"]) & 
#     trimws(cdec_bbl.df[,"STREET_NUMBER"]) %in% trimws(new.df[,"STREET_NUMBER"])
# )



## To standardize the street names in cdec/ACRIS I've decided to create a separate vector and then put it back in
## .orig serves as a save point

# cdec_bbl.df.hold <- cdec_bbl.df
cdec.street.orig <- cdec_bbl.df[,"STREET_NAME"]

## again, getting rid of the fucked up spacing
cdec.street <- trimws(gsub("   "," ",
                           gsub("  "," ",
                                gsub("   "," ",trimws(cdec.street.orig))
                           )
)
)

## had this idea about ADDING white space to either end to help differentiate abbreviations like "ST" 
## from words that end in those abbreviations, went ahead and dropped in a couple of adhoc fixes here
cdec.street <- paste(" ",trimws(cdec.street)," ",sep=" ")
cdec.street <- gsub("AV OF AMERICAS ","AVENUE OF THE AMERICAS ",
                    gsub(" S T ","ST ",
                         # gsub("FT.","FORT",
                         gsub("NYNY"," ",
                              gsub(" NY"," ",
                                   gsub(",NY"," ", 
                                        gsub("NY,NY"," ", cdec.street)
                                   )
                              )
                         )
                         # )
                    )
)

## trim the white space after each time
cdec.street <- trimws(cdec.street)


## any avenue that is spelled out should just be a number
aves <- c("FIRST","SECOND","THIRD","FOURTH","FIFTH","SIXTH","SEVENTH","EIGHTH","NINTH","TENTH")

for(i in 1:length(aves)){
  cdec.street <- gsub(aves[i],i,cdec.street) 
}


## previously had used a double digit prior to a suffix, turns out that using a single one works better
# sfx.orig <- c("[[:digit:]][[:digit:]]TH"
#               ,"[[:digit:]][[:digit:]]ST"
#               ,"[[:digit:]][[:digit:]]ND"
#               ,"[[:digit:]][[:digit:]]RD")
# 
# 
# sfx.change <- c("[[:digit:]][[:digit:]]"
#                 ,"[[:digit:]][[:digit:]]"
#                 ,"[[:digit:]][[:digit:]]"
#                 ,"[[:digit:]][[:digit:]]")

sfx.orig <- c("[[:digit:]]TH"
              ,"[[:digit:]]ST"
              ,"[[:digit:]]ND"
              ,"[[:digit:]]RD")


sfx.change <- c("[[:digit:]]"
                ,"[[:digit:]]"
                ,"[[:digit:]]"
                ,"[[:digit:]]")

## identify everything that has a suffix
sfx.obs <- which(grepl(paste(sfx.orig,collapse="|"),cdec.street))


## go through those and pull out the suffixes
for(i in 1:length(sfx.obs)){
  tmp.obs <- cdec.street[sfx.obs[i]]
  
  ## where is the number and suffix in the string
  match.start <- regexpr(paste(sfx.orig,collapse="|"),tmp.obs)[[1]]
  match.len <- attr(
    regexpr(paste(sfx.orig,collapse="|"),tmp.obs)
    ,"match.length"
  )
  
  ## pull out the offending characters
  tmp.orig <- str_sub(cdec.street[sfx.obs[i]],
                      start=match.start,
                      end=(match.start+match.len-1)
  )
  
  ## create a version that does not have the suffix by remove alpha characters
  tmp.change <- gsub("[[:alpha:]]","",tmp.orig)
  
  ## replace number suffix with just number in the original string
  tmp.out <- gsub(tmp.orig,tmp.change,tmp.obs)
  
  ## put into the appropriate place in the street name vector
  cdec.street[sfx.obs[i]] <- tmp.out
  
  ## only show output every 25th iteration
  if(i%%25==0){
  cat("changed: ",tmp.obs," to ",tmp.out,"\n")
    }
}

## remove thos fucking periods and shit, going to need some of the other punctuations
cdec.street <- paste("  ",trimws(cdec.street),"  ",sep=" ")
cdec.street <- gsub("\\.","",
                    gsub(",","", cdec.street)
)


## cleaning up street names prior to doing levenshtein matching
## double padding for some reason I'm currently forgetting
## just use it though, fack eeet
cdec.street <- paste("  ",trimws(cdec.street),"  ",sep=" ")

orig.sn <- list(
  avenue = c(" AVE ", " AV ")
  ,street = c(" ST  "," STR ")
  ,boulevard = c("BLVD ")
  ,road = c(" RD ")
  ,place = c(" PL ")
  ,parkway = c(" PKWY ")
  ,drive = c(" DR ")
  ,west = c(" W ")
  ,south = c(" S ")
  ,east = c(" E ")
  ,north = c(" N ")
)

new.sn <- list(
  avenue = " AVENUE "
  ,street = " STREET "
  ,boulevard = " BOULEVARD "
  ,road = " ROAD "
  ,place = " PLACE "
  ,parkway = " PARKWAY "
  ,drive = " DRIVE "
  ,west = " WEST "
  ,south = " SOUTH "
  ,east = " EAST "
  ,north = " NORTH "
)

for(i in 1:length(orig.sn)){
  cdec.street <- gsub(paste(orig.sn[[i]],collapse="|"),new.sn[[i]],cdec.street)
  cat(i,"\n")
}

cdec.street <- trimws(cdec.street)

## *1
## original eyeball specification of street name changes at bottom market with *1



# sel <- sample(1:length(cdec.street),1000,replace=F)
# View(cbind(cdec_bbl.df[sel,"STREET_NAME"]
#            ,cdec.street[sel])
# )

## putting back into the cdec/ACRIS dataframe
cdec_bbl.df[,"STREET_NAME"] <- cdec.street



## for levenshtein matching I want to limit the number of pluasible choices
## only searching for street names that share the same borough and lot
## creating "BB" instead of "BBL"
cdec_bbl.df[,"BB"] <- paste(cdec_bbl.df[,"BOROUGH"],cdec_bbl.df[,"BLOCK"],sep="_")
new.df[,"BB"] <- paste(new.df[,"boro"],as.numeric(new.df[,"block"]),sep="_")

## since this is a multicore job, better to slim down the dataframe I'm matching on
## removing instances where both BB and street name are duplicated
BB_name.dupes <- duplicated(paste(new.df[,"BB"],new.df[,"STREET_NAME"]))
new.df.slim <- new.df[!BB_name.dupes,c("BB","STREET_NAME")]

## vector of all BBs in the cdec/ACRIS dataframe... not sure why
cdec.bb.vec <- unique(cdec_bbl.df[,"BB"])

## levenshtein matching for street names
ptm <- proc.time()
tmp.out <- mclapply(1:nrow(cdec_bbl.df), function(x){
# tmp.out.notcool <- mclapply(notcool, function(x){
  # tmp.out <- lapply(1:1000, function(x){
  itertell.fun(x,1)
  
  ## start off with output being NA
  out <- NA

  ## original streetname
  stname.init <- cdec_bbl.df[x,"STREET_NAME"]
  
  if(nchar(stname.init)>0){
    bb.init <- cdec_bbl.df[x,"BB"]
    # boba.tmp <- (new.df.slim %>% filter(BB==bb.init))[,"STREET_NAME"]
    boba.stnames <- (new.df.slim %>% filter(BB==bb.init))[,"STREET_NAME"]
    
    ## first, if the street name contains a number try for an exact match on a number street
    if(grepl("[[:digit:]]",stname.init)==T){
      stname.digit <- gsub("[^[:digit:]]","",stname.init)  
      boba.digit <- gsub("[^[:digit:]]","",boba.stnames)  
      numer.match <- boba.stnames[which(boba.digit==stname.digit)]      
      ## if there's only one match, we're gucci
      if(length(numer.match)==1){
        out <- numer.match
      } 
    }
    
    ## if that didn't work, doing levenshtein matching
    if(is.na(out)){
      out.tmp <- agrep(stname.init,boba.stnames,value=T)
      # out.tmp <- agrep(stname.init,boba.stnames,max.distance=.2,value=T)
      ## if there's only one match, we're gucci
      if(length(out.tmp)==1){
        out <- out.tmp
      }
    } 
  } else {
    ## redundant but setting out to NA if none of that shit worked
    out <- NA
  }
  return(out)
}
,mc.cores=4
)
lvnstn.time <- proc.time() - ptm
# serial.time <- proc.time() - ptm

## think these next few lines are depricated
# notcool <- which(!unlist(lapply(tmp.out, function(x) length(x)))==1)
# length(notcool)
# tmp.out[notcool] <- NA

## combining output into a single dataframe
tmp.derp <- as.data.frame(
  cbind(cdec_bbl.df[,"STREET_NAME"],
        unlist(tmp.out)
  )
  ,stringsAsFactors=F
)
colnames(tmp.derp) <- c("orig","new")

## where did it not work?
notcool <- which(is.na(tmp.derp[,"new"]) & 
                   !(tmp.derp[,"orig"] %in% trimws(new.df[,"STREET_NAME"]))
                 )

## on the last run I did there were only 41 unique fuckups, changing by hand is the best route
## different methods: 
### check BBL online and see what the street is 
### look for PAD matches on two key words within the fucked up ACRIS name
### Need for the original vector to be the COMPLETE name of the fucked up street

adhoc.orig <- c(
  "GRAND STNYNY"
  ,"ELEVEN AVENUE"
  ,"GRAND CENTRAL PARKWY"
  ,"GRAND CENTRAL PWKY"
  ,"FORT HAMILTON PARKWY"
  ,"FRED DOUGLASS BOULEVARD"
  ,"FRED DOUGLAS BOULEVARD"
  ,"FRED DOUG BOULEVARD"
  ,"SAINT MARKS PLACE"
  ,"CARROLL STBROOKLYN"
  ,"CARROLL STREET BKLYN"
  ,"801 RIVERSIDE DRIVE"
  ,"LYNCH PARK STREET"
  ,"SAINT MARKS AVENUE"
  ,"AVENUE OF AMERICAS"
  ,"ADAM C POWELL BOULEVARD"
  ,"ADAN C POWELL BOULEVARD"
  ,"NORTH END AVENUE1028"
  ,"BURROUGHS PLACE"
  ,"DEKALB STREET"
  ,"545 WEST 25 STREET"
  ,"ACPOWELL JRBLVD"
  ,"BOWERYNEW YORK"
  ,"SAINT FELIX STREET"
  ,"BOARDWAY"
  ,"BOWERY STREET"
  ,"HUTCH RIVER PARKWAY SR"
  ,"HUTCHINSON RVR PY SR"
  ,"FLUSHING STREET"
  ,"STREET"
  ,"T"
  ,"BOWERY CONDOMINIUM"
  ,"WEST 110 STREET"
  ,"AVENUE WEST"
  ,"LAIGHT STAKA414WASH"
  ,"AVENUE"
  ,"ELEVENTH AVENUE"
  # ,"FORT HAMILTON PARKWAY"
  ,"SAINT JAMES PLACE"
)

adhoc.new <- c(
  "GRAND STREET"
  ,"11 AVENUE"
  ,"GRAND CENTRAL PARKWAY"
  ,"GRAND CENTRAL PARKWAY"
  ,"FT HAMILTON PARKWAY"
  ,"FREDERICK DOUGLASS BOULEVARD"
  ,"FREDERICK DOUGLASS BOULEVARD"
  ,"FREDERICK DOUGLASS BOULEVARD"
  ,"ST MARKS PLACE"
  ,"CARROLL STREET"
  ,"CARROLL STREET"
  ,"RIVERSIDE DRIVE"
  ,"LYNCH STREET"
  ,"ST MARKS AVENUE"
  ,"AVENUE OF THE AMERICAS"
  ,"ADAM CLAYTON POWELL JR BOULEVARD"
  ,"ADAM CLAYTON POWELL JR BOULEVARD"
  ,"NORTH END AVENUE"
  ,"66 STREET"
  ,"DEKALB AVENUE"
  ,"WEST 25 STREET"
  ,"ADAM CLAYTON POWELL JR BOULEVARD"
  ,"BOWERY"
  ,"ST FELIX STREET"
  ,"BROADWAY"
  ,"BOWERY"
  ,"HUTCHINSON RIVER PARKWAY SR"
  ,"HUTCHINSON RIVER PARKWAY SR"
  ,"FLUSHING AVENUE"
  ,"16 STREET"
  ,"CENTRE STREET"
  ,"BOWERY"
  ,"CATHEDRAL PARKWAY"
  ,"AVENUE W"
  ,"LAIGHT STREET"
  ,"SKILLMAN AVENUE"
  ,"11 AVENUE"
  # ,"FT HAMILTON PARKWAY"
  ,"ST JAMES PLACE"
)

# View(cbind(adhoc.orig,adhoc.new))

## run through and put them bitches into a new column called "adhoc"
## NOTE: this is not an elegant method, I is awares
tmp.derp[,"adhoc"] <- tmp.derp[,"orig"]
for(i in 1:length(adhoc.orig)){
  restr <- which(tmp.derp[,"orig"] == adhoc.orig[i])
  # tmp.derp[restr,"adhoc"] <- adhoc.new[i]
  tmp.derp[restr,"adhoc"] <- gsub(adhoc.orig[i],adhoc.new[i],tmp.derp[restr,"orig"])
}

## where the levenshtein matched column is NA, put in the manually matched names
restr <- is.na(tmp.derp[,"new"])
tmp.derp[restr,"new"] <- tmp.derp[restr,"adhoc"]

## test
sum(tmp.derp[,"new"] %in% new.df[,"STREET_NAME"])
sum(!tmp.derp[,"new"] %in% new.df[,"STREET_NAME"])/nrow(tmp.derp)
# 0.001199342
## these are like three addresses that were so fucked I couldn't find anything on them


# View(tmp.derp[!(tmp.derp[,"new"] %in% new.df[,"STREET_NAME"]),])

## put into the cdec/ACRIS dataframe as a new column name
cdec_tmp.derp.match <- match(cdec_bbl.df[,"STREET_NAME"],tmp.derp[,"orig"])
cdec_bbl.df[,"STREET_NAME.LEGAL"] <- tmp.derp[cdec_tmp.derp.match,"new"]

## somehow i did not create a legit "BBL" column in the PAD dataset 
new.df <- new.df %>% mutate(
  BBL = paste(as.numeric(boro)
              ,as.numeric(block)
              ,as.numeric(lot)
              ,sep="_")
)



rm(rp_legal.df,rp_mast_deeds.df)
gc()
save.image("condo_coop_ACRIS_timeseries 20170302.RData")


## checking the BBLs
## the PAD files line up almost exactly
sum(new.df[,"BBL"] %in% boba.bbl[,"BBL"])/nrow(new.df)
## but unfortunately our ACRIS data does not line up very well at all 
sum(cdec_bbl.df[,"BBL"] %in% boba.bbl[,"BBL"]) /nrow(cdec_bbl.df)

## so now the acris condo certificate dataset has streetnames that line up with pluto with 5% drop out
## want to get that a little bit better
sum(!((cdec_bbl.df[,"STREET_NUMBER"] %in% new.df[,"STREET_NUMBER"]) 
      | (cdec_bbl.df[,"BBL"] %in% new.df[,"BBL"]))
)/nrow(cdec_bbl.df)
## 0.0524943


## data frame with just the jacked up shit
fuckedup.df <-   cdec_bbl.df[!((cdec_bbl.df[,"STREET_NUMBER"] %in% new.df[,"STREET_NUMBER"]) 
                               | (cdec_bbl.df[,"BBL"] %in% new.df[,"BBL"])
),
c("BBL","FULL_ADDRESS.LEGAL","STREET_NUMBER","STREET_NAME.LEGAL")
]

# nrow(fuckedup.df)
## 6259 don't match

# View(fuckedup.df)

# sum(cdec_bbl.df[,"BBL"] %in% new.df[,"BBL"])
# nomatch <- View(cdec_bbl.df[which(cdec_bbl.df[,"BBL"] %in% new.df[,"BBL"])[1:1000],])

## going to do this piece wise

## for all observations that have east,west, etc in it, take those out, may want to put them onto street name
cdec.snumb <- fuckedup.df[,"STREET_NUMBER"]

cdec.direct <- cdec.snumb
directions <- c("NORTH","SOUTH","EAST","WEST")
cdec.direct[!grepl(paste(directions,collapse="|"),cdec.direct)] <- NA

for(i in 1:length(directions)){
  restr <- grepl(directions[i],cdec.direct)
  cdec.direct[restr] <- directions[i]
}
## this is a vector with east, west, etc in it where those appear in street names and nothing
cdec.direct <- ifelse(is.na(cdec.direct),"",cdec.direct)


## a couple have the number spelled out. fix that shit up 
numbers <- toupper(numbers2words(1:100))
for(i in 1:length(numbers)){
  cdec.snumb <- gsub(numbers[i],i,cdec.snumb)
  itertell.fun(i,10)
}

# Vector with the first number before any non-digit characters
cdec.firstnumb <- as.numeric(gsub("[^[:digit:]].*","",cdec.snumb))

## vector with the second number... kind of
cdec.secnumb <- as.numeric(
  gsub(".*[^[:digit:]]","",cdec.snumb)
                           )
cdec.secnumb <- ifelse(is.na(cdec.secnumb),"",cdec.secnumb)

## so now here's a vector that includes the direction we just pulled in front of the street name
cdec.altstreet <- trimws(paste(cdec.direct,cdec_bbl.df[,"STREET_NAME.LEGAL"],sep=" "))

  View(fuckedup.df[
which(!((cdec.firstnumb %in% new.df[,"STREET_NUMBER"]) & (fuckedup.df[,"STREET_NAME.LEGAL"] %in% new.df[,"STREET_NAME"])))
  ,])

## this would be the dataframe with only those that can't be matched up 
## by using only the first number from the street number  
tmp.fuckedup <- fuckedup.df[
  which(
    !paste(cdec.firstnumb,fuckedup.df[,"STREET_NAME.LEGAL"],sep=" ") %in%
      paste(new.df[,"STREET_NUMBER"],new.df[,"STREET_NAME"],sep=" "))
  ,]


## SUCCESS!!!
length(unique(tmp.fuckedup[,"FULL_ADDRESS.LEGAL"]))/
length(unique(cdec_bbl.df[,"FULL_ADDRESS.LEGAL"]))
## 0.01160966
## only 1% drop out


## This is where I've left off
## new.df is the PAD Address file which contains condo level BBLS
## we just cleaned up the addresses to match the PAD Address file
## what we REALLY want is the Billing lot BBLS which are in the PAD BBL file
## the PAD Address file serves as a link file to the PAD BBL file. 
## so from here the idea is to link cdec/ACRIS to new.df/PAD_Address and then link that to PAD BBL
## the goal is the get the PAD BBLs into the cdec/ACRIS dataframe
## But this is where I left off to start working on the AG data 

























## *1
## original eyeball of the street names to change, too sentimental to delete


## original notes on what to change, too sentimental to delete it, prolly should put it at the bottom 
# PAD.streetnames <- c(
#   STREET
#   ,BOULEVARD
#   ,AVENUE
#   ,ROAD
#   ,HIGHWAY
#   ,TERRACE
#   ,PARKWAY
#   ,CENTER
#   ,DRIVE
#   ,LANE
#   ,PLACE
#   ,COURT
#   ,TURNPIKE
#   ,TERRACE)

# abbrv.streetnames <- c(
#   ST
#   ,BLVD
#   ,AVE
#   ,RD
#   ,
#   ,
#   ,PKWY
#   ,
#   ,DR
#   ,
#   ,
#   ,CT
#   ,)

## need to remove NY from the end of some records
## punctuation appears 
## RD and TH and ND



