
##############################################################################################################
##
## This script uses the pluto augmented dataframe to step back through time and determine housing 
## stock by year and geographic area
##
##############################################################################################################

# Dataframe containing the proportion of condo/coops being rented  --------

# pluto.aug <- readRDS("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Refresh/data/pluto_augmented 20180501_1332.rds")
pluto.aug <- readRDS("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Refresh/data/pluto_augmented 20180619_1634.rds")


## read in initial
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


# what does this script require? ------------------------------------------
## pluto augmented
## condo rental adjustment 


## names of boroughs and neighborhoods 

Borough.levs.conversion <- pluto.aug %>%
  filter(!is.na(Borough) & !duplicated(Borough)) %>%
  pull(Borough)

Neighborhood.levs.conversion <- pluto.aug %>%
  mutate(AREA = as.character(Neighborhood)) %>%
  filter(!is.na(AREA) & !duplicated(AREA)) %>%
  arrange(AREA) %>%
  pull(AREA)

year.levs <- (year(Sys.Date())-1):1980

# Step backward through time ----------------------------------------------

# Neighborhood ------------------------------------------------------------

cl <- makeCluster(detectCores()-2,type="FORK")

tmp.derp <- parLapply(cl,Neighborhood.levs.conversion, function(z){
  tmp.df.outer <- pluto.aug %>% filter(Neighborhood==z)

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
  cat("\n")
  return(out.df)
}
)

stopCluster(cl)

# saveRDS(tmp.derp,"tmp_nbrhd_stats.rds")

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
    ,AreaType = "Neighborhood"
  ) %>% 
  rename(AREA = Neighborhood)

out.df.nbrhd <- out.df

# View(out.df.nbrhd)

# Borough -----------------------------------------------------------------


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



# NYC ---------------------------------------------------------------------

# z <- Neighborhood.levs.conversion[1]
# x <- 1

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


# Combine -----------------------------------------------------------------

out.df <- bind_rows(out.df.nyc,out.df.boro,out.df.nbrhd) %>% 
  select(Year,AREA,AreaType,Rentals,Condo,Coop,Small,Total,Rentals.adj,Coop.rentals,Condo.rentals,Condo.adj,Coop.adj,Owned.cc) %>% 
  mutate(Rentals.adj = round(Rentals.adj)
         ,Coop.rentals = round(Coop.rentals)
         ,Condo.rentals = round(Condo.rentals)
         ,Coop.adj = round(Coop.adj)
         ,Condo.adj = round(Condo.adj)
         ,Owned.cc = round(Owned.cc)
  )

# Save to disk ------------------------------------------------------------



getwd()
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

save.image(
  paste(
    "/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Refresh/data/backintime_workspace "
    ,format(
      Sys.time()
      ,"%Y%m%d_%H%M"
    )
    ,".RData"
    ,sep=""
  )
)