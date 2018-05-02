## Script to use inputs and step back through time to get rentals, condos and coops by year


# Dataframe containing the proportion of condo/coops being rented  --------

condo_rental.df <- read.csv("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/HVS/condo rental metric small.csv"
                            ,stringsAsFactors=F)

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
## conversion dataframes
## pluto augmented
## condo rental adjustment 


## names of boroughs and neighborhoods 

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


Conversions.df <- bind_rows(
  Conversions
)

# Step backward through time ----------------------------------------------


year.levs <- unique(pluto.aug %>% pull(TCO))
year.levs <- round(year.levs[order(year.levs,decreasing=T)])
year.levs <- year.levs[year.levs>=1980 & year.levs<=2017]

cl <- makeCluster(detectCores()-2,type="FORK")

## step backward through time with each geographical area as an item in a list
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
    
    rentals.tmp <- as.numeric(tmp.df %>% filter(Type=="rental") %>% summarize(sum(UnitsRes)))
    rentals <- rentals.tmp + converted_to_owned
    
    owned.tmp <- as.numeric(tmp.df %>% filter(Type!="rental") %>% summarize(sum(UnitsRes)))
    owned <- owned.tmp - converted_to_owned

    condo.tmp <- as.numeric(tmp.df %>% filter(Type=="condo") %>% summarize(sum(UnitsRes)))
    condo <- condo.tmp - converted_to_condo

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
    rentals.tmp <- as.numeric(tmp.df %>% filter(Type=="rental") %>% summarize(sum(UnitsRes)))
    converted_to_owned <- as.numeric(Conversions.df %>% filter(YEAR>year.levs[x] & AREA== z) %>% summarize(sum(CONVERTED_UNITS)) )
    rentals <- rentals.tmp + converted_to_owned
    owned.tmp <- as.numeric(tmp.df %>% filter(Type!="rental") %>% summarize(sum(UnitsRes)))
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
# year.levs <- 1980:2016
# x <- 2
# tmp.out.hold <- tmp.out
cl <- makeCluster(detectCores()-1,type="FORK")
# tmp.out <- lapply(1:length(year.levs), function(x){
tmp.out <- parLapply(cl,1:length(year.levs), function(x){
  cat(year.levs[x],"\n")
  tmp.df <- pluto.aug %>% filter(YearBuilt<year.levs[x])
  rentals.tmp <- as.numeric(tmp.df %>% filter(Type=="rental") %>% summarize(sum(UnitsRes)))
  converted_to_owned <- as.numeric(Conversions.df %>% filter(YEAR>year.levs[x] & AREA== "NYC") %>% summarize(sum(CONVERTED_UNITS)) )
  rentals <- rentals.tmp + converted_to_owned
  owned.tmp <- as.numeric(tmp.df %>% filter(Type!="rental") %>% summarize(sum(UnitsRes)))
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