
##############################################################################################################
##
## This script joins population with supply
##
##############################################################################################################

library(dplyr)

options(scipen=999)

# Read in data ------------------------------------------------------------

## supply
supply.df <- readRDS("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Refresh/data/output/multifam_supply20180622_153247.rds") %>% 
  filter(!is.na(AREA)) %>% 
  mutate(
    # Year = as.character(Year)
    AREA = trimws(as.character(AREA))
  )

## Population
## Note: time series population by neighborhood and borough derived from separate project
pop.df <- readRDS("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Data Sources/Census/Population Estimates/Historical/neighborhood_boro_population_1970_2017.rds") %>% 
  mutate(Borough = ifelse(Borough %in% "Mahattan"
                          ,"Manhattan"
                          ,Borough)
  )


pop.df <- pop.df %>% 
  mutate(AREA = trimws(ifelse(neighborhood=="Borough"
                              ,toupper(as.character(Borough))
                              ,as.character(neighborhood)
  ))
  ,AreaType = as.character(ifelse(neighborhood=="Borough"
                                  ,"Borough"
                                  ,"Neighborhood"
  )
  )
  )

## removing inter-censal and post-censal estimates where wehave actual census data 
pop.df <- anti_join(pop.df
                    ,left_join(pop.df %>% 
                                 filter(AreaType %in% "Borough") %>%
                                 group_by(Year) %>% 
                                 summarize(AreaCount = n()) %>% 
                                 filter(AreaCount > 5) %>% 
                                 select(Year)
                               ,pop.df %>% 
                                 filter(AreaType %in% "Borough"
                                        & Source %in% "NY_opendata_PostCensal")
                    )
                    ,by=c("Year","Source","AreaType")
)


pop_nyc.df <- pop.df %>% 
  filter(AreaType %in% "Borough") %>%
  group_by(Year) %>% 
  summarize(
    AREA = as.character("NYC")
    ,AreaType = as.character("City")
    ,Population = sum(Population)
    ,Population.adj = sum(Population.adj)
    ,Population.smooth = sum(Population.smooth)
  )


pop.df <- bind_rows(pop_nyc.df,
                    pop.df)

popsup.df <- left_join(
  supply.df
  ,pop.df %>% 
    select(Year,AREA,Borough
           ,Population.smooth)
  ,by=c("Year","AREA")
)

## put into same directory as the finished multifamily supply csvs
setwd("/Users/billbachrach/Dropbox (hodgeswardelliott)/Data Science/Bill Bachrach/Major projects/Multifamily Supply/Refresh/data/output")
write.csv(popsup.df
          ,paste(
            "multifamily_supply_and_population "
            ,format(
              Sys.time()
              ,"%Y%m%d_%H%M"
            )
            ,".csv"
            ,sep=""
          )
          ,na=""
          ,row.names=F
)
