###########################################################################
##
## Functions to be used in NOPV scrape
##
##
###########################################################################

# Scrape Functions --------------------------------------------------------


# Gross income ------------------------------------------------------------

gross_inc.fun <- function(tmp.text){
  gi.location <- str_locate(tmp.text,"We estimated gross income at ")
  if(sum(apply(gi.location,1,function(x){sum(!is.na(x))}))>0){
    gi.line <- which(
      apply(gi.location,1,function(x){sum(is.na(x))}) == 0 
    )
    gi.start = gi.location[gi.line,2]
    gi.snippet <- str_sub(tmp.text[gi.line],start=gi.start,end=gi.start+25)
    
    first.dig <- str_locate(gi.snippet,"[[:digit:]]")[1,2]
    
    gross_inc <- gsub("[^[:digit:]]",""
                      ,str_sub(gi.snippet
                               ,start = first.dig
                               ,end= first.dig+15
                      )
    )
  } 
  
  if(sum(apply(gi.location,1,function(x){sum(!is.na(x))}))==0){
    gi.location <- str_locate(tmp.text,"Estimated Gross Income: ")
    if(sum(apply(gi.location,1,function(x){sum(!is.na(x))}))>0){
      gi.line <- which(
        apply(gi.location,1,function(x){sum(is.na(x))}) == 0 
      )
      gi.start = gi.location[gi.line,2]
      gi.snippet <- str_sub(tmp.text[gi.line],start=gi.start,end=gi.start+20)
      
      first.dig <- str_locate(gi.snippet,"[[:digit:]]")[1,2]
      
      gross_inc <- gsub("[^[:digit:]]",""
                        ,str_sub(gi.snippet
                                 ,start = first.dig
                                 ,end= first.dig+15
                        )
      )
    } else{
      gross_inc <- NA
    }
  } 
  return(gross_inc)
}

# Rent for condos ---------------------------------------------------------

cr_1 <- function(tmp.text){
  cr.location <- str_locate(tmp.text,"We estimate the rent for your condominium lot as")
  
  if(sum(apply(cr.location,1,function(x){sum(!is.na(x))}))==0){
    cr.location <- str_locate(tmp.text,"We estimated the rent for your condominium lot")
  }
  
  if(sum(apply(cr.location,1,function(x){sum(!is.na(x))}))==0){
    cr.location <- str_locate(tmp.text,"We estimate the rent for your condominium unit as")
  }
  
  if(sum(apply(cr.location,1,function(x){sum(!is.na(x))}))==0){
    cr.location <- str_locate(tmp.text,"We estimated the rent for your condominium unit as")
  }
  
  cr.line <- which(
    apply(cr.location,1,function(x){sum(is.na(x))}) == 0 
  )
  
  cr.start = cr.location[cr.line,2]
  cr.snippet <- str_sub(tmp.text[cr.line],start=cr.start,end=cr.start+40)
  
  if(length(cr.snippet)>0){
    month_flag <- grepl("month",cr.snippet,ignore.case=T)
    
    rent <- gsub("[^[:digit:]]",""
                 ,gsub("[^[:digit:]].*",""
                       ,gsub(",",""
                             ,trimws(
                               str_sub(cr.snippet
                                       ,start = regexpr("[[:digit:]]",cr.snippet)[1]
                                       ,end=-1
                               )
                             )
                       )
                 )
    )
    
    
  } else{
    rent <- NA
    month_flag <- NA
  }
  if(length(rent)==0){
    rent <- NA
  }
  return(c(rent,month_flag))
}


rent.fun <- function(tmp.text){
  rent <- try(cr_1(tmp.text))
  if(class(rent[1]) %in% "try_error"){
    rent <- c(NA,NA)
  }
  return(rent)
}



# Building Class ----------------------------------------------------------
## format differs by year

bc.15_18 <- function(tmp.text){
  bc.location <- str_locate(tmp.text,"BUILDING CLASS: ")
  bc.line <- which(
    apply(bc.location,1,function(x){sum(is.na(x))}) == 0 
  )
  bc.start = bc.location[bc.line,2]
  build_class <- str_sub(tmp.text[bc.line],start=bc.start+1,end=bc.start+2)
  return(build_class)
}


bc.13_14 <- function(tmp.text){
  bc.location <- str_locate(tmp.text,"Building Class            ")
  bc.line <- which(
    apply(bc.location,1,function(x){sum(is.na(x))}) == 0 
  )
  bc.start = bc.location[bc.line,2]
  build_class <- str_sub(
    trimws(str_sub(tmp.text[bc.line],start=bc.start+1,end=bc.start+30))
    ,start=1
    ,end=2
  )
  return(build_class)
}

bc.12 <- function(tmp.text){
  bc.location <- str_locate(tmp.text,"BUILDING CLASS: ")
  # bc.location <- str_locate(tmp.text,"yo homie waddup")
  if(sum(!is.na(bc.location))>0){
    bc.line <- which(
      apply(bc.location,1,function(x){sum(is.na(x))}) == 0 
    )
    bc.start = bc.location[bc.line,2]
    build_class <- str_sub(tmp.text[bc.line],start=bc.start+1,end=bc.start+2)
  } else {
    build_class <- NA
  }
  return(build_class)
}


## NOTE: not all documents in 2010 - 2011 have building class 
bc.10_11 <- function(tmp.text){
  bc.location <- str_locate(tmp.text,"Building Class:")
  if(sum(!is.na(bc.location))>0){
    bc.line <- which(
      apply(bc.location,1,function(x){sum(is.na(x))}) == 0 
    )
    bc.start = bc.location[bc.line,2]
    build_class <- gsub(" .*","",
                        trimws(str_sub(tmp.text[bc.line],start=bc.start+1,end=bc.start+6))
    )
  } else {
    build_class <- NA
  }
  return(build_class)
}


bldg_class.fun <- function(tmp.text,doc_year){
  if(doc_year %in% c("2015","2016","2017","2018")){
    bldg_class <- bc.15_18(tmp.text)
  } else if(doc_year %in% c("2013","2014")){
    bldg_class <- bc.13_14(tmp.text)
  } else if(doc_year %in% c("2012")){
    bldg_class <- bc.12(tmp.text)
  } else if(doc_year %in% c("2010","2011")){
    bldg_class <- bc.10_11(tmp.text)
  } else{ 
    bldg_class <- NA
  }
  return(bldg_class)
}



# Gross square footage ----------------------------------------------------------


## function to pull just characters from the handy little table at the bottom of the pdfs
textblock.14_17 <- function(tmp.text){
  txtblck.location <- str_locate(tmp.text,regex("Finance has the following information on record for your property:",ignore_case=T))
  # if(sum(apply(txtblck.location,1,function(x){sum(!is.na(x))}))==0){
  #   txtblck.location <- str_locate(tmp.text, "Finance has the following information on record for your Property:")
  # }
  if(sum(apply(txtblck.location,1,function(x){sum(!is.na(x))}))>0){
    txtblck.line <- which(
      apply(txtblck.location,1,function(x){sum(is.na(x))}) == 0 
    )
    txtblck.start = txtblck.location[txtblck.line,2]
    txtblck.snippet <- str_sub(tmp.text[txtblck.line],start=txtblck.start,end=-1)
    txtblck.snippet <- gsub("If you believe.*","",txtblck.snippet)
  } else {
    txtblck.snippet <- tmp.text
  }
  return(txtblck.snippet)
}

## 2014-2017 has residential and commercial square footage in addition to building gross 
gs.14_17 <- function(tmp.text,bldgclass_broad){
  ## don't run if there's nothing there or the building class is a hotel
  if(sum(!is.na(tmp.text))>0 & !bldgclass_broad %in% "H"){
    
    ## use textblock function to get just the table at the bottom of the PDF
    tmp.text <- textblock.14_17(tmp.text)
    
    ## gross square footage
    if(!bldgclass_broad %in% "R"){
      gs.location <- str_locate(tmp.text,"Gross Square Footage: ")
      if(sum(apply(gs.location,1,function(x){sum(!is.na(x))}))>0){
        gs.line <- which(
          apply(gs.location,1,function(x){sum(is.na(x))}) == 0 
        )
        gs.start = gs.location[gs.line,2]
        gs.snippet <- str_sub(tmp.text[gs.line],start=gs.start+1,end=gs.start+125)
        gsf <- gsub("[^[:digit:]].*",""
                    ,trimws(gsub(",","",gs.snippet))
        )
      } else {
        gsf <- NA
      }
    } else {
      gs.location <- str_locate(tmp.text,regex("Square Footage from the Condo Declaration",ignore_case=T))
      
      if(sum(apply(gs.location,1,function(x){sum(!is.na(x))}))>0){
        gs.line <- which(
          apply(gs.location,1,function(x){sum(is.na(x))}) == 0 
        )
        gs.start = gs.location[gs.line,2]
        gs.snippet <- str_sub(tmp.text[gs.line],start=gs.start+1,end=gs.start+125)
        gsf <- gsub("[^[:digit:]].*",""
                    ,trimws(gsub(",","",gs.snippet))
        )
      } else {
        gsf <- NA
      }
    }
    
    ## gross resi square footage 
    grs.location <- str_locate(tmp.text,"Gross Residential Square Footage:")
    
    if(sum(apply(grs.location,1,function(x){sum(!is.na(x))}))==0){
      grs.location <- str_locate(tmp.text,"Gross Residential Sq. Footage:")
    } 
    
    if(sum(apply(grs.location,1,function(x){sum(!is.na(x))}))>0){
      
      grs.line <- which(
        apply(grs.location,1,function(x){sum(is.na(x))}) == 0 
      )
      grs.start <- grs.location[grs.line,2]
      grs.snippet <- str_sub(tmp.text[grs.line],start=grs.start+1,end=grs.start+125)
      
      grsf <- gsub("[^[:digit:]].*",""
                   ,trimws(gsub(",","",grs.snippet))
      )
    } else{
      grsf <- NA
    }
    
    ## gross commercial square footage 
    gcs.location <- str_locate(tmp.text,"Gross Commercial Square Footage:")
    if(sum(apply(gcs.location,1,function(x){sum(!is.na(x))}))==0){
      gcs.location <- str_locate(tmp.text,"Gross Commercial Sq. Footage:")
    } 
    
    if(sum(apply(gcs.location,1,function(x){sum(!is.na(x))}))>0){
      gcs.line <- which(
        apply(gcs.location,1,function(x){sum(is.na(x))}) == 0 
      )
      gcs.start <- gcs.location[gcs.line,2]
      gcs.snippet <- str_sub(tmp.text[gcs.line],start=gcs.start+1,end=gcs.start+125)
      
      gcsf <- gsub("[^[:digit:]].*",""
                   ,trimws(gsub(",","",gcs.snippet))
      )
    } else{
      gcsf <- NA
    }
    out <- cbind(gsf,grsf,gcsf)
  } else {
    out <- cbind(NA,NA,NA)
  }
  return(out)
}

## 2010 to 2013 don't have the more detailed information, just pulling square footage 
gs.10_13 <- function(tmp.text){
  gs.location <- str_locate(tmp.text,regex("Square Footage ",ignore_case=T))
  if(sum(apply(gs.location,1,function(x){sum(!is.na(x))}))==0){
    gs.location <- str_locate(tmp.text,regex("Square Footage:",ignore_case=T))
  }
  
  if(sum(apply(gs.location,1,function(j) sum(!is.na(j)))) > 0){
    gs.line <- which(
      apply(gs.location,1,function(j){sum(is.na(j))}) == 0 
    )
    gs.start <- gs.location[gs.line,2]
    gs.snippet <- str_sub(tmp.text[gs.line],start=gs.start+1,end=gs.start+125)
    gs.start <- regexpr("[[:digit:]]",gs.snippet)[1]
    
    gsf <- gsub("[^[:digit:]]",""
                ,str_sub(gs.snippet
                         ,start = gs.start
                         ,end= gs.start + 15
                )
    )
  } else {
    gsf <- NA
  }
  return(cbind(gsf,NA,NA))
}


## catch all for condo square footage
gs.sffcd <- function(tmp.text){
  if(sum(!is.na(tmp.text))>0){
    gs.location <- str_locate(tmp.text,regex("Square Footage from the Condo Declaration:",ignore_case=T))
    if(sum(apply(gs.location,1,function(x){sum(!is.na(x))}))==0){
      gs.location <- str_locate(tmp.text,regex("Square Footage of the Lot from the Condo Declaration:",ignore_case=T))
    }
    if(sum(apply(gs.location,1,function(x){sum(!is.na(x))}))>0){
      gs.line <- which(
        apply(gs.location,1,function(x){sum(is.na(x))}) == 0 
      )
      gs.start = gs.location[gs.line,2]
      gs.snippet <- str_sub(tmp.text[gs.line],start=gs.start+1,end=gs.start+125)
      gs.start <- regexpr("[[:digit:]]",gs.snippet)[1]
      
      gsf <- gsub("[^[:digit:]]",""
                  ,str_sub(gs.snippet
                           ,start = gs.start
                           ,end= gs.start + 15
                  )
      )
    } else {
      gsf <- NA
    }
    out <- cbind(gsf,NA,NA)
  } else {
    out <- cbind(NA,NA,NA)
  }
  return(out)
}


## catch all function for building square footage
gs.ebgf <- function(tmp.text){
  if(sum(!is.na(tmp.text))>0){
    gs.location <- str_locate(tmp.text,regex("Building Gross Square Footage:",ignore_case=T))
    
    if(sum(apply(gs.location,1,function(x){sum(!is.na(x))})) > 0){
      gs.line <- which(
        apply(gs.location,1,function(x){sum(is.na(x))}) == 0 
      )
      gs.start = gs.location[gs.line,2]
      gs.snippet <- str_sub(tmp.text[gs.line],start=gs.start+1,end=-1)
      gsf <- gsub("[^[:digit:]]","",
                  # trimws(gsub("\\n[^[:digit:]].*","",gs.snippet))
                  gsub("\\n[^[:digit:]].*","",gsub("     .*","",trimws(gs.snippet)))
      )
    } else {
      gsf <- NA
    }
    out <- cbind(gsf,NA,NA)
  } else {
    out <- cbind(NA,NA,NA)
  }
  return(out)
}


gsf.fun <- function(tmp.text,BldgClass.pdf,BldgClass_broad.pdf,doc_year){
  # gsf.fun <- function(x){
  if(doc_year %in% 2014:2017){
    gsf <- try(gs.14_17(tmp.text,BldgClass_broad.pdf))
  }
  
  if(doc_year %in% 2010:2013){
    gsf <- try(gs.10_13(tmp.text))
  }
  
  if(class(gsf)=="try-error"){
    if(BldgClass_broad.pdf %in% "R"){
      gsf <- gs.sffcd(tmp.text)
    } else{ 
      gsf <- gs.ebgf(tmp.text)
    }
  } else {
    if(is.na(gsf[1,1]) | !grepl("[[:digit:]]",gsf[1,1])){
      if(BldgClass_broad.pdf %in% "R"){
        gsf[1,1] <- gs.sffcd(tmp.text)[1,1]
      } else{ 
        gsf[1,1] <- gs.ebgf(tmp.text)[1,1]
        if(is.na(gsf[1,1]) | !grepl("[[:digit:]]",gsf[1,1])){
          gsf[1,1] <- gs.sffcd(tmp.text)[1,1]  
        }
      }
    }
  }
  return(gsf)
}


# Changes to property -----------------------------------------------------

change.fun <- function(tmp.text){
  change.list <- c("Demolition","Alteration","New Construction")
  
  ch.logic <- grepl(" X ",tmp.text) & grepl("Demolition",tmp.text,ignore.case=T)
  if(sum(ch.logic)==1){
    ch.line <- which(ch.logic)
    tmp.text <- tmp.text[ch.line]
    ch.location <- gregexpr("X ",tmp.text)[1]
    tmp.ch <- unlist(lapply(ch.location, function(j)
      str_sub(tmp.text
              ,start = j
              ,end= j + 20
      )
    )
    )
    
    out <- unlist(
      lapply(change.list, function(j)
        sum(grepl(j,tmp.ch))==1
      )
    )
  } else if(sum(ch.logic)==2){
    out <- c(NA,NA,NA)
  } else {
    out <- c(FALSE,FALSE,FALSE)
  }
  
  return(out)
}



# Property Address --------------------------------------------------------

## most documents have a second page with the property address in a convenient location
pa.primary <- function(tmp.text){
  pa.location <- str_locate(tmp.text,"Property Address:")
  
  if(sum(apply(pa.location,1,function(x){sum(!is.na(x))}))>0){
    
    pa.line <- which(
      apply(pa.location,1,function(x){sum(is.na(x))}) == 0 
    )
    
    pa.start <- pa.location[pa.line,2]+1
    pa.snippet <- trimws(str_sub(tmp.text[pa.line],start=pa.start,end=pa.start+200),which="left")
    prop_add <- trimws(
      str_sub(pa.snippet
              ,start=1
              ,end=regexpr("Borough",pa.snippet)[1]-1)
    )
  } else {
    prop_add <- NA
  }
  if(length(prop_add)==0){
    prop_add <- NA
  }
  return(prop_add)
}


## for the more troublesome ones, the location of property address varies by year 

pa.10_12 <- function(tmp.text){
  pa.location <- str_locate(tmp.text,"PROPERTY ADDRESS:")
  
  pa.line <- which(
    apply(pa.location,1,function(x){sum(is.na(x))}) == 0 
  )
  
  pa.start = pa.location[pa.line,2]
  pa.snippet <- trimws(str_sub(tmp.text[pa.line],start=pa.start+3,end=pa.start+200),which="left")
  
  prop_add <- trimws(
    str_sub(pa.snippet
            ,start = regexpr("      ",pa.snippet)[1]
            ,end=regexpr("\n",pa.snippet)[1]-1
    )
  )
  
  multi_space <- grepl("  ",prop_add)
  while(multi_space == T){
    prop_add <- gsub("  "," ",prop_add)
    multi_space <- grepl("  ",prop_add)
  }
  
  if(length(prop_add)==0){
    prop_add <- NA
  }
  return(prop_add)
}


# tmp.text <- tmp.list[[4]]$data
pa.13_14 <- function(tmp.text){
  # pa.location <- str_locate(tmp.text,regex("PROPERTY ADDRESS   ",ignore_case=T))
  pa.location <- str_locate(tmp.text,"Property Address   ")
  
  pa.line <- which(
    apply(pa.location,1,function(x){sum(is.na(x))}) == 0 
  )
  
  pa.start = pa.location[pa.line,2]
  pa.snippet <- trimws(str_sub(tmp.text[pa.line],start=pa.start+3,end=pa.start+225),which="left")
  
  prop_add <- trimws(
    str_sub(pa.snippet
            ,start = 1
            ,end=regexpr("Borough-Block-Lot",pa.snippet)[1] - 1
    )
  )
  
  multi_space <- grepl("  ",prop_add)
  while(multi_space == T){
    prop_add <- gsub("  "," ",prop_add)
    multi_space <- grepl("  ",prop_add)
  }
  
  if(length(prop_add)==0){
    prop_add <- NA
  }
  return(prop_add)
}

pa.15_17 <- function(tmp.text){
  pa.location <- str_locate(tmp.text,"PROPERTY ADDRESS")
  
  pa.line <- which(
    apply(pa.location,1,function(x){sum(is.na(x))}) == 0 
  )
  
  pa.start = pa.location[pa.line,2]
  pa.snippet <- trimws(str_sub(tmp.text[pa.line],start=pa.start+3,end=pa.start+225),which="left")
  
  prop_add <- trimws(
    str_sub(pa.snippet
            ,start = regexpr("      ",pa.snippet)[1]
            ,end=regexpr("BOROUGH",pa.snippet)[1]-1
    )
  )
  
  multi_space <- grepl("  ",prop_add)
  while(multi_space == T){
    prop_add <- gsub("  "," ",prop_add)
    multi_space <- grepl("  ",prop_add)
  }
  
  if(length(prop_add)==0){
    prop_add <- NA
  }
  return(prop_add)
}

## container function
prop_add.fun <- function(tmp.text,doc_year){
  prop_add <- try(pa.primary(tmp.text))
  if(is.na(prop_add) | length(prop_add)==0 | class(prop_add)=="try-error"){
    if(doc_year %in% c(2010,2011,2012)){
      prop_add <- try(pa.10_12(tmp.text))
    }
    if(doc_year %in% c(2013,2014)){
      prop_add <- try(pa.13_14(tmp.text))
    }
    if(doc_year %in% c(2015,2016,2016)){
      prop_add <- try(pa.15_17(tmp.text))
    }
  }
  if(length(prop_add)==0 | class(prop_add)=="try-error"){
    prop_add <- NA
  }
  return(prop_add)
}
