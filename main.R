  # TODO: Description

# Change directory when calling source()
# e.g. source("path/main.R", chdir=T)



cat("Loading packages...\n")

library(RPostgreSQL)
library(yaml)
library(stringi)
library(stringr)
library(httr)
library(zipcode)
library(RDSTK)
library(ggmap)


Sys.sleep(3)

cat("Loading configuration file...\n")

config = yaml.load_file("config.yml")






connectToDb <- function(){
  cat("Establishing connection...\n")
  drv <- dbDriver(config$db$driver)
  con <<- dbConnect(drv, dbname=config$db$dbname, host=config$db$host, user=config$db$user, password=config$db$pass)
  # Test connection by listing available tables
  x <- dbGetInfo(con)
  tmp <- dbListTables(con)
  
  query <- "SELECT * FROM location"
  original <<- dbGetQuery(con, query)
  origCopy <<- original
  locs <<- original

  
  cat(paste("Found", nrow(locs), "rows in the location table.\n"))
  cat("----------------------------------------------\n")
  
  
  if(!('location' %in% tmp)){
    cat("Location table not found.\n")
  }else{
    cat("Connected!\n")
  }
  
  return(con)
}



compare <- function(locs, original){
  if( exists("locs")){
    
    if (nrow(locs) == nrow(original)){

      
      snap <- data.frame(names(locs))
      
      names(snap)[1] <- "var"
      
      snap$total <- ""
      snap$old.populated <- ""
      snap$new.populated <- ""
      snap$old.percentage <- ""
      snap$new.percentage <- ""
      snap$numIncrease <- ""
      snap$percentIncrease <- ""
      
      
      for ( i in 1:nrow(snap)){
        # Get columns 
        tmp <- locs[,i]
        old <- original[,i]
        
        # Get total rows 
        numTotal <- length(tmp)
        snap$total[i] <- numTotal
        
        # Num Populated
        oldPop <- length(old[old != "" & !is.na(old)])
        newPop <- length(tmp[tmp != "" & !is.na(tmp)])
        snap$old.populated[i] <- oldPop
        snap$new.populated[i] <- newPop
        
        # Percent Populated
        oldPercent <-  round((100 * (oldPop / numTotal)), digits=2)
        newPercent <-  round((100 * (newPop / numTotal)), digits=2)
        snap$old.percentage[i] <- oldPercent
        snap$new.percentage[i] <- newPercent
        
        # deltas
        snap$numIncrease[i] <- newPop - oldPop
        snap$percentIncrease[i] <- round(newPercent - oldPercent, digits=2)
        
      }
      print(snap)
      
      
    }else{
      cat("Tables do not have same columns")
    }
    
    
  }else{
    cat("Not connected to database.\n")
  }
    
  
}




getHelp <- function(){
  cat("Enter your database connection settings in 'config.yml' prior to using this script\n\n")
  
  cat("Available methods:\n")
  cat("------------------\n")
  cat("showHelp       :  displays relevant information\n")
  cat("clear          :  delete changes; revert to original\n")
  cat("quit           :  quit the program\n")
  cat("connect        :  connect to database, uses configuration yml file\n")
  cat("head           :  show the first six rows of the working table\n")
  cat("compare        :  compare density stats of data (original vs. current)\n")
  cat("cleanZip       :  cleans zip code data (strips suffixes, restores leading 0's, converts invalid to NA, changes to char vector)\n")
  cat("matchZip       :  adds missing zip codes by matching with city and state on 'zipcode' data set\n")
  cat("addColumns     :  adds latitude and longtiude columns to location table\n")
  cat("geocode dstk   :  geocode using Data Science Tool Kit API\n")
  cat("geocode census :  geocode using Census API\n")
  cat("geocode google :  gecode using Google API (volume constricted)\n")
  cat("lacking        :  displays up to 10 rows that have not been geocoded\n")
  cat("fullService    :  automated routine for geocoding\n")
  cat("commit         :  save changes to server\n")

  
  
  cat("\n")
}


cleanZip <- function(locs){
  if (exists("locs")){
    locs$zip <- clean.zipcodes(locs$zip)
    cat("Zip codes successfully cleaned")
  }else{
    cat("Location table not found.")
  }
  return(locs)
}


matchZip <- function(locs){
  
  
  if (exists("locs")){
    data(zipcode)
    # Set both to lowercase for better matching
    zipcode$city <- tolower(zipcode$city)
    locs$city <- tolower(locs$city)
    orig <- nrow(locs[is.na(locs$zip),])
    
    for(i in 1:nrow(locs)){
      if (is.na(locs$zip[i])){
        
        tmpVal <- zipcode[zipcode$city == locs$city[i] & zipcode$state == locs$state[i],]
        if (nrow(tmpVal) > 0 ){
          locs$zip[i] <- tmpVal$zip[1]
        }
      }
    }
    cat(paste("Successfully added", orig - nrow(locs[is.na(locs$zip),]), "zip codes"  ))
  
  }else{
    cat("Location table not found.")
  }
  
  return(locs)
}
  

addColumns <- function(locs){
  
  if (exists("locs")){
    if("latitude" %in% colnames(locs)){
      cat("Latitude column already exists\n")
    }else{
      locs$latitude <- ""
      original$latitude <<- ""
      cat("ADDED: Latitude column\n")
    }
    if("longitude" %in% colnames(locs)){
      cat("Longitude column already exists\n")
    }else{
      locs$longitude <- ""
      original$longitude <<- ""
      cat("ADDED: Longitude column\n")
    }
    
    
  }else{
    cat("Location table not found\n")
  }
  return(locs)
  
}
  
resetChanges <- function(original){
  locs <<- origCopy
  original <<- origCopy
  cat("Reverted to original")
}


geocode_dstk <- function(x){
  if (exists("locs")){
    
    if("latitude" %in% colnames(x) & "longitude" %in% colnames(x) ){
      

      cat("Starting DSTK Geocode...\n...\n")
      pb <- txtProgressBar(1, nrow(x),style=3)
      
     # orig <-  nrow(x[is.na(x$latitude) | x$latitude == "" ,])
      orig <- 0
      
      for( i in 1:nrow(x)){

        if(is.na(x$latitude[i])  | x$latitude[i] == ""){
          
          tryCatch({
            inp <- paste(x$address_1[i],",", x$city[i],",", x$state[i],x$zip[i]) 
            
            p <- street2coordinates(inp)
            
            if(nrow(p) > 0){
              
              x$latitude[i] <- p$latitude[1]
              x$longitude[i] <- p$longitude[1]
            }
          }, error=function(e){})
        }
       #setTxtProgressBar(pb, i)
        
      }
      cat("Completed.\n")
      cat(paste("Added coordinates to", orig - nrow(x[is.na(x$latitude) | x$latitude == "" ,]), "locations"))
      
    }else{
      cat("One or more coordinate columns missing")
    }
  }else{
    cat("Location table not found")
  }
  
  return(x)
}


geocode_census <- function(x){
  
  # Make sure input is data
  if (class(x) == "data.frame"){
    
    if("latitude" %in% colnames(x) & "longitude" %in% colnames(x) ){
      
      cat("Starting Census Geocode...\n...\n")
      
      p <- data.frame(x[is.na(x$latitude) | x$latitude =="", c(1,2,4,5,6)])
      colnames(p) <- c("Unique_ID","Street address","City","State", "Zip")
      
      orig <-  nrow(x[is.na(x$latitude) | x$latitude == "",])
      
      apiurl <- "http://geocoding.geo.census.gov/geocoder/geographies/addressbatch"
      file <- tempfile(fileext = ".csv")
      
      resTemp <- data.frame("location_id" = character(0), "latlon" = character(0))
      
      
      # Census does not take batches where n > 1000 
      # Break up the locations into n = 500 bins and send separately
      numBins <- ceiling(nrow(p) / 500)
      placeHolder <- 0
      
    
      pb <- txtProgressBar(0, numBins,style=3)
      
      for( i in 1:numBins){
        if (placeHolder + 500 > nrow(p)){
          tmpBin <- p[placeHolder:(nrow(p)),]
          
        }
        else{
          tmpBin <- p[placeHolder:(placeHolder+500),]
          placeHolder <- placeHolder + 501
        }
        
        
        
        write.table(tmpBin, file, row.names=FALSE, col.names=FALSE, sep=",")
        
        req <- POST(apiurl, body=list(
          addressFile = upload_file(file) 
         # benchmark = "Public_AR_Census2010",
        #  vintage = "Census2010_Census2010"
          
        ),
        encode = "multipart"
        )
        
        # Suppresses warning caused by census returning 3 columns when not found
        suppressWarnings(resp <-
                           content(
                             req,
                             as = "parsed",
                             type = "text/csv",
                             encoding = "UTF-8"
                           ))
        
        if(ncol(resp) > 3 ){
          # Workaround bug that sends a row to title
          resp <- rbind(resp, names(resp))
          resp <- resp[,c(1,6)]
          names(resp) <- c("location_id", "latlon")
          resTemp <- rbind(resTemp, resp)
          
        }
        setTxtProgressBar(pb, i)
      }

      
      # Separate lat/lon
      
      resTemp$latitude <- ""
      resTemp$longitude <- ""
      
      for (i in 1:nrow(resTemp)) {
        lonlat <- resTemp$latlon[i]
        
        if (!is.na(lonlat)) {
          # if there is a comma
          if (stri_detect_fixed(lonlat, ",")) {
            # split items into vector
            vec <- unlist(strsplit(lonlat, ","))
            
            resTemp$latitude[i] <- vec[2]
            resTemp$longitude[i] <- vec[1]
            
          }
        }
      }
      # Merge lat/lon into tables
      
      x$latitude <- with(resTemp, resTemp$latitude[match(x$location_id, resTemp$location_id)])
      x$longitude <- with(resTemp, resTemp$longitude[match(x$location_id, resTemp$location_id)])
      cat("Complete.\n")
      cat(paste("Added coordinates to", orig - nrow(x[is.na(x$latitude) | x$latitude == "", ]), "locations"))
      
      
      
    } else{
      cat("One or more coordinate columns missing")
    }
    
  } else{
    cat("Parameter must be of type data.frame")
  }
  
  return(x)
}


geocode_google <- function(locs){
  
  if (exists("locs")){
    
    if("latitude" %in% colnames(locs) & "longitude" %in% colnames(locs) ){
      require(ggmap)
      cat("Starting Google Geocode...\n...\n")
      orig <-  nrow(locs[is.na(locs$latitude) | locs$latitude =="",])
      
      
      
      counter = 0
      for (i in 1:nrow(locs)){
        
        if(is.na(locs$latitude[i]) | locs$latitude[i] == ""){
          
          addr <- paste(locs$address_1[i], ", ", locs$city[i], ", ", locs$state[i])
          x <- geocode(addr, output="more")
          
          # if zip is missing and response contains postal_code
          if (is.na(locs$zip[i]) & ("postal_code" %in% colnames(x))){
            locs$zip[i] <- as.character(x$postal_code)
          }
          # if latitude exists in response
          if ("lat" %in% colnames(x)){
            locs$latitude[i] <- as.character(x$lat)
          }
          # if longitude exists in response
          if ("lon" %in% colnames(x)){
            locs$longitude[i] <- as.character(x$lon)
          }
          
          counter = counter + 1
          
          if (counter > 20){
            # Use sleep to avoid Google's request per second restrictions
            Sys.sleep(5)
            counter = 0
          }
        }
      }
      
      
    }else{
      cat("One or more coordinate columns missing")
    }
    
  }else{
    cat("Location table not found")
  }
  cat("Completed\n")
  cat(paste("Added coordinates to", orig - nrow(locs[is.na(locs$latitude) | locs$latitude =="", ]), "locations"))
  return(locs)
}

showHead <- function(x){
  print(head(x))
}

commitChanges <- function(x){
  commitInput <- readline(prompt="Are you sure you want to commit changes?(yes/no)")
  if (commitInput == "no"){
    cat("Aborted commit")
  }
  else if (commitInput == "yes"){
    query <- "DROP TABLE IF EXISTS location_output;"
    tmp <- dbGetQuery(con, query)
    query <- "CREATE TABLE location_output (LIKE location);"
    tmp <- dbGetQuery(con, query)
    query <- "ALTER TABLE location_output ADD COLUMN latitude VARCHAR(20);"
    tmp <- dbGetQuery(con, query)
    query <- "ALTER TABLE location_output ADD COLUMN longitude VARCHAR(20);"
    tmp <- dbGetQuery(con, query)
    
    # write table to database
    dbWriteTable(con, "location_output", x, append=TRUE, row.names=FALSE)
    cat("Commit successful\n")
  }
  else{
    cat("Input not recognized")
    commitChanges()
  }
}



lacking <- function(x){
  y <- x[is.na(x$latitude) | x$latitude =="",]
  print( y[1:min(10, nrow(y)),])
}


fullService <- function(x){
  
  locs <<- matchZip(locs)
  locs <<- cleanZip(locs)
  locs <<- addColumns(locs)
  locs <<- geocode_census(locs)
  locs <<- geocode_dstk(locs)
  locs <<- geocode_google(locs)
  compare(locs, original)
}


getInput <- function(){
  input <- readline()
  
  if( input == "showHelp"){
    getHelp()
    getInput()
  }
  else if (input == "compare"){
    compare(locs, original)
    getInput()
  }
  else if (input == "quit"){
    rm(list=ls())
  }
  else if (input == "cleanZip"){
    locs <<- cleanZip(locs)
    getInput()
  }
  else if (input == "matchZip"){
    locs <<- matchZip(locs)
    getInput()
  }
  else if (input == "connect"){
    connectToDb()
    getInput()
  }
  else if (input == "addColumns"){
    locs <<- addColumns(locs)
    getInput()
  }
  else if (input =="clear"){
    resetChanges(original)
    getInput()
  }
  else if (input =="geocode dstk"){
    locs <<- geocode_dstk(locs)
    getInput()
  }
  else if (input =="geocode census"){
    locs <<- geocode_census(locs)
    getInput()
  }
  else if (input =="geocode google"){
    locs <<- geocode_google(locs)
    getInput()
  }
  else if (input == "head"){
    showHead(locs)
    getInput()
  }
  else if (input =="commit"){
    commitChanges(locs)
    getInput()
  }
  else if (input =="lacking"){
    lacking(locs)
    getInput()
  }
  else if (input == "fullService"){
    fullService(locs)
    getInput()
  }
  
  
  
  
  else if (nchar(input) > 0){
    cat("Input not recognized\n")
    getInput()
  }
  
  else{
    getInput()
  }
}



# --------------------------------------


connectToDb()


cat("\n----Welcome----\n\n")
cat("Type 'showHelp' for method options\n\n")



getInput()




