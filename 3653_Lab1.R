# 1. Wczytaj plik autoSmall.csv i wypisz pierwsze 5 wierszy

data <- read.csv("autaSmall.csv", encoding = "UTF-8")
head(data, 5)
nrow(data)

# 2. Pobierz dane pogodowe z REST API
install.packages("httr")
install.packages("jsonlite")

library(httr)
library(jsonlite)

endpoint <- "https://api.openweathermap.org/data/2.5/weather?q=Warszawa&appid=1765994b51ed366c506d5dc0d0b07b77"

getWeather <- GET(endpoint)
weatherText <- content(getWeather,"text")
View(weatherText)
weatherJSON<-fromJSON(weatherText)
wdf<- as.data.frame(weatherJSON)
View(wdf)

# 3. Napisz funkcję zapisującą porcjami danych plik csv do tabeli w SQLite
# Mały przykład - autaSmall.csv

install.packages("DBI")
install.packages("RSQLite")
library(DBI)
library(RSQLite)

con <- dbConnect(SQLite(), "auta2.sqlite")

readToBase<-function(filepath,con,tablename,size=100, sep=",",header=TRUE,delete=TRUE, encoding="UTF-8"){
  ap = !delete
  ov = delete
  
  fileCon <- file(description=filepath, open = "r", encoding = encoding)
  
  df1 <- read.table(fileCon, header = TRUE, sep=sep, fill=TRUE,
                    fileEncoding = encoding, nrows = size)
  if( nrow(df1)==0)
    return(0)
  myColNames <- names(df1)
  dbWriteTable(con, tablename, df1, append=ap, overwrite=ov)
  # zapis do bazy
  repeat{
    if(nrow(df1)==0){
      close(fileCon)
      dbDisconnect(con)
      break;
    }
    df1 <- read.table(fileCon, col.names = myColNames, sep=sep,
                      fileEncoding = encoding, nrows = size)
    dbWriteTable(con, tablename, df1, append=TRUE, overwrite=FALSE)
  }
}

readToBase("auta2.csv", con, "auta2", 1000)


#4.Napisz funkcję znajdującą tydzień obserwacji z największą średnią ceną ofert korzystając z zapytania SQL.

con <- dbConnect(SQLite(), "auta2.sqlite")
query <- "SELECT tydzien, avg_week_price 
          FROM 
          (
            SELECT tydzien, AVG(cena) as avg_week_price 
            FROM auta2
            GROUP BY tydzien
          ) 
          WHERE avg_week_price=(SELECT  max(avg_week_price) 
                        FROM (select tydzien, AVG(cena) as avg_week_price 
                              FROM auta2 GROUP BY tydzien))"
max_avg_week_price <- dbSendQuery(con, query)
dbFetch(max_avg_week_price)

dbClearResult(res)
dbDisconnect(con)


