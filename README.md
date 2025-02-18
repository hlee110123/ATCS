# dbprofile analysis Package

This R package provides tools for analyzing healthcare utilization in OMOP CDM databases.

Analysis of Healthcare Data (2016–2024):

1. Prevalence Rates of Disease-Related Groups (DRG) (2016–2024)
# 2. Prescription Rates of Anatomical Therapeutic Chemical (ATC) Classifications (2016–2024)
3. Number of Individuals Across Domains (2016–2024)
4. Number of Occurrence Records Across Domains (2016–2024)
5. Prevalence Rates per 100,000 Patients (2016–2024)
6. Prescription Rates per 100,000 Patients (2016–2024)
7. Average Number of Records per Person in Each Table
8. Average Number of Records per Visit in Each Table
9. Distribution of Chronic vs. Acute Conditions
10. Ratio of Intensive Interventions
11. Prevalence of Comorbidity-Indicating Codes
12. Severity Scores (Elixhauser Comorbidity Index or Charlson Comorbidity Index)
    
# Installation: ATCS package from GITHUB 

```r
remotes::install_github("hlee110123/ATCS")
```
# How to run

```r
#install packages 
install.packages("DBI")
install.packages("DatabaseConnector")
install.packages("rJava")

#Load packages
library(rJava)
library(DBI)
library(DatabaseConnector)
library(ATCS)

#Verify change of Java setting in R Environment
Sys.getenv('JAVA_OPTIONS')

#Set your JAVA_HOME environment variable (set to path where Java was installed)
Sys.setenv(DATABASECONNECTOR_JAR_FOLDER = "#your JAR FOLDER directory") 

#Setup the connection details for your OMOP instance
connectionDetails <- createConnectionDetails(   
  dbms = #your dbms,   
  server = #your server,   
  user = #your username,   
  password = #your password,   
  port = #your port number,   
  pathToDriver = Sys.getenv('DATABASECONNECTOR_JAR_FOLDER')) ## Establish a connection using the DatabaseConnector "connect" function 

#Connect to the database
conn <- connect(connectionDetails)

# Get statistics for all ATC categories (2016-2024)
all_stats <- get_all_atc_stats(conn, "dbo")

# Save as CSV
write.csv(all_stats, "atc_prescription_rates_2016_2024.csv", row.names = FALSE)

```

## Notes
Once you've completed the analysis, please export the results to "atc_prescription_rates_2016_2024.csv" and kindly share it as an email attachment. Thank you! 

Email address: "hlee292@jh.edu"
