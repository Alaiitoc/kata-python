# kata-python

## FlightRadar24 Exercise

### Context
A data engineer often have to create and process ETL jobs and data pipelines.
For this project you'll have to ``extract`` data from https://www.flightradar24.com/ (a track of all flights world-wide, an airport and company listing).

After extracting the data you'll have to ``transform`` it (e.g pyspark, pandas, ...) to finaly implement answers to our questions.

Please commit early & often :)

### Accessing FlightRadar24 made easy in Python
The FlightRadarAPI project is available for you to simplify you queries to the FlightRadar24 API : https://github.com/JeanExtreme002/FlightRadarAPI 

### Questions

    - Q1: What is the company with the most active flights in the world ?
    - Q2: By continent, what are the companies with the most regional active flights (airports of Origin & Destination within the same continent) ?
    - Q3: World-wide, Which active flight has the longest route ?
    - Q4: By continent, what is the average route distance ? (flight localization by airport of origin)
    - Q5.1: Which leading airplane manufacturer has the most active flights in the world ?
    - Q5.2: By continent, what is the most frequent airplane model ? (airplane localization by airport of origin)
    - Q6: By company registration country, what are the tops 3 airplanes model flying ?
    - Q7.1: By continent, what airport is the most popular destination ?
    - Q7.2: What airport airport has the greatest inbound/outbound flights difference ? (positive or negative)
    - Q8: By continent, what is the average active flight speed ? (flight localization by airport of origin)

# Repo Explaination

## TODO

The kafka cluster works well in the dockers and it is possible to interact with it through the jupyter notebooks.
Next steps :  
- [] Dockerising consumer and producer app  
- [] connect the differents dockers   
- [] connect the streamlit docker with the others  
- [] put the data in docker volumes  
- [] actually use the data for the app  
- [] implement the back in pyspsark  

V2 :
- rewrite everything in a live table format (Delta Table/ Apache Iceberg)


## app 

Folder with the original solution in a dockerised streamlit app :

- requirements.txt 
- test.ipynb is a jupyter notebook used to test and understand the dataset
- ETL.ipynb is a jupyter notebook used to clean the dataset and create the files
- kata.py is the core of this repo with all the clean code
- wikiscraping.ipynb is the notebook used to scrap the useful wikipedia page
- Loading.py is a file used to update the data with the current flights
- The results are in the streamlit webapp launched with: ```streamlit run Results.py```
    - In the webapp, the questions are computed separatly and therefore it may take some time to get all the answers

## data

Folder with the different data files in parquet.

## kafka

Folder with the dockercompose file required to launch a kafka instance.
This folder also contains jupyter notebooks with python producer and consumer that can interact with the kafka cluster.

## producer 

Folder with the files to launch the Kafka producer API.

## consumer 

Folder with the files to launch the Kafka consumer API.

## pyspark

second version of the kata but wirh a spypsark implementation this time. (To be finished)