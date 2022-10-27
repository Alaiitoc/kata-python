from FlightRadar24.api import FlightRadar24API
import pandas as pd
from geopy import distance


def same_continent(x):
    df_airports = pd.read_parquet("data/Airports.parquet")
    dest = x["dest_iata"]
    org = x["org_iata"]
    try :
        if df_airports[df_airports["iata"] == org]["Continent"].iloc[0] == df_airports[df_airports["iata"] == dest]["Continent"].iloc[0]:
            return df_airports[df_airports["iata"] == org]["Continent"].iloc[0]
        else :
            return None
    except IndexError as e:
        return None

def airport_dist(x):
    df_airports = pd.read_parquet("data/Airports.parquet")
    org = x["org_iata"]
    dest = x["dest_iata"]
    try :
        coord_org = (df_airports[df_airports["iata"] == org]["lat"].iloc[0], df_airports[df_airports["iata"] == org]["lon"].iloc[0])
        coord_dest = (df_airports[df_airports["iata"] == dest]["lat"].iloc[0], df_airports[df_airports["iata"] == dest]["lon"].iloc[0])
        return distance.distance(coord_org, coord_dest).km
    except IndexError as e:
        return None

def cont_org(x):
    df_airports = pd.read_parquet("data/Airports.parquet")
    try :
        return df_airports[df_airports["iata"] == x["org_iata"]]["Continent"].iloc[0]
    except IndexError:
        return None

def company_country(x):
    airline = pd.read_parquet("data/AirlineCountries.parquet", columns=["ICAO","Country"])
    try :
        return airline[airline["ICAO"] == x["company"]]["Country"].iloc[0]
    except IndexError:
        return None


#Q1

def most_active_airline():
    df_airlines = pd.read_parquet("data/Airlines.parquet")
    df_flights = pd.read_parquet("data/Flights.parquet", columns=["company"])

    count = df_flights.value_counts()
    if not count.empty:
        return(df_airlines[df_airlines["ICAO"] == count.index[0][0]].iloc[0]["Name"] , count.max())

#Q2

def active_by_continent():
    df= pd.read_parquet("data/Flights.parquet",columns=["dest_iata","org_iata","company"])

    df["Continent"] = df.apply(same_continent, axis=1)
    return(df)
            


#Q3 

def longuest_journey(): 
    df3 = pd.read_parquet("data/Flights.parquet",columns=["id","dest_iata","org_iata"])

    df3["travel_size"] = df3.apply(airport_dist, axis=1)
    return df3

#Q4 

def average_journey():
    fr_api = FlightRadar24API()
    df4 = pd.read_parquet("data/Flights.parquet",columns=["id","dest_iata","org_iata"])

    df4["travel_size"] = df4.apply(airport_dist, axis=1)
    df4["org_Continent"] = df4.apply(cont_org, axis=1)
    return(df4)


#Q5.1



def leading_manufacturer():
    df_flights = pd.read_parquet("data/Flights.parquet", columns=["model"])
    return df_flights
    

#Q5.2

def continent_manufacturer():
    df5 = pd.read_parquet("data/Flights.parquet",columns=["id","dest_iata","org_iata","model"])
    
    df5["org_Continent"] = df5.apply(cont_org, axis=1)
    return df5


#Q6

def flying_models():
    # Flights data
    df6 = pd.read_parquet("data/Flights.parquet", columns=["model","company"])
    df6["company_country"] = df6.apply(company_country,axis=1)
    df6.dropna(inplace=True)
    df6.reset_index(allow_duplicates=False, inplace=True)

    # List of countries with active companies
    countries = df6[["company_country"]].drop_duplicates()
    countries.dropna(inplace=True)
    countries = countries["company_country"].to_list()

    dic = {"Company registration country":[],"model_1":[],"number_1":[],"model_2":[],"number_2":[],"model_3":[],"number_3":[]}

    for country in countries:
        models = df6[df6["company_country"] == country].value_counts("model")
        
        if len(models) == 0:
            pass
        elif len(models) == 1:
            dic["Company registration country"].append(country)
            dic["model_1"].append(models.index[0])
            dic["number_1"].append(models[0])
            dic["model_2"].append("")
            dic["number_2"].append("")
            dic["model_3"].append("")
            dic["number_3"].append("")

        elif len(models) == 2:
            dic["Company registration country"].append(country)
            dic["model_1"].append(models.index[0])
            dic["number_1"].append(models[0])
            dic["model_2"].append(models.index[1])
            dic["number_2"].append(models[1])
            dic["model_3"].append("")
            dic["number_3"].append("")

        elif len(models) == 3:
            dic["Company registration country"].append(country)
            dic["model_1"].append(models.index[0])
            dic["number_1"].append(models[0])
            dic["model_2"].append(models.index[1])
            dic["number_2"].append(models[1])
            dic["model_3"].append(models.index[2])
            dic["number_3"].append(models[2])

    df_answer6 = pd.DataFrame(dic)
    df_answer6 = df_answer6.sort_values(by="Company registration country")
    return(df_answer6)

#Q7.1


def popular_destination():
    df_airports = pd.read_parquet("data/Airports.parquet")
    df7 = pd.read_parquet("data/Flights.parquet",columns=["id","dest_iata","org_iata"])

    df7["travel_size"] = df7.apply(airport_dist, axis=1)
    df7["org_Continent"] = df7.apply(cont_org, axis=1)
    return df7
    

#Q7.2

def inbounds():
    df_airports = pd.read_parquet("data/Airports.parquet")
    df72 = pd.read_parquet("data/Flights.parquet",columns=["org_iata","dest_iata"])
    df_inbounds = pd.read_parquet("data/Airports.parquet",columns=["iata","Continent"])
    df_inbounds["inbounds"] = [0]*len(df_inbounds.index)

    for i in range(len(df72.index)):
        flight = df72.iloc[i]
        try :
            df_inbounds.iat[df_inbounds[df_inbounds["iata"] == flight["org_iata"]]["inbounds"].index[0],2] -=1
            df_inbounds.iat[df_inbounds[df_inbounds["iata"] == flight["dest_iata"]]["inbounds"].index[0],2] +=1
        except :
            pass
    
    df_inbounds["inbounds"] = df_inbounds["inbounds"].apply(lambda x: abs(x))
    return df_inbounds
    


#Q8

def average_speed():
    fr_api = FlightRadar24API()
    df8 = pd.read_parquet("data/Flights.parquet",columns=["org_iata","speed"])

    df8["org_Continent"] = df8.apply(cont_org, axis=1)
    return df8
