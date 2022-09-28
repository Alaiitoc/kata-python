from FlightRadar24.api import FlightRadar24API
import pandas as pd
from geopy import distance


fr_api = FlightRadar24API()

# Airports and airlines are data that don't really change
df_airports = pd.read_parquet("Airports.parquet")
df_airlines = pd.read_parquet("Airlines.parquet")


def same_continent(x):
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
    org = x["org_iata"]
    dest = x["dest_iata"]
    try :
        coord_org = (df_airports[df_airports["iata"] == org]["lat"].iloc[0], df_airports[df_airports["iata"] == org]["lon"].iloc[0])
        coord_dest = (df_airports[df_airports["iata"] == dest]["lat"].iloc[0], df_airports[df_airports["iata"] == dest]["lon"].iloc[0])
        return distance.distance(coord_org, coord_dest).km
    except IndexError as e:
        return None

def cont_org(x):
    try :
        return df_airports[df_airports["iata"] == x["org_iata"]]["Continent"].iloc[0]
    except IndexError:
        return None


# Get important data on current flights

def active_airplane(airline_icao):
    flight_list=[]
    flights = fr_api.get_flights(airline = airline_icao)
    for flight in flights :
        try :
            dest_iata = flight.destination_airport_iata
        except :
            dest_iata = None
        try :
            org_iata = flight.origin_airport_iata
        except :
            org_iata = None
        try :
            model = flight.aircraft_code
        except :
            model = None 
        try :
            registration = flight.registration
        except :
            registration = None
        try :
            speed = flight.ground_speed
        except :
            speed = None

        f = {
            "id" : flight.id,
            "dest_iata" : dest_iata,
            "org_iata" : org_iata,
            "model" : model, 
            "registration" : registration,
            "speed" : speed,
            "company" : airline_icao
            }
        flight_list.append(f)
    return flight_list

#Update active airplanes
active_flights = []
for airline in df_airlines["ICAO"]: # since the API only gave 1500 flights on one request I choose to request flights by airlines
    active_flights += active_airplane(airline)
df_flights = pd.DataFrame(active_flights)
df_flights = df_flights[df_flights["dest_iata"] != 'N/A'] #clean data
df_flights = df_flights[df_flights["org_iata"] != 'N/A']
df_flights.to_parquet("Flights.parquet")

#Q1

def most_active_airline():
    count = df_flights["company"].value_counts()
    if not count.empty:
        print(df_airlines[df_airlines["ICAO"] == count.index[0]].iloc[0]["Name"] , ":", count.max(), "planes")

#Q2

def active_by_continent():
    df= pd.read_parquet("Flights.parquet",columns=["dest_iata","org_iata","company"])
    df["Continent"] = df.apply(same_continent, axis=1)
    for continent in fr_api.get_zones().keys():
        count = df[df["Continent"] == continent]["company"].value_counts()
        if not count.empty:
            print(continent.upper(), ":\n", df_airlines[df_airlines["ICAO"] == count.index[0]].iloc[0]["Name"] , ":", count.max(), "planes")


#Q3 

def longuest_journey(): 
    df3 = pd.read_parquet("Flights.parquet",columns=["id","dest_iata","org_iata"])
    df3["travel_size"] = df3.apply(airport_dist, axis=1)
    print("The flight with the longuest journey started at",end=" ")
    print(df_airports[df_airports["iata"] == df3.max()["org_iata"]].iloc[0]["name"],end=" ")
    print("and will end at",end=" ")
    print(df_airports[df_airports["iata"] == df3.max()["dest_iata"]].iloc[0]["name"],end=" ")
    print(f'for a journey of {df3.max()["travel_size"]} km')

#Q4 

def average_journey():
    df4 = pd.read_parquet("Flights.parquet",columns=["id","dest_iata","org_iata"])
    df4["travel_size"] = df4.apply(airport_dist, axis=1)
    df4["org_Continent"] = df4.apply(cont_org, axis=1)
    print("The average route distance is :")
    for continent in fr_api.get_zones().keys():
        print(continent.upper(), ":\n", df4[df4["org_Continent"] == continent]["travel_size"].mean(), "km")


#Q5.1

'''
I didn't find a way to link manufacturers to models yet
'''

def leading_manufacturer():
    print(df_flights["model"].value_counts().index[0], df_flights["model"].value_counts().iloc[0])

#Q5.2

def continent_manufacturer():
    df5 = pd.read_parquet("Flights.parquet",columns=["id","dest_iata","org_iata","model"])
    df5["org_Continent"] = df5.apply(cont_org, axis=1)
    print("The average route distance is :")
    for continent in fr_api.get_zones().keys():
        count = df5[df5["org_Continent"] == continent]["model"].value_counts()
        if not count.empty:
            print(continent.upper(), ":\n", count.index[0], count.iloc[0],"planes")

#Q6

'''
I didn't find a way to link companies to countries yet
'''


#Q7.1


def popular_destination():
    df4 = pd.read_parquet("Flights.parquet",columns=["id","dest_iata","org_iata"])
    df4["travel_size"] = df4.apply(airport_dist, axis=1)
    df4["org_Continent"] = df4.apply(cont_org, axis=1)
    for continent in fr_api.get_zones().keys():
        count = df4[df4["org_Continent"] == continent]["dest_iata"].value_counts()
        if not count.empty:
            print(continent.upper(), ":\n", "To", df_airports[df_airports["iata"] == count.index[0]].iloc[0]["name"] , ":", count.max(), "flights")

#Q7.2

def inbounds():
    df72 = pd.read_parquet("Flights.parquet",columns=["org_iata","dest_iata"])
    df_inbounds = pd.read_parquet("Airports.parquet",columns=["iata","Continent"])
    df_inbounds["inbounds"] = [0]*len(df_inbounds.index)

    for i in range(len(df72.index)):
        flight = df72.iloc[i]
        try :
            df_inbounds.iat[df_inbounds[df_inbounds["iata"] == flight["org_iata"]]["inbounds"].index[0],2] -=1
            df_inbounds.iat[df_inbounds[df_inbounds["iata"] == flight["dest_iata"]]["inbounds"].index[0],2] +=1
        except :
            pass
    
    df_inbounds["inbounds"] = df_inbounds["inbounds"].apply(lambda x: abs(x))
    print("Greatest inbound/outbound flights difference at ", df_airports[df_airports["iata"] == df_inbounds["iata"].max()].iloc[0]["name"], end=" ")
    print("with a différence of", df_inbounds["inbounds"].max())


#Q8

def average_speed():
    df8 = pd.read_parquet("Flights.parquet",columns=["org_iata","speed"])
    df8["org_Continent"] = df8.apply(cont_org, axis=1)
    for continent in fr_api.get_zones().keys():
        count = df8[df8["org_Continent"] == continent]["speed"].mean()
        if count :
            print(continent.upper(), ":\n", "Avearage speed", count, "km/h")