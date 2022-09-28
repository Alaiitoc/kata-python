from FlightRadar24.api import FlightRadar24API
import pandas as pd
from geopy import distance


fr_api = FlightRadar24API()

# Airports and airlines are data that don't really change
df_airports = pd.read_parquet("Airports.parquet")
df_airlines = pd.read_parquet("Airlines.parquet")


def active_airplane(a_icao): # number of active airplanes of a company
    return len(fr_api.get_flights(airline = a_icao))

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
            dest_icao = flight.destination_airport_icao
        except :
            dest_icao = None
        try :
            org_icao = flight.origin_airport_icao
        except :
            org_icao = None
        try :
            model = flight.aircraft_model
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
            "dest_icao" : dest_icao,
            "org_icao" : org_icao,
            "dest_iata" : dest_iata,
            "org_iata" : org_iata,
            "model" : model,
            "registration" : registration,
            "speed" : speed,
            "company" : airline_icao
            }
        flight_list.append(f)
    return flight_list

active_flights = []
for airline in df_airlines["ICAO"]:
    active_flights += active_airplane(airline)
df_flights = pd.DataFrame(active_flights)
df_flights.to_parquet("Flights.parquet")

#Q1

def most_active_airline():
    df = pd.DataFrame(fr_api.get_airlines())
    df["Active_planes"] = df["ICAO"].apply(active_airplane)
    return df[df["Active_planes"] == df["Active_planes"].max()]

#Q2

def active_by_continent():
    df= pd.read_parquet("Flights.parquet",columns=["dest_iata","org_iata","company"])
    df["Continent"] = df.apply(same_continent, axis=1)
    for continent in fr_api.get_zones().keys():
        count = df[df["Continent"] == continent]["company"].value_counts()
        if not count.empty:
            print(continent.upper(), ":\n", df_airlines[df_airlines["ICAO"] == count.index[0]].iloc[0]["Name"] , ":", count.max())


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
    print("The average route distance is :")
    for continent in fr_api.get_zones().keys():
        print(continent.upper(), ":\n", df4[df4["Continent"] == continent]["travel_size"].mean(), "km")