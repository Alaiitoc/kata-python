from FlightRadar24.api import FlightRadar24API
import pandas as pd

# Get important data on current flights

def active_airplane(airline_icao):
    fr_api = FlightRadar24API()
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

def update_planes():
    df_airlines = pd.read_parquet("data/Airlines.parquet")
    active_flights = []
    for airline in df_airlines["ICAO"]: # since the API only gave 1500 flights on one request I choose to request flights by airlines
        active_flights += active_airplane(airline)
    df_flights = pd.DataFrame(active_flights)
    df_flights = df_flights[df_flights["dest_iata"] != 'N/A'] #clean data
    df_flights = df_flights[df_flights["org_iata"] != 'N/A']

    df_flights.to_parquet("data/Flights.parquet")
    return True