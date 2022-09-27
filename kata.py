from FlightRadar24.api import FlightRadar24API
import pandas as pd

fr_api = FlightRadar24API()

def active_airplane(a_icao):
    return len(fr_api.get_flights(airline = a_icao))


def most_active_airline():
    df = pd.DataFrame(fr_api.get_airlines())
    df["Active_planes"] = df["ICAO"].apply(active_airplane)
    return df[df["Active_planes"] == df["Active_planes"].max()]

