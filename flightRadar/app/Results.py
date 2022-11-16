import os
import time
import streamlit as st
import pandas as pd
import requests

from FlightRadar24.api import FlightRadar24API
from kata import most_active_airline, active_by_continent, longuest_journey, average_journey,\
    leading_manufacturer, continent_manufacturer, flying_models, popular_destination, inbounds, average_speed
from producer.Loading import update_planes


#caching results
@st.cache
def kata(func):
    return func()

#Loading some data 
df_airlines = pd.read_parquet("data/Airlines.parquet")
df_airports = pd.read_parquet("data/Airports.parquet")
fr_api = FlightRadar24API()
continents = fr_api.get_zones().keys()


st.set_page_config(
    page_title="FlightRadar Kata", page_icon=":airplane:", initial_sidebar_state="expanded"
)

# CSS to inject contained in a string
hide_table_row_index = """
            <style>
            thead tr th:first-child {display:none}
            tbody th {display:none}
            </style>
            """

# Inject CSS with Markdown
st.markdown(hide_table_row_index, unsafe_allow_html=True)

st.title("""
FlightRadar Kata
""")

st.subheader("Number of planes flying currently : " + str(len(pd.read_parquet("data/Flights.parquet").index)))

#                       --- UPDATE BUTTON ---

URL = "urlfromapp_consumerœp"

def fetch(session, url):
    try : 
        result = session.get(url)
    except Exception:
        return {}


if st.button(label="Update Live data !"):
    session = requests.Session()
    if fetch(session, URL):
    # if update_planes() : 
        st.info("Flight list updated !")
st.write("Last update : ", time.ctime(os.path.getmtime("data/Flights.parquet")))
st.write("This operation may take a few minutes")

st.header("""
Questions :
""")

#                           --- QUESTIONS ---


with st.expander("Q1 : What is the company with the most active flights in the world ?"):
    if st.checkbox("Answer Q1", key="1") :
        company,number = kata(most_active_airline)
        st.write(f"The company with the highest number of active flights in the world is the **{company}** with", number ,"active flights !")


with st.expander("Q2: By continent, what are the companies with the most regional active flights (airports of Origin & Destination within the same continent) ?"):
    if st.checkbox("Answer Q2",key="2") :
        df_2 = kata(active_by_continent)
        for continent in continents:
            count = df_2[df_2["Continent"] == continent]["company"].value_counts()
            if not count.empty:
                st.subheader(continent.upper())
                st.write(df_airlines[df_airlines["ICAO"] == count.index[0]].iloc[0]["Name"] , ":", count.max(), "active planes")
                

with st.expander("Q3: World-wide, Which active flight has the longest route ?"):
    if st.checkbox("Answer Q3",key="3"):
        df_3 = kata(longuest_journey)
        txt = ""
        txt += "The flight with the longuest journey started at : "
        txt += "**" + df_airports[df_airports["iata"] == df_3.max()["org_iata"]].iloc[0]["name"] + "**\n"
        txt += "and will end at : "
        txt += "**" + df_airports[df_airports["iata"] == df_3.max()["dest_iata"]].iloc[0]["name"] + "**\n"
        txt += 'for a journey of' 
        st.write(txt, (df_3.max()["travel_size"]),  'km')


with st.expander("Q4: By continent, what is the average route distance ? (flight localization by airport of origin)"):
    if st.checkbox("Answer Q4",key="4"):
        df_4 = kata(average_journey)
        st.subheader("The average route distance is :")
        for continent in fr_api.get_zones().keys():
            count = df_4[df_4["org_Continent"] == continent]["travel_size"]
            if not count.empty:
                st.subheader(continent.upper())
                st.write(count.mean(), "km")


with st.expander("Q5.1: Which leading airplane manufacturer has the most active flights in the world ?"):
    if st.checkbox("Answer Q5.1",key="5.1"):
        df_51 = kata(leading_manufacturer)
        st.write("The airplane manufacturer with the most active planes is ", df_51.value_counts().index[0][0],\
                "with ",df_51.value_counts().iloc[0], "actives flights")


with st.expander("Q5.2: By continent, what is the most frequent airplane model ? (airplane localization by airport of origin)"):
    if st.checkbox("Answer Q5.2",key="5.2"):
        df_52 = kata(continent_manufacturer)
        for continent in continents:
            count = df_52[df_52["org_Continent"] == continent]["model"].value_counts()
            if not count.empty:
                st.subheader(continent.upper())
                st.write(count.index[0], " : ", count.iloc[0]," planes")
            

with st.expander("Q6: By company registration country, what are the tops 3 airplanes model flying ?"):
    if st.checkbox("Answer Q6",key="6"):
        df_6 = kata(flying_models)
        for country in range(len(df_6.index)):
            if st.checkbox(df_6["Company registration country"].iloc[country]):
                st.write(df_6["model_1"].iloc[country], " : ", df_6["number_1"].iloc[country])
                if df_6["model_2"].iloc[country] : st.write(df_6["model_2"].iloc[country], " : ", df_6["number_2"].iloc[country])
                if df_6["model_3"].iloc[country] : st.write(df_6["model_3"].iloc[country], " : ", df_6["number_3"].iloc[country])

            

with st.expander("Q7.1: By continent, what airport is the most popular destination ?"):
    if st.checkbox("Answer Q7.1",key="7.1"):
        df_7 = kata(popular_destination)
        for continent in continents:
            count = df_7[df_7["org_Continent"] == continent]["dest_iata"].value_counts()
            if not count.empty:
                st.subheader(continent.upper())
                st.write("To", df_airports[df_airports["iata"] == count.index[0]].iloc[0]["name"] , ":", count.max(), "flights")


with st.expander("Q7.2: What airport airport has the greatest inbound/outbound flights difference ? (positive or negative)"):
    if st.checkbox("Answer Q7.2",key="7.2"):
        df_72 = kata(inbounds)
        st.write("Greatest inbound/outbound flights difference at ", df_airports[df_airports["iata"] == df_72["iata"].max()].iloc[0]["name"], \
                "with a difference of", df_72["inbounds"].max())

with st.expander("Q8: By continent, what is the average active flight speed ? (flight localization by airport of origin)"):
    if st.checkbox("Answer Q8",key="8"):
        df_8 = kata(average_speed)
        for continent in continents:
            count = df_8[df_8["org_Continent"] == continent]["speed"]
            if not count.empty :
                st.subheader(continent.upper())
                st.write("Avearage speed", count.mean(), "km/h")


st.write("[Link to the site flightRadar website](https://www.flightradar24.com/)")
st.write("[Link to the github kata](https://gitlab.com/exalt-it-dojo/katas-python-data/-/tree/main/flightradar24)")
st.write("[Link ot my github](https://github.com/Alaiitoc/kata-python)")