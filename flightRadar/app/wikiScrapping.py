""" 
Script to get all correspondances between airlines ICAO and country names from Wikipedia
"""

#                       ---  IMPORTS  ---                       #

from bs4 import BeautifulSoup
import requests
import pandas as pd

#                       ---  SCRAPING TABLES  ---                       #

URL = 'https://en.wikipedia.org/wiki/List_of_airline_codes'

page = requests.get(URL)
soup = BeautifulSoup(page.content, 'html.parser')

tables = soup.findAll("table", { "class" : "wikitable" })
table  = str(tables[0])

table = table.replace("\n",'')
table = table.replace("</tr>", '')
table = table.replace("</td>", '')
table = table.replace("n/a", '')

data  = [r.split('<td>') for r in table.split("<tr>")][2:]

df = pd.DataFrame(data)[[2,3,5,6]]
df.rename(columns={2:"ICAO",5:"Country",3:"Name",6:"Comments"}, inplace=True)




#                       ---  Drop all airlines that doesn't exist anymore  ---                      #

df1 = df.drop_duplicates(subset=["Name"]).dropna()
df1 = df1[["ICAO","Country","Comments"]]

to_drop = []
to_drop += list(df1[df1["Comments"].str.contains("Ceased")].index)
to_drop += list(df1[df1["Comments"].str.contains("ceased")].index)
to_drop += list(df1[df1["Comments"].str.contains("defunct")].index)
to_drop += list(df1[df1["Comments"].str.contains("Defunct")].index)
to_drop += list(df1[df1["Comments"].str.contains("no")].index)
to_drop += list(df1[df1["Comments"].str.contains("No")].index)
to_drop += list(df1[df1["Comments"].str.contains("merged")].index)
to_drop += list(df1[df1["Comments"].str.contains("Merged")].index)
to_drop += list(df1[df1["Comments"].str.contains("DEFUNCT")].index)

df1 = df1.drop(to_drop).dropna(subset=["ICAO","Country"]).sort_values("ICAO").reset_index(drop=True).drop(list(range(98)))
df1 = df1.drop_duplicates(["ICAO","Country"])

#                       ---  Fix hyperlinks issues  ---                     #

df1 = df1.reset_index(drop=True)
df1["Country"].iat[3767] = "Moldova"
df1["Country"].iat[1576] = "Egypt"
df1["Country"].iat[2886] = "Malta"
df1["Country"].iat[4904] = "Republic of Korea"
df1["ICAO"].iat[3767] = "PVV"

#                       ---  FIX COUNTRIES NAMES   ---                      #

def correctWiki(df,nameWiki,nameCountry):
    L_country = list(df[df["Country"].str.contains(nameWiki, case=False)].index)
    for l in L_country:
        df["Country"].iloc[l] = nameCountry

correctWiki(df,"congo","Congo")
correctWiki(df,"turks","Turks And Caicos Islands")
correctWiki(df,"São","Sao Tome And Principe")
correctWiki(df,"Republic of Korea","South Korea")
correctWiki(df,"Lao","Laos")
correctWiki(df,"Trinidad","Trinidad And Tobago")
correctWiki(df,"Somali","Somalia")
correctWiki(df,"gambia","Gambia")
correctWiki(df,"Syria","Syria")
correctWiki(df,"Syria","Syria")
correctWiki(df,"Russia","Russia")
correctWiki(df,"Syria","Syria")
correctWiki(df,"Myanmar","Myanmar (Burma)")
correctWiki(df,"Syria","Syria")
correctWiki(df,"ivory","Cote D'ivoire (Ivory Coast)")
correctWiki(df,"ivoire","Cote D'ivoire (Ivory Coast)")
correctWiki(df,"Czech","Czechia")
correctWiki(df,"Molodva","Moldova")
correctWiki(df,"Canada","Canada")
correctWiki(df,"Hong","Hong Kong")
correctWiki(df,"Netherlands","Netherlands")
correctWiki(df,"Burma","Myanmar (Burma)")
correctWiki(df,"Burkino Faso","Burkina Faso")
correctWiki(df,"Bosnia and Herzegovina","Bosnia And Herzegovina")
correctWiki(df,"Antigua and Barbuda","Antigua and Barbuda")
correctWiki(df,"Macedonia","North Macedonia")
df = df.drop([5053])

#                       ---  SAVE  ---                      #

df1.to_parquet("../data/AirlineCountries.parquet")