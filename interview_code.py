import numpy as np
import pandas as pd
from collections import defaultdict
import aiohttp
import asyncio
from datetime import datetime
from dateutil.relativedelta import relativedelta
import matplotlib.pyplot as plt

'''We define the global variables here, we get the month of the 
   last dataset in the format 2022-12,
   we define the API endpoints and the location polygon'''
currentTimeDate = datetime.now() - relativedelta(months=2)
pastDate = currentTimeDate.strftime("%Y-%m")
URL_C = "https://data.police.uk/api/crimes-street/all-crime"
POLY_LOC = "52.268,0.543:52.794,0.238:52.130,0.478"
PARAMS_C = {"date": pastDate, "poly": POLY_LOC}
URL_N = "https://data.police.uk/api/locate-neighbourhood"



'''Asynchronously hit the API ends points using Asyncio'''
async def getdata(url, params):
   async with aiohttp.ClientSession() as session:
      async with session.get(url=url, params=params) as resp:
         response = await resp.json()
         return response

'''We populate a dictionairy of lists by getting the outcome 
   and crime fron the Crime API
   and the force from the Neighbourhood API'''
async def populate_dict(results):
    d = defaultdict(list)
    for result in results:
         try:
            latitude = result["location"]["latitude"]
            longitude = result["location"]["longitude"]
            PARAMS_N = {"q": f"{latitude},{longitude}"}
            resp = await getdata(URL_N, PARAMS_N)
            if result["outcome_status"] and "category" in result["outcome_status"]:
              d["OUTCOME"].append(result["outcome_status"]["category"])
            else:
              d["OUTCOME"].append(None)
            d["CRIME"].append(result["category"])
            d["FORCE"].append(resp["force"])
         except KeyError as ke:
           raise KeyError('A key is missing') 
    return d

'''We display the police forces which do not provide the outcome, 
   we create a dataframe without the null values in the outcome column, 
   we group by crime category to find the number of occurences and 
   create a matplotlib plot'''
def display_results(df):
   df_police_stations = df[["FORCE"]][df["FLG"] == 1]
   df_police_stations = df_police_stations["FORCE"].unique()
   print(df_police_stations)

   df = df[df["OUTCOME"].notna()]

    
   df_crime = df.groupby("CRIME")["CRIME"].count().reset_index(name="COUNT")
   df_crime.plot.bar(x="CRIME", y="COUNT", fontsize="4")
   plt.show()  # display the graph

'''This is the main function which orchestrates the steps 
   by calling the respective functions'''
async def main():
   
   results = await getdata(url=URL_C, params=PARAMS_C)
   d = await populate_dict(results)
   df = pd.DataFrame.from_dict(d)
   df = df.assign(FLG=np.where(df.OUTCOME.isnull(), 1, 0))
   display_results(df)


if __name__ == "__main__":
    asyncio.run(main())
