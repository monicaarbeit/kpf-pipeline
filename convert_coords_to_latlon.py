import numpy as np
import pandas as pd
import json
from pyathena import connect

from osgeo import osr
from pyproj import Proj, transform

inProj  = Proj("+init=EPSG:2263",preserve_units=True)
outProj = Proj("+init=ESRI:102718")#Proj("+init=EPSG:4326") # WGS84 in degrees and not EPSG:3857 in meters)
projstr = '+proj=lcc +lat_1=40.66666666666666 +lat_2=41.03333333333333 +lat_0=40.16666666666666 +lon_0=-74 +x_0=300000 +y_0=0 +ellps=GRS80 +datum=NAD83 +to_meter=0.3048006096012192 +no_defs'

df = pd.read_csv("data/pluto_18v2_FIXED.csv")
print(df.head())

lat = []
lon = []


# null_values = ["nan", float('inf')]

count = 0
for _, row in df.iterrows():

        if count % 50000 == 0:
            print("count: ", count)
        count += 1

        # if row['xcoord'] not in null_values or row['ycoord'] not in null_values:

        inp= osr.SpatialReference()
        inp.ImportFromEPSG(3628)
        out= osr.SpatialReference()
        out.ImportFromEPSG(4326)
        transformation = osr.CoordinateTransformation(inp, out)
        point = transformation.TransformPoint(row['xcoord'], row['ycoord'])

        if point[0] <= 360 and point[0] >= -360 and point[1] <= 360 and point[1] >= -360:
            lon.append(point[0])
            lat.append(point[1])
        else:
            lon.append(0)
            lat.append(0)


# new_df = pd.DataFrame(data={'lat': lat, 'lon': lon}, columns=['lat', 'lon'])
# pd.concat([df, new_df], axis=1)

df['lat'] = lat
df['lon'] = lon

print("converting to dataframe to csv")
df.to_csv("./data/converted_data.csv")
