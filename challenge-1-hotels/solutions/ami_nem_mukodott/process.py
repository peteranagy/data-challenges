import pandas as pd
import numpy as np
import json

input_json = json.load(open('input.json','r'))
data_args = json.load(open('data.json','r'))

df = pd.read_csv(data_args['staging_folder'] + '/filtered.csv')
df = df.drop_duplicates(subset=['name'], keep='first')

out = []

for place in input_json:
    
    df['distance'] = ((place['lon']-df['lon']) ** 2 + (place['lat']-df['lat']) ** 2) ** 0.5
    
    out.append(df.ix[df['distance'].idxmin(),["lon","lat","name"]].to_dict().copy())

json.dump(out,open('output.json','w'))