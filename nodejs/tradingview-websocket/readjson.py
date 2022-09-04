import json 
import os 
import pandas as pd 

print(os.getcwd()); 

with open('config.json', 'rb') as f:
    file = f.read()
    data = json.loads(file)

df = pd.DataFrame(data)
# print(df)
print(data)


