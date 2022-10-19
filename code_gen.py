import pandas as pd
from hashlib import sha256
import json

df = pd.read_csv('prodlenia_sept.csv')

tutors = df['vk_tutor'].dropna().drop_duplicates()

tutors_dict = {sha256(tutor.encode('utf-8')).hexdigest()[0:8]:tutor for tutor in tutors}

with open('tutors.json','w+') as f:
    json.dump(tutors_dict,f)

with open('tutors.json') as f:
    tut_d = json.load(f)
