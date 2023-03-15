import pandas as pd
from hashlib import sha256
import json
from datetime import datetime as date


df = pd.read_csv('monitor.csv')

tutors = df['email_tutor'].dropna().drop_duplicates()

tutors_dict = {sha256((tutor+'10').encode('utf-8')).hexdigest()[0:10]:tutor for tutor in tutors}

with open('tutors.json','w+') as f:
    json.dump(tutors_dict,f)


with open('updated_time.txt','w+') as f:
    f.write(date.now().strftime('%Y-%m-%d %H:%M'))