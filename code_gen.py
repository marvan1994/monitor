import pandas as pd
from hashlib import sha256
import json


def actual_tutors():
    df = pd.read_csv('monitor_test.csv')

    tutors = df['email_tutor'].dropna().drop_duplicates()

    tutors_dict = {sha256((tutor+'09').encode('utf-8')).hexdigest()[0:10]:tutor for tutor in tutors}

    with open('tutors.json','w+') as f:
        json.dump(tutors_dict,f)
