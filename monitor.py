import streamlit as st
import pandas as pd
import json
from code_gen import actual_tutors
import random as rand
actual_tutors()

code = st.text_input('Введите ваш код','').replace(' ','')

df = pd.read_csv('monitor_test.csv')
df = df.set_index('student_id')

with open('fruits.txt') as f:
    fruits = f.readlines()

if len(code) != 0:
    with open('tutors.json') as f:
        tut_d = json.load(f)
    if code == 'bakuma_top':
        st.markdown(f'Привет Юля! Сегодня ты {fruits[rand.randint(0,len(fruits))]}')
        st.dataframe(df)
    elif code in tut_d.keys():
        st.markdown(f'Приветик! Сегодня ты {fruits[rand.randint(0,len(fruits))]}')
        dft = df.query(f'email_tutor == "{tut_d[code]}"')
        st.dataframe(dft)
    else:
        st.markdown('Куратора с таким кодом не найдено')