import streamlit as st
import pandas as pd
import json


code = st.text_input('Введите ваш код','').replace(' ','')

df = pd.read_csv('prodlenia_sept.csv')
df = df.set_index('student_id')

if len(code) != 0:
    with open('tutors.json') as f:
        tut_d = json.load(f)
    if code[0:8] in tut_d.keys():
        st.dataframe(df.query(f'vk_tutor == "{tut_d[code[0:8]]}"'))
    else:
        st.markdown('Куратора с таким кодом не найдено')