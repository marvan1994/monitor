import streamlit as st
import pandas as pd
import json
import random as rand

st.set_page_config(page_title='Умный монитор', page_icon = 'um.ico')
hide_st_style = """
            <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            header {visibility: hidden;}
            </style>
            """
st.markdown(hide_st_style, unsafe_allow_html=True)


st.markdown('### Вас приветствует Умный Монитор! ###')
code = st.text_input('Введите ваш код','').replace(' ','')

@st.cache
def load_df():
    return pd.read_csv('monitor.csv').set_index('student_id')

df = load_df()
main_cols = ['student_id', 'stud_name', 'stud_vk', 'stud_email', 'month_product',
       'pur_type', 'subject', 'class_degree', 'speaker', 'tariff', 'is_pack',
       'goal', 'max_count_hw', 'count_done_hw', 'avg_result', 'max_count_web',
       'count_viewed_web', 'count_viewed_web_online', 'avg_nps', 'curator_vk',
       'curator_fio', 'curator_last_login','curator_last_activity',
       'curator_email']

col_dict = {'stud_vk':'ВК', 'stud_email':'Почта', 'stud_name':'Имя', 'tariff':'Тариф',
       'product_title':'Продукт','pur_type':'Тип покупки', 'subject':'Предмет','class_degree':'Класс', 'speaker':'Спикер',
       'month_product':'Месяц', 'avg_result':'Ср. результат ДЗ', 'last_hw_sending':'Время решения последнего ДЗ', 'max_count_hw':'Макс. кол-во ДЗ на данный момент',
       'count_done_hw':'Кол-во решённых ДЗ', 'max_count_web':'Макс. кол-во вебов на данный момент', 'count_viewed_web':'Кол-во просмотренных вебов','count_viewed_web_online':'Кол-во просмотренных вебов онлайн',
        'avg_nps':'Средний балл за уроки','is_pack':'Это пакет?','goal':'Цель',
       'curator_vk':'ВК наставника','curator_email':'Почта наставника'}


with open('updated_time.txt') as f:
    update_date = f.readline()

with open('fruits.txt') as f:
    fruits = f.readlines()

with open('tutors.json') as f:
    tut_d = json.load(f)
    tam_d = {tut_d.get(key):key for key in tut_d.keys()}


if len(code) != 0:
    st.markdown(f'Обновление данных произошло в {update_date}')
    ismain = st.checkbox('Только основную информацию')
    if ismain:
        main = main_cols
    else:
        main = df.columns

    if code == 'umschool_top':

        st.markdown(f'Привет Юля! Сегодня ты {fruits[rand.randint(0,len(fruits))]}')

        with st.expander("Если хочешь узнать код сотрудника, то тебе сюда"):
            get_code = st.text_input('Введи почту').replace(' ','')
            if len(get_code)>1:
                st.markdown(f"*{tam_d.get(get_code,'Такого сотрудника не нашли')}*")

        curators = df['curator_email'].unique().tolist()
        selected_cur = st.selectbox('Отфильтровать по кураторам', [''] + curators)

        if selected_cur !='':
            st.dataframe(df[main].query(f'curator_email == "{selected_cur}"').rename(columns = col_dict))
        else:
            st.dataframe(df[main].rename(columns = col_dict))
    elif code in tut_d.keys():
        st.markdown(f'Приветик! Сегодня ты {fruits[rand.randint(0,len(fruits)-1)]}')
        dft = df.query(f'curator_email == "{tut_d[code]}"')
        students = dft['stud_name'].unique().tolist()
        selected_stud = st.selectbox('Отфильтровать по ученикам', [''] + students)
        if selected_stud !='':
            st.dataframe(dft[main].query(f'stud_name == "{selected_stud}"').rename(columns = col_dict))
        else:
            st.dataframe(dft[main].rename(columns=col_dict))


    else:
        st.markdown('Куратора с таким кодом не найдено')