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
    return pd.read_csv('monitor.csv',dtype={'student_id':'int', 'stud_vk':'str', 'stud_email':'str', 'stud_name':'str', 'paid_at':str, 'tariff':str,
       'product_title':'str', 'is_pack':'int', 'subject':'str', 'class_degree':'str', 'speaker':'str',
       'month_product':'str', 'avg_result':'float', 'last_hw_sending':'str', 'max_count_hw':'int',
       'count_done_hw':int, 'max_count_web':int, 'count_vieved_web':int, 'tutor_role':'str',
       'vk_tutor':str, 'email_tutor':'str', 'name_tutor':str, 'last_activity_tutor':str,
       'last_login_tutor':str, 'curator_name_in_vk':str}).set_index('student_id')

df = load_df()
main_cols = ['stud_vk', 'stud_name', 'stud_email', 'paid_at', 'tariff',
       'product_title', 'subject','class_degree', 'speaker',
       'month_product', 'avg_result', 'last_hw_sending', 'max_count_hw',
       'count_done_hw', 'max_count_web', 'count_vieved_web', 'tutor_role',
       'vk_tutor', 'email_tutor']

col_dict = {'stud_vk':'ВК', 'stud_email':'Почта', 'stud_name':'Имя', 'paid_at':'Дата оплаты', 'tariff':'Тариф',
       'product_title':'Продукт', 'subject':'Предмет','class_degree':'Класс', 'speaker':'Спикер',
       'month_product':'Месяц', 'avg_result':'Ср. результат ДЗ', 'last_hw_sending':'Время решения последнего ДЗ', 'max_count_hw':'Макс. кол-во ДЗ на данный момент',
       'count_done_hw':'Кол-во решённых ДЗ', 'max_count_web':'Макс. кол-во вебов на данный момент', 'count_vieved_web':'Кол-во просмотренных вебов',
        'tutor_role':'Роль наставника',
       'vk_tutor':'ВК наставника', 'email_tutor':'Почта на ставника'}


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

        curators = df['email_tutor'].unique().tolist()
        selected_cur = st.selectbox('Отфильтровать по кураторам', [''] + curators)

        if selected_cur !='':
            st.dataframe(df[main].query(f'email_tutor == "{selected_cur}"').rename(columns = col_dict))
        else:
            st.dataframe(df[main].rename(columns = col_dict))
    elif code in tut_d.keys():
        st.markdown(f'Приветик! Сегодня ты {fruits[rand.randint(0,len(fruits)-1)]}')
        dft = df.query(f'email_tutor == "{tut_d[code]}"')
        students = dft['stud_name'].unique().tolist()
        selected_stud = st.selectbox('Отфильтровать по ученикам', [''] + students)
        if selected_stud !='':
            st.dataframe(dft[main].query(f'stud_name == "{selected_stud}"').rename(columns = col_dict))
        else:
            st.dataframe(dft[main].rename(columns=col_dict))


    else:
        st.markdown('Куратора с таким кодом не найдено')