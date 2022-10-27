import pandas as pd
import psycopg2 as pg
import os
import numpy as np
import requests
import datetime

path_for_save = './'

# 1)monitor

# автоматически определяет период выгрузки дз(месяц), ее нижний лимит
month_monitor_date_min_limit = datetime.datetime.now().replace(day=1).strftime("%Y-%m-%d")

# автоматически определяет период выгрузки дз(месяц), ее верхний лимит
month_monitor_date_max_limit = (datetime.datetime.now().replace(day=1) + datetime.timedelta(days=32)).replace(
    day=1).strftime("%Y-%m-%d")

# Ограничение для дз и вебов которые не открылись по настоящее время
date_limit = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

engine = pg.connect(user="dep_obr",
                    password="Ato9ahw8teit0ephiez9eekee3uup7",
                    host="rc1a-l3qgv4gfaeii464l.mdb.yandexcloud.net",
                    # host="rc1a-5t3x5xyo07mstgts.mdb.yandexcloud.net",
                    port="6432",
                    database="umdb")

query_monitor = f'''select 
hw_info.student_id,
 hw_info.stud_vk,
 hw_info.stud_email,
 hw_info.stud_fio as stud_name,
 hw_info.master_group_month_id,

  --hw_info.paid_at,

  hw_info.created_at as paid_at,
  hw_info.paid_at as real_paid_at,

  hw_info.product_title,
  --hw_info.course_type,

  hw_info.is_pack,
  hw_info.tariff,
  hw_info.subject,
  hw_info.class_degree,
  hw_info.speaker,
  hw_info.month_product,
  hw_info.avg_result, 
  hw_info.last_hw_sending,
  hw_info.count_base_hw as max_count_hw,
  hw_info.count_done_hw,
  web_info.count_base_web as max_count_web,
  web_info.count_vieved_web,
  curator_info_pro.curator_vk,
  curator_info_pro.curator_fio,
  curator_info_pro.email as curator_email,
  pm_info2.pm_vk,
  pm_info2.pm_fio,
  pm_info2.email as pm_email,
  curator_info_pro.curator_last_login,
  curator_info_pro.curator_last_activity,
  pm_info2.pm_last_login,
  pm_info2.pm_last_activity
from 
  (select  
  mg_pur.student_id ,
  TO_CHAR(mg_pur.paid_at, 'YYYY-MM-dd HH24:MI') as paid_at,
  TO_CHAR(mg_pur.created_at, 'YYYY-MM-dd HH24:MI') as created_at,

  stud_info.vk as stud_vk,
  stud_info.email as stud_email,
  concat(stud_info.first_name,' ',stud_info.last_name) as stud_fio,
  case 
  when (mg_pur.pack_template_id notnull)
  then 1 
  else 0
  end is_pack,
  mg_pur.master_group_month_id,
  mg_month.rate_plan_id,
  mg_mg.id as master_group_id,
  mg_mg.class_year_id,
  mg_mg.class_type_id,
  mg_mg.title as product_title,
  rateplan.title as tariff,
    ccc.title as subject,
    cct.title as class_degree,
    cu.last_name as speaker,
    TO_CHAR(mg_month.start_date, 'MM') as month_product,
    round(avg(hhs_status.number_of_points),2) as avg_result, 
    count(*) as count_base_hw,
    sum(case 
    when hhs_status.status isnull or hhs_status.status='UNDONE'
    then 0 
    else 1 end) as count_done_hw,
   TO_CHAR(max(hhs_status.sending_date), 'YYYY-MM-dd HH24:MI') as last_hw_sending
   --coursetype.title as course_type


    from mastergroup_mgpurchase as mg_pur
    join mastergroup_mgmonth as mg_month
    on mg_pur.master_group_month_id = mg_month.id
    inner join core_rateplan as rateplan
    on rateplan.id = mg_month.rate_plan_id
    join mastergroup_mastergroup as mg_mg
    on mg_month.master_group_id =mg_mg.id
    inner join core_user as cu on cu.id = mg_mg.user_id
    inner join core_coursetype as cct on cct.id = mg_mg.course_type_id
    inner join core_classtype as ccc on ccc.id = mg_mg.class_type_id
    inner join core_user as stud_info on stud_info.id = mg_pur.student_id
    --left join core_coursetype as coursetype
    --on mg_mg.course_type_id = coursetype.id 

    --join с поиском максимального кол-ва дз до определенной даты 

    inner join  (select les_mg_months.lesson_id,
       les_mg_months.mgmonth_id
       from lesson_lesson_mg_months 
       as les_mg_months
       join homework_homework as hw
       on hw.lesson_id = les_mg_months.lesson_id
       where hw.homework_level notnull
       and hw.available_from <= '{date_limit}') as les_mg_months_2
    on mg_pur.master_group_month_id = les_mg_months_2.mgmonth_id

    --join с поиском результатами за дз  

    left join (select 
    hhs.status,
    hhs.sending_date,
    hhs.student_id,
    hw.lesson_id,
    hhs.number_of_points   


    from homework_homeworksubmission as hhs
    join homework_homework as hw 
    on hw.id = hhs.homework_id
    join lesson_lesson as les
    on les.id = hw.lesson_id
    where hw.homework_level notnull
    and hw.available_from::date>= '{month_monitor_date_min_limit}' --'2022-10-01'
    and hw.available_from::date< '{month_monitor_date_max_limit}' --'2022-11-01'
    and les.type='MG') as hhs_status 
  on hhs_status.student_id = mg_pur.student_id
  and hhs_status.lesson_id = les_mg_months_2.lesson_id


     where mg_pur.type != 'STAFF' 
    and mg_month.start_date >= '{month_monitor_date_min_limit}' --'2022-10-01' 
    and mg_month.start_date < '{month_monitor_date_max_limit}' --'2022-11-01' 
    and mg_month.end_date - mg_month.start_date >1 
    and mg_pur.is_valid='true' 
    and mg_pur.type != 'FREE'
    and mg_mg.type = 'MG'
    and stud_info.is_staff = false

   group by mg_pur.student_id,mg_pur.master_group_month_id,
    mg_mg.title,rateplan.title,ccc.title,cct.title,
    cu.last_name,mg_month.start_date,mg_mg.id,mg_month.rate_plan_id,
    mg_pur.pack_template_id,stud_info.vk,stud_info.email
    ,mg_pur.created_at,stud_fio,paid_at) as hw_info

 -- join для определения кураторов (про)

left join (select 
  mg_group_par.user_id as student_id,
  mg_group_par.group_id,
  mg_group.title,
  mg_group.master_group_id,
  mg_group.rate_plan_id,
  mg_group.curator_vk,
  concat(curator_info.first_name,' ',curator_info.last_name) as curator_fio,
  TO_CHAR(curator_info.last_login, 'YYYY-MM-dd HH24:MI') as curator_last_login,
  TO_CHAR(curator_info.last_activity, 'YYYY-MM-dd HH24:MI') as curator_last_activity,
  curator_info.email

  from mastergroup_mggroupparticipation as mg_group_par
  join mastergroup_mggroup as mg_group
  on mg_group.id = mg_group_par.group_id
  left join (SELECT
		  * 
		FROM (
		  SELECT
			--ROW_NUMBER() OVER (PARTITION BY vk ORDER BY date_joined desc) AS r,
            ROW_NUMBER() OVER (PARTITION BY vk ORDER BY last_activity desc) AS r,
			t.*
		  FROM
			core_user t
		where role = 'CURATOR_ROLE') x
		WHERE x.r <= 1) as curator_info
    on curator_info.vk=mg_group.curator_vk 
    where mg_group_par.is_active = true
  and mg_group.master_group_id notnull ) as curator_info_pro
on curator_info_pro.master_group_id=hw_info.master_group_id
and curator_info_pro.student_id = hw_info.student_id
and curator_info_pro.rate_plan_id = hw_info.rate_plan_id

-- join для определения ПМ (стадндарт)

left join (select 
  mg_group_par.user_id as student_id,
  mg_group_par.group_id,
  mg_group.title,
  --mg_group.master_group_id,
  mg_group.group_manager_id,
  mg_group.rate_plan_id,
  --rateplan.class_year_id,
  rateplan.title as rate_title,   
  --com_class_types.classtype_id,
  concat(pm_info.first_name,' ',pm_info.last_name) as pm_fio,
  pm_info.vk as pm_vk,
  --pm_info.last_login as pm_last_login,
  --pm_info.last_activity as pm_last_activity,
  mg_group.community_id,
  TO_CHAR(pm_info.last_login, 'YYYY-MM-dd HH24:MI') as pm_last_login,
  TO_CHAR(pm_info.last_activity, 'YYYY-MM-dd HH24:MI') as pm_last_activity,
  pm_info.email

  from mastergroup_mggroupparticipation as mg_group_par
  join mastergroup_mggroup as mg_group
  on mg_group.id = mg_group_par.group_id
  join core_rateplan as rateplan
  on rateplan.id = mg_group.rate_plan_id
  --join core_community_class_types as com_class_types
  --on com_class_types.community_id=mg_group.community_id
  join core_user as pm_info
  on pm_info.id = mg_group.group_manager_id
  where mg_group_par.is_active = true 
  and master_group_id isnull) as pm_info2
  on pm_info2.student_id =hw_info.student_id
  and pm_info2.rate_title = hw_info.tariff

  --and pm_info.rate_plan_id = hw_info.rate_plan_id

  --and pm_info.class_year_id =hw_info.class_year_id

  --and pm_info.classtype_id = hw_info.class_type_id

left join (select  mg_pur.student_id ,
mg_pur.master_group_month_id as month_id,
mg_mg.title as product_title,
rateplan.title as tariff,
  ccc.title as subject,
  cct.title as class_degree,
  cu.last_name as speaker,
  TO_CHAR(mg_month.start_date, 'MM') as month_product,
  mg_month.lessons_amount as lessons_amount,
  count(*) as count_base_web,
    sum(case 
    when les_sub.got_exp isnull or les_sub.got_exp=false
    then 0 
    else 1 end) as count_vieved_web



  from mastergroup_mgpurchase as mg_pur
  join mastergroup_mgmonth as mg_month
  on mg_pur.master_group_month_id = mg_month.id
  inner join core_rateplan as rateplan
  on rateplan.id = mg_month.rate_plan_id
  join mastergroup_mastergroup as mg_mg
  on mg_month.master_group_id =mg_mg.id
  inner join core_user as cu on cu.id = mg_mg.user_id
  inner join core_coursetype as cct on cct.id = mg_mg.course_type_id
  inner join core_classtype as ccc on ccc.id = mg_mg.class_type_id
  inner join  (select les_mg_months.lesson_id,
         les_mg_months.mgmonth_id
         from lesson_lesson_mg_months 
         as les_mg_months
			join lesson_lesson as les
			on les.id = les_mg_months.lesson_id

         where les.webinar_level notnull
		 and les.without_video='false'
		 and les.start_datetime<= '{date_limit}') as les_mg_months_2
  on mg_pur.master_group_month_id = les_mg_months_2.mgmonth_id
  left join lesson_lessonsubmission as les_sub
  on mg_pur.student_id = les_sub.student_id 
  and les_mg_months_2.lesson_id = les_sub.lesson_id




  where mg_pur.type != 'STAFF' 
  and mg_month.start_date >= '{month_monitor_date_min_limit}' --'2022-10-01' 
  and mg_month.start_date < '{month_monitor_date_max_limit}' --'2022-11-01' 
  and mg_month.end_date - mg_month.start_date >1 
  and mg_pur.is_valid='true' 
  and mg_pur.type != 'FREE'
  and mg_mg.type = 'MG'
  group by mg_pur.student_id, product_title,subject,
  class_degree,speaker,month_product,cu.id, 
  mg_pur.master_group_month_id,rateplan.title,lessons_amount)
  as web_info 
  on web_info.student_id = hw_info.student_id
  and web_info.month_id = hw_info.master_group_month_id'''

# формирование df из запроса с ограничением на нынешнее время

# query_monitor = query_monitor.format(date,date)
df_data_oct = pd.read_sql(query_monitor, con=engine)

# заполнение пустых значений PM из заполненных (для уникального ученика)
df_data_oct_merge = df_data_oct.query('tariff=="Стандарт" and pm_vk.notnull() and  pm_fio.notnull()', engine='python') \
    [['student_id', 'tariff', 'pm_vk', 'pm_fio', 'pm_email', 'pm_last_login', 'pm_last_activity']] \
    .drop_duplicates(
    subset=['student_id', 'tariff', 'pm_vk', 'pm_fio', 'pm_email', 'pm_last_login', 'pm_last_activity'], keep='first') \
    .merge(df_data_oct, on=['student_id', 'tariff'], how='right')

# заполнение роли тьютора
df_data_oct_merge.loc[(df_data_oct_merge['curator_vk'].notnull()), 'tutor_role'] = 'curator'
df_data_oct_merge.loc[(df_data_oct_merge['pm_vk_x'].notnull()), 'tutor_role'] = 'PM'

# объединение кураторов и pm в один атрибут "тьютора"
df_data_oct_merge['vk_tutor'] = df_data_oct_merge.curator_vk.combine_first(df_data_oct_merge.pm_vk_x)
df_data_oct_merge['email_tutor'] = df_data_oct_merge.curator_email.combine_first(df_data_oct_merge.pm_email_x)
df_data_oct_merge['name_tutor'] = df_data_oct_merge.curator_fio.combine_first(df_data_oct_merge.pm_fio_x)
df_data_oct_merge['last_login_tutor'] = df_data_oct_merge.curator_last_login.combine_first(
    df_data_oct_merge.pm_last_login_x)
df_data_oct_merge['last_activity_tutor'] = df_data_oct_merge.curator_last_activity.combine_first(
    df_data_oct_merge.pm_last_activity_x)

# Выносим нужные атрибуты
df_data_oct_merge = df_data_oct_merge[['student_id', 'stud_vk', 'stud_email', 'stud_name', 'paid_at',
                                       'tariff', 'product_title', 'is_pack', 'subject', 'class_degree', 'speaker',
                                       'month_product', 'avg_result', 'last_hw_sending', 'max_count_hw',
                                       'count_done_hw', 'max_count_web', 'count_vieved_web', 'tutor_role', 'vk_tutor',
                                       'email_tutor',
                                       'name_tutor', 'last_activity_tutor', 'last_login_tutor']]

# отбрасываем пользователей без тьюторов (недавно купившие или недавно сформированные мг группы еще без тьютора)
df_data_oct_merge_drop = df_data_oct_merge.query('tutor_role.notnull()', engine='python')

# Удаляем полные дубликаты
df_data_oct_merge_drop = df_data_oct_merge_drop.drop_duplicates(
    subset=['student_id', 'stud_vk', 'stud_email', 'paid_at',
            'tariff', 'product_title', 'is_pack', 'subject', 'class_degree', 'speaker',
            'month_product', 'avg_result', 'last_hw_sending', 'max_count_hw',
            'count_done_hw', 'max_count_web', 'count_vieved_web', 'tutor_role', 'email_tutor',
            'vk_tutor', 'name_tutor'], keep='first')

# Оставляем последнюю покупку из дублированных но разным по тарифу или преподавателю,
# случаеться когда выбрал не того спикера или поменяли тариф
df_data_oct_merge_drop.sort_values(by=['paid_at'], ascending=False) \
    .drop_duplicates(subset=['student_id', 'subject'], keep='first')

# Сортируем для вывода в дф
df_data_oct_merge_drop = df_data_oct_merge_drop.sort_values(by=['student_id', 'paid_at'])

df_data_oct_drop_merge_last_act_cur = df_data_oct_merge_drop
# отбрасываем пользователей без почты
# df_data_oct_drop_merge_last_act_cur=df_data_oct_merge_drop.query('email_tutor.notnull()',engine='python')


# 1.2)join vk name

# из-за сложности определения куратора по vk (несколько старых кураторов может иметь такую же ссылку вк),
# проверяем имя куратора из БД с именем из вк, если они чем-то(не всегда значительным) отличаются, то имя такого куратора выводиться

# выводим vk уникальных кураторов и формируем список их vk id
list_vk_link = df_data_oct_drop_merge_last_act_cur.query('tutor_role != "PM"').vk_tutor.unique().tolist()
dict_vk_link_id = {}
for i in list_vk_link:
    dict_vk_link_id[i] = int(i.split('id')[1])

df_vk_id = pd.DataFrame(dict_vk_link_id.items(), columns=['link_vk', 'id_vk'])

list_vk_id = df_vk_id.id_vk.to_list()
str_list_vk_id = f"{','.join(str(xs) for xs in list_vk_id)}"

# запрос к api vk на фио
resp = requests.get(f'https://api.vk.com/method/users.get?\
&access_token=876f6247876f6247876f62476d847fa8558876f876f6247e444546f819cc09b88d90c45\
&user_ids={str_list_vk_id}\
&fields=first_name,last_name\
&lang=0\
&v=5.131')
json_response = resp.json()

merge_df = pd.json_normalize(json_response['response'])[['id', 'first_name', 'last_name']].rename(
    columns={'id': 'id_vk'}).merge(df_vk_id, how='right').rename(columns={'link_vk': 'vk_tutor'})
merge_df['real_name_tutor'] = merge_df[['first_name', 'last_name']].astype({'first_name': str, 'last_name': str}).agg(
    ' '.join, axis=1)
merge_df = merge_df[['vk_tutor', 'real_name_tutor']]

# соединяем с основной табл
df_data_oct_merge_name_cur_dop = df_data_oct_drop_merge_last_act_cur.merge( \
    df_data_oct_drop_merge_last_act_cur.merge(merge_df, how='left') \
        .query('real_name_tutor.notnull() and name_tutor.notnull() and name_tutor!=real_name_tutor', engine='python')[
        ['vk_tutor', 'real_name_tutor']].
        drop_duplicates(subset=['vk_tutor', 'real_name_tutor'], keep='first'), how='left')
df_data_oct_merge_name_cur_dop_ren = df_data_oct_merge_name_cur_dop.rename(
    columns={'real_name_tutor': 'curator_name_in_vk'})

# Сохраняем monitor
df_data_oct_merge_name_cur_dop_ren.to_csv(f'{path_for_save}monitor.csv', index=False)

# 2) goals

# Создаем список учеников которые нам нужны
list_group = df_data_oct_merge_name_cur_dop_ren.student_id.unique().tolist()
str_list_group = f"({','.join(str(xs) for xs in list_group)})"

query_goals = '''
select  
--student_goal.id,
student_goal.user_id as student_id,
--student_goal.is_active as student_goal_is_active,
umit_sct.is_active as student_goal_is_active, --umit_sct_is_active,
TO_CHAR(student_goal.created_at, 'YY-MM-dd HH24:MI') as created_at,
TO_CHAR(student_goal.updated_at, 'YY-MM-dd HH24:MI') as updated_at,
umit_sct.number_of_points,
ccy.title as class_degree,
cct.title as subject

from umit_studentgoal as student_goal
join umit_studentclasstypegoal as umit_sct
on umit_sct.goal_id = student_goal.id
join core_classyear as ccy
on ccy.id = student_goal.class_year_id
join core_classtype  as cct
on cct.id =  umit_sct.class_type_id
where student_goal.user_id in {0}
'''

query_goals = query_goals.format(str_list_group)

# Формируем дф и сохраняем
df_data_goal = pd.read_sql(query_goals, con=engine)
df_data_goal.to_csv(f'{path_for_save}goals.csv', index=False)

# 3) homework
query_hw = '''select   
hhs.student_id,
student_users.vk as stud_vk,
student_users.email as stud_email,
cct.title as subject,
ccy.title as class_degree,
users.last_name as teacher,
hhs.status,
hhs.homework_id as hw_id,
hw.homework_level,
--hhs.assigner_id,
DATE_TRUNC('second', age(hhs.sending_date , hhs.start_datetime))::text as hw_time, -- Длительность выполнения ДЗ
TO_CHAR(hhs.sending_date, 'YY-MM-dd HH24:MI') as hw_end_date, -- Дата когда он закончил
hhs.number_of_points,
exp_hhs.sum_exp as exp

from homework_homeworksubmission as hhs
join homework_homework as hw on hhs.homework_id = hw.id 
join core_user student_users on hhs.student_id = student_users.id 
join lesson_lesson as les on hw.lesson_id = les.id 
join core_classtype cct on les.class_type_id = cct.id 
join core_classyear ccy on les.class_year_id = ccy.id
join core_user users on les.teacher_id = users.id 
left join (select  object_id as hhs_id,student_id, sum(exp) as sum_exp
		from student_studentexpaudit as stud_exp
		where type='HW'
		group by object_id,student_id) as exp_hhs 
		on exp_hhs.hhs_id = hhs.id 
		and exp_hhs.student_id = hhs.student_id

where hhs.status != 'UNDONE'
and hw.available_from::date>='2022-10-01'
and hw.available_from::date<'2022-11-01'
and les.type = 'MG'
'''

# формируем и сохраняем df
df_data_hw_info = pd.read_sql(query_hw, con=engine)
df_data_hw_info.to_csv(f'{path_for_save}homework.csv', index=False)

# 4) webinar
query_web = '''select  mg_pur.student_id ,
mg_mg.title as product_title,
--TO_CHAR(mg_month.start_date, 'MM') as month_product,
-- case 
--   when mg_mg.type = 'MG' 
--   then 1
--     else 0
--   end as is_mg,
  ccc.title as subject,
  cct.title as class_degree,
  cu.last_name as speaker,
  les.id as les_id,
  les.video_length as base_video_length,
  --les.title les_title,
  --hw.title hw_title,
  les_sub.viewed_online+les_sub.viewed_offline as sum_viewed,
  les_sub.viewed_online,
  les_sub.viewed_offline,
  TO_CHAR(les_sub.last_update, 'YYYY-MM-dd HH24:MI') as viewed_last_update,
  --les_sub.got_exp, -- если нужны только просмотренные,
  les_exp_audit.les_exp
  --то в where les_sub.got_exp = true



  from mastergroup_mgpurchase as mg_pur
  join mastergroup_mgmonth as mg_month
  on mg_pur.master_group_month_id = mg_month.id
  join mastergroup_mastergroup as mg_mg
  on mg_month.master_group_id =mg_mg.id
  inner join core_user as cu on cu.id = mg_mg.user_id
  inner join core_coursetype as cct on cct.id = mg_mg.course_type_id
  inner join core_classtype as ccc on ccc.id = mg_mg.class_type_id
  inner join  lesson_lesson_mg_months as les_mg_months
  on mg_pur.master_group_month_id = les_mg_months.mgmonth_id
  join lesson_lesson as les
  on les.id =  les_mg_months.lesson_id
  join homework_homework as hw 
  on hw.lesson_id = les_mg_months.lesson_id
  left join lesson_lessonsubmission as les_sub
		on les_sub.student_id= mg_pur.student_id
		and les_sub.lesson_id = les_mg_months.lesson_id 

  --left join(select exp,object_id,student_id from (select 
		--ROW_NUMBER() OVER (PARTITION BY exp ORDER BY created_at desc) as r,
			--t.*
		 --FROM student_studentexpaudit t
		 --where t.type='LSN') as x
		--where x.r <= 1) as audit_exp
   --on audit_exp.object_id = les.id
   --and audit_exp.student_id = mg_pur.student_id

   left join (select max(stud_exp_audit.exp) as les_exp,
			  stud_exp_audit.object_id,
			  stud_exp_audit.student_id
			 from student_studentexpaudit as stud_exp_audit
			 where stud_exp_audit.type='LSN'
			 group by stud_exp_audit.object_id,
			  stud_exp_audit.student_id) as les_exp_audit
   on les_exp_audit.object_id = les.id
   and les_exp_audit.student_id = mg_pur.student_id

  where mg_pur.type != 'STAFF' -- не стафф покупка 
  and mg_month.start_date >= '2022-10-01' --ограничение по месяцу мг или курса 
  and mg_month.start_date < '2022-11-01' --ограничение по месяцу мг или курса
  and mg_month.end_date - mg_month.start_date >1 -- не 0 дневной курс 
  and mg_pur.is_valid='true' --  курс по валидным покупкам 
  and mg_pur.type != 'FREE'-- по бесплатным покупкам
  and les_sub.got_exp = true 
  and mg_mg.type = 'MG'
  '''

# формируем df и переводим секунды в минуты
df_data_web_info = pd.read_sql(query_web, con=engine)
df_data_web_info = df_data_web_info.sort_values(by=['student_id', 'subject', 'viewed_last_update'])
df_data_web_info['base_video_length'] = df_data_web_info.base_video_length.apply(lambda x: x // 60)
df_data_web_info['sum_viewed'] = df_data_web_info.sum_viewed.apply(lambda x: x // 60)
df_data_web_info['viewed_online'] = df_data_web_info.viewed_online.apply(lambda x: x // 60)
df_data_web_info['viewed_offline'] = df_data_web_info.viewed_offline.apply(lambda x: x // 60)

# Сохранчем результат
df_data_web_info.to_csv(f'{path_for_save}webinar.csv', index=False)