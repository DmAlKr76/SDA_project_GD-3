#Импортируем библиотеку и данные
import pandas as pd

events_df = pd.read_csv('7_4_Events.csv')
purchase_df = pd.read_csv('7_4_Purchase.csv')

#Отфильтруем полученнные данные в соответствии с заданием и произведем преобразование типа и даты 
cond = (events_df.start_time>='2018-01-01') & (events_df.start_time<'2019-01-01') & (events_df.event_type=='registration')
registered = events_df[cond]['user_id'].to_list()
events_df = events_df[events_df.user_id.isin(registered)]
events_df.start_time = pd.to_datetime(events_df.start_time, format='%Y-%m-%dT%H:%M:%S')

purchase_df['event_type'] = 'purchase'
purchase_df = purchase_df[purchase_df.user_id.isin(registered)]
purchase_df.event_datetime = pd.to_datetime(purchase_df.event_datetime, format='%Y-%m-%dT%H:%M:%S')

#Переименуем колонки и произведем объединение
events_df = events_df.rename(columns={'id':'event_id'})
purchase_df = purchase_df.rename(columns={'id':'purchase_id'})

total_events_df = pd.concat([events_df,purchase_df],sort=False)
total_events_df = total_events_df.reset_index(drop=True).sort_values('start_time')

#Распределим пользователей по группам, в зависимости от выбора уровня сложности бесплатных тренировок. 
#Сформируем датафреймы по группам пользователей с датой регистрации и с выбором уровня сложности, 
#Произведем замену названия колонок даты и времени события.
#Произведем проверки единичности событий.
users_with_easy_level = total_events_df[total_events_df['selected_level'] == 'easy']['user_id'].unique()
users_with_easy_level_df_registration = total_events_df[(total_events_df['event_type'] == 'registration') & (total_events_df['user_id'].isin(users_with_easy_level))]
users_with_easy_level_df_registration = users_with_easy_level_df_registration[['user_id','start_time']].rename(columns={'start_time':'registration_time'})
print('Проверка единичности события регистрации "easy":', (users_with_easy_level_df_registration['user_id'].value_counts().mean()) == 1)
users_with_easy_level_df_level_choice = total_events_df[(total_events_df['event_type'] == 'level_choice') & (total_events_df['user_id'].isin(users_with_easy_level))]
users_with_easy_level_df_level_choice = users_with_easy_level_df_level_choice[['user_id','start_time']].rename(columns={'start_time':'level_choice_time'})
print('Проверка единичности события выбора уровня сложности "easy":', (users_with_easy_level_df_level_choice['user_id'].value_counts().mean()) == 1)

users_with_medium_level = total_events_df[total_events_df['selected_level'] == 'medium']['user_id'].unique()
users_with_medium_level_df_registration = total_events_df[(total_events_df['event_type'] == 'registration') & (total_events_df['user_id'].isin(users_with_medium_level))]
users_with_medium_level_df_registration = users_with_medium_level_df_registration[['user_id','start_time']].rename(columns={'start_time':'registration_time'})
print('Проверка единичности события регистрации "medium":', (users_with_medium_level_df_registration['user_id'].value_counts().mean()) == 1)
users_with_medium_level_df_level_choice = total_events_df[(total_events_df['event_type'] == 'level_choice') & (total_events_df['user_id'].isin(users_with_medium_level))]
users_with_medium_level_df_level_choice = users_with_medium_level_df_level_choice[['user_id','start_time']].rename(columns={'start_time':'level_choice_time'})
print('Проверка единичности события выбора уровня сложности "medium":', (users_with_medium_level_df_level_choice['user_id'].value_counts().mean()) == 1)

users_with_hard_level = total_events_df[total_events_df['selected_level'] == 'hard']['user_id'].unique()
users_with_hard_level_df_registration = total_events_df[(total_events_df['event_type'] == 'registration') & (total_events_df['user_id'].isin(users_with_hard_level))]
users_with_hard_level_df_registration = users_with_hard_level_df_registration[['user_id','start_time']].rename(columns={'start_time':'registration_time'})
print('Проверка единичности события регистрации "hard":', (users_with_hard_level_df_registration['user_id'].value_counts().mean()) == 1)
users_with_hard_level_df_level_choice = total_events_df[(total_events_df['event_type'] == 'level_choice') & (total_events_df['user_id'].isin(users_with_hard_level))]
users_with_hard_level_df_level_choice = users_with_hard_level_df_level_choice[['user_id','start_time']].rename(columns={'start_time':'level_choice_time'})
print('Проверка единичности события выбора уровня сложности "hard":', (users_with_hard_level_df_level_choice['user_id'].value_counts().mean()) == 1)


#Посчитаем для каждого уровня сложности бесплатных тренировок количество и процент оплативших пользователей, 
# а также временной промежуток между регистрацией и оплатой, выбором уровня сложности и оплатой.

#Уровень сложности 'easy':
count_of_users_with_easy_level = len(users_with_easy_level)
purchase_df_of_users_with_easy_level = purchase_df[purchase_df['user_id'].isin(users_with_easy_level)]
percent_of_users_with_easy_level = purchase_df_of_users_with_easy_level['user_id'].nunique()/count_of_users_with_easy_level
print ('Процент оплативших пользователей, выбравших уровень сложности "easy": {:.2%}'.format(percent_of_users_with_easy_level))
print ()

purchase_df_of_users_with_easy_level_2 = purchase_df_of_users_with_easy_level[['user_id','event_datetime']].rename(columns={'event_datetime':'purchase_time'})

merged_df_of_users_with_easy_level_registration = purchase_df_of_users_with_easy_level_2.merge(users_with_easy_level_df_registration,on='user_id',how='inner')
merged_df_of_users_with_easy_level_registration['timedelta'] = merged_df_of_users_with_easy_level_registration['purchase_time'] - merged_df_of_users_with_easy_level_registration['registration_time']
mean_time_of_users_with_easy_level_registration = merged_df_of_users_with_easy_level_registration['timedelta'].mean()
print ('Среднее время между регистрацией и оплатой для пользователей, выбравших уровень сложности "easy": {}'.format(mean_time_of_users_with_easy_level_registration))
print ('Характеристики времени:')
print (merged_df_of_users_with_easy_level_registration['timedelta'].describe())
print ()
merged_df_of_users_with_easy_level_level_choice = purchase_df_of_users_with_easy_level_2.merge(users_with_easy_level_df_level_choice,on='user_id',how='inner')
merged_df_of_users_with_easy_level_level_choice['timedelta'] = merged_df_of_users_with_easy_level_level_choice['purchase_time'] - merged_df_of_users_with_easy_level_level_choice['level_choice_time']
mean_time_of_users_with_easy_level_level_choice = merged_df_of_users_with_easy_level_level_choice['timedelta'].mean()
print ('Среднее время между выбором уровня сложности и оплатой для пользователей, выбравших уровень сложности "easy": {}'.format(mean_time_of_users_with_easy_level_level_choice))
print ('Характеристики времени:')
print (merged_df_of_users_with_easy_level_level_choice['timedelta'].describe())
print ()


#Уровень сложности 'medium':
count_of_users_with_medium_level = len(users_with_medium_level)
purchase_df_of_users_with_medium_level = purchase_df[purchase_df['user_id'].isin(users_with_medium_level)]
percent_of_users_with_medium_level = purchase_df_of_users_with_medium_level['user_id'].nunique()/count_of_users_with_medium_level
print ('Процент оплативших пользователей, выбравших уровень сложности "medium": {:.2%}'.format(percent_of_users_with_medium_level))
print ()

purchase_df_of_users_with_medium_level_2 = purchase_df_of_users_with_medium_level[['user_id','event_datetime']].rename(columns={'event_datetime':'purchase_time'})

merged_df_of_users_with_medium_level_registration = purchase_df_of_users_with_medium_level_2.merge(users_with_medium_level_df_registration,on='user_id',how='inner')
merged_df_of_users_with_medium_level_registration['timedelta'] = merged_df_of_users_with_medium_level_registration['purchase_time'] - merged_df_of_users_with_medium_level_registration['registration_time']
mean_time_of_users_with_medium_level_registration = merged_df_of_users_with_medium_level_registration['timedelta'].mean()
print ('Среднее время между регистрацией и оплатой для пользователей, выбравших уровень сложности "medium": {}'.format(mean_time_of_users_with_medium_level_registration))
print ('Характеристики времени:')
print (merged_df_of_users_with_medium_level_registration['timedelta'].describe())
print ()
merged_df_of_users_with_medium_level_level_choice = purchase_df_of_users_with_medium_level_2.merge(users_with_medium_level_df_level_choice,on='user_id',how='inner')
merged_df_of_users_with_medium_level_level_choice['timedelta'] = merged_df_of_users_with_medium_level_level_choice['purchase_time'] - merged_df_of_users_with_medium_level_level_choice['level_choice_time']
mean_time_of_users_with_medium_level_level_choice = merged_df_of_users_with_medium_level_level_choice['timedelta'].mean()
print ('Среднее время между выбором уровня сложности и оплатой для пользователей, выбравших уровень сложности "medium": {}'.format(mean_time_of_users_with_medium_level_level_choice))
print ('Характеристики времени:')
print (merged_df_of_users_with_medium_level_level_choice['timedelta'].describe())
print () 


#Уровень сложности 'hard':
count_of_users_with_hard_level = len(users_with_hard_level)
purchase_df_of_users_with_hard_level = purchase_df[purchase_df['user_id'].isin(users_with_hard_level)]
percent_of_users_with_hard_level = purchase_df_of_users_with_hard_level['user_id'].nunique()/count_of_users_with_hard_level
print ('Процент оплативших пользователей, выбравших уровень сложности "hard": {:.2%}'.format(percent_of_users_with_hard_level))
print ()

purchase_df_of_users_with_hard_level_2 = purchase_df_of_users_with_hard_level[['user_id','event_datetime']].rename(columns={'event_datetime':'purchase_time'})

merged_df_of_users_with_hard_level_registration = purchase_df_of_users_with_hard_level_2.merge(users_with_hard_level_df_registration,on='user_id',how='inner')
merged_df_of_users_with_hard_level_registration['timedelta'] = merged_df_of_users_with_hard_level_registration['purchase_time'] - merged_df_of_users_with_hard_level_registration['registration_time']
mean_time_of_users_with_hard_level_registration = merged_df_of_users_with_hard_level_registration['timedelta'].mean()
print ('Среднее время между регистрацией и оплатой для пользователей, выбравших уровень сложности "hard": {}'.format(mean_time_of_users_with_hard_level_registration))
print ('Характеристики времени:')
print (merged_df_of_users_with_hard_level_registration['timedelta'].describe())
print ()
merged_df_of_users_with_hard_level_level_choice = purchase_df_of_users_with_hard_level_2.merge(users_with_hard_level_df_level_choice,on='user_id',how='inner')
merged_df_of_users_with_hard_level_level_choice['timedelta'] = merged_df_of_users_with_hard_level_level_choice['purchase_time'] - merged_df_of_users_with_hard_level_level_choice['level_choice_time']
mean_time_of_users_with_hard_level_level_choice = merged_df_of_users_with_hard_level_level_choice['timedelta'].mean()
print ('Среднее время между выбором уровня сложности и оплатой для пользователей, выбравших уровень сложности "hard": {}'.format(mean_time_of_users_with_hard_level_level_choice))
print ('Характеристики времени:')
print (merged_df_of_users_with_hard_level_level_choice['timedelta'].describe())
print ()

### АНАЛИЗ ПОЛУЧЕННЫХ ДАННЫХ И ВЫВОДЫ:

# Процент оплативших пользователей, выбравших уровень сложности easy: 7.72%
# Процент оплативших пользователей, выбравших уровень сложности medium: 20.86%
# Процент оплативших пользователей, выбравших уровень сложности hard: 35.39%
# Следовательно, с возрастанием уровня сложности тренировки возрастает процент оплативших пользователей.

# Среднее время между регистрацией и оплатой для пользователей, выбравших уровень сложности "easy": 3 days 22:10:23.
# Среднее время между регистрацией и оплатой для пользователей, выбравших уровень сложности "medium": 4 days 06:12:06.
# Среднее время между регистрацией и оплатой для пользователей, выбравших уровень сложности "hard": 3 days 14:55:19.
# Среднее время между выбором уровня сложности и оплатой для пользователей, выбравших уровень сложности "easy": 3 days 14:58:52.
# Среднее время между выбором уровня сложности и оплатой для пользователей, выбравших уровень сложности "medium": 3 days 23:14:13.
# Среднее время между выбором уровня сложности и оплатой для пользователей, выбравших уровень сложности "hard": 3 days 07:20:41.

# Наибольшее время между событием регистрации / выбора уровня сложности и моментом первой оплаты, тратят пользователи, которые выбрали уровень сложности medium.
# Пользователи, выбравшие уровень сложности easy, тратят времени незначительно меньшеб чем которые выбрали уровень сложности medium(разница порядка 10%).
# Меньше всего времени на принятие решение о покупке тратят пользователи, выбравшие уровень сложности hard.

# Таким образом, по результату изучения датасета можно сделать вывод, что больше всего совершали оплату пользователи, 
# которые завершали обучение и выбирали высокий уровень сложности тренировки. Также данные пользователи тратили меньше времени на обдумывание оплаты. 
# Следовательно, на данная группая пользователей является "ключевым клиентом" приложения, так как именно она больше всего влияет на выручку.






