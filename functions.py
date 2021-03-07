import pandas as pd
import numpy as np
from tqdm import tqdm
import os

path = './csv/'

def clean_data(file):
    data = pd.read_csv(path+file, low_memory=False)
    data = data[~data['#event_name'].isin(['push_send'
                                         ,'xs_user_online'
                                         ,'ta_app_start'
                                         ,'ta_app_end'
                                         ,'new_grade'
                                         ,'skilled_player'])]
    data['#event_time'] = pd.to_datetime(data['#event_time'])
    data['date'] = data['#event_time'].dt.date

    data.sort_values(by=['#account_id','#event_time'], inplace=True)
    
    return data

def get_churn_ids(file):
    data = pd.read_csv(path+file, low_memory=False)
    data = data[~data['#event_name'].isin(['push_send'
                                             ,'xs_user_online'
                                             ,'ta_app_start'
                                             ,'ta_app_end'
                                             ,'new_grade'
                                             ,'skilled_player'])]
    churn_ids = data['#account_id'].unique().tolist()
    return churn_ids

def get_df(data):

    df = pd.DataFrame()
    df['#account_id'] = data['#account_id'].unique()

    df_ = data[data['#account_id'].notnull()]\
            [['#account_id',
              '#province',
              #'#device_model',
              '#city',
              #'#device_id',
              '#manufacturer',
              '#app_version',
              '#os',
              #'#os_version',
              '#carrier',
              #'#ip',
              '#screen_width',
              '#screen_height',
              '#network_type',
              'birthday',
              'register_time',
              'channel.1'
             ]].copy()

    df_.drop_duplicates(subset='#account_id',keep='last', inplace=True, ignore_index=True)

    df = df.merge(df_, on='#account_id', how='left')
    df.set_index('#account_id', inplace=True)
    
    return df

def count_unique_event(data):
    return data.groupby('#account_id')['#event_name'].nunique().to_frame(name='event_cnt_unique')

def count_total_events(data):
    return data.groupby('#account_id')['#event_name'].count().to_frame(name='event_cnt_total')

def count_each_event(data):
    group = data.groupby(['#account_id','#event_name']).size().unstack()
    group = group.add_prefix('event_cnt_')
    return group

def get_event_time(data):
    data['time'] = data['#event_time'].apply(lambda x: x.hour + x.second/60)
    group = data.groupby('#account_id')['time'].agg(time_min='min', time_max='max')
    group['time_range'] = group['time_max'] - group['time_min']
    return group

def get_event_time_by_event(data):
    group = data.groupby(['#account_id','#event_name'])['time'].agg(time_min='min', time_max='max')
    group['time_range'] = group['time_max'] - group['time_min']
    group = group.unstack()
    group.columns = ['_'.join(t) for t in group.columns]
    return group

def add_features(data, features_list):
    features_dict = {
                     'cue':count_unique_event(data),
                     'cte':count_total_events(data),
                     'cee':count_each_event(data),
                     'get':get_event_time(data),
                     'getbe':get_event_time_by_event(data)
                    }
    features_list_=[]
    for f in features_list:
        features_list_.append(features_dict[f])
    return pd.concat(features_list_, axis=1)

def fe(file1, file2, features_list):
    
    data = clean_data(file1)
    df = get_df(data)
    
    df_featured = df.join(add_features(data, features_list), how='left')
    
    # get churn ids
    churn_ids = get_churn_ids(file2)
    df_featured['churn'] = df_featured.index.map(lambda x: 1 if x in churn_ids else 0)
    
    return df_featured
                            
def get_big_df(features_list):
    numbers = [int(x[:-4]) for x in os.listdir(path)]
    df_list = []
    churn_rate_list = []
    for i in tqdm(range(len(numbers)-1)):
        df = fe('%d.csv'%sorted(numbers)[i],
                '%d.csv'%sorted(numbers)[i+1],
                features_list)
        churn_rate_list.append(df['churn'].sum()/len(df))
        df_list.append(df)

    big_df = pd.concat(df_list, ignore_index=True)    
    return big_df, numbers, churn_rate_list                        
