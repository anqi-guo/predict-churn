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
              #'birthday',
              'register_time',
              'qudao',
              '#event_time'
             ]].copy()

    df_.drop_duplicates(subset='#account_id',keep='last', inplace=True, ignore_index=True)
    # change register_time to register_days
    for d in ['register_time','#event_time']:
        df_[d] = pd.to_datetime(df_[d]).dt.date
    df_['register_days'] = df_['#event_time'] - df_['register_time']
    df_['register_days'] = df_['register_days'].apply(lambda x: x.days)
    
    df = df.merge(df_.drop(['#event_time'],axis=1), on='#account_id', how='left')
    df.set_index('#account_id', inplace=True)
    
    
    
    return df

def count_unique_event(data):
    return data.groupby('#account_id')['#event_name'].nunique().to_frame(name='event_CNT_unique')

def count_total_events(data):
    return data.groupby('#account_id')['#event_name'].count().to_frame(name='CNT_total')

def count_each_event(data):
    group = data.groupby(['#account_id','#event_name']).size().unstack()
    group = group.add_prefix('CNT_')
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

# app使用时长
def app_duration(data):
    duration = data[data['#event_name']=='ta_app_end'].groupby('#account_id')['#duration'].agg(['sum','max','min','mean'])
    duration = duration.add_prefix('Duration_')
    return duration

# 人机本
def ai_guide(data):
    events_dict = {'ai_guide':['from']
                  ,'quit_ai_guide':['stage','juben_title']
                  ,'finish_ai_guide':['juben_title','is_win']
                  }

    new_features = []
    for event, attrs in events_dict.items():
        new_features_e = []
        for attr in attrs:
            group = data[data['#event_name']==event][['#account_id',attr]]\
                    .drop_duplicates(subset='#account_id',keep='last')\
                    .rename(columns={attr:'%s_LAST_%s'%(event, attr)})\
                    .set_index('#account_id')
            new_features_e.append(group)
        new_features.extend(new_features_e)

    df_features = pd.concat(new_features)
    df_features = df_features.fillna(0)
    
    return df_features

# 点击区域
def click_button(data):
    # event_name = click
    click = data[data['#event_name']=='click'].groupby(['#account_id','click_page']).size().unstack()
    click = click.add_prefix('CLICK_BUTTON_')
    click = click.fillna(0)

    page_dict = {'game_page':['guess','juben','party','under','wolf','wolf12','wolf6']
                ,'mine_page':['daily_bonus','visitor']
                ,'message_page':['msg_moment']
                ,'navigate_bar':['main_tab_home','main_tab_live','main_tab_msg','main_tab_profile']}

    for page, buttons in page_dict.items():
        click['CLICK_in_%s'%page] = 0
        for button in buttons:
            click['CLICK_in_%s'%page] += click['CLICK_BUTTON_%s'%button] 
            
    # event_name = party_page 房间页点击
    party_page_tab = data[data['#event_name']=='party_page'].groupby(['#account_id','click_tab']).size().unstack()
    party_page_tab = party_page_tab.add_prefix('CLICK_TAB_')
    party_page_button = data[data['#event_name']=='party_page'].groupby(['#account_id','click_button']).size().unstack()
    party_page_button = party_page_button.add_prefix('CLICK_BUTTON_')

    party_page = party_page_tab.join(party_page_button)
    party_page = party_page.fillna(0)
    party_page['CLICK_in_rooms_page'] = party_page.sum(axis=1)
    
    
    df_features = pd.concat([click
                            ,party_page])
    
    return df_features

# 浏览页面
def view_page(data):
    # 动态详情页和动态发布页
    view_moments = data[data['#event_name']=='view_page'].groupby(['#account_id','page_name']).size().unstack()
    view_moments = view_moments.add_prefix('VIEW_page_')
    
    # 商城页
    view_shop = data[data['#event_name']=='view_shop_page'].groupby('#account_id').size().to_frame(name='VIEW_page_shop')
    
    # 技能页
    view_skill = data[data['#event_name']=='view_skill_page'].groupby('#account_id').size().to_frame(name='VIEW_page_skill')
    
    # 个人页
    ## 看的是自己的个人页还是别人的个人页
    view_userpage = data[data['#event_name']=='view_user_homepage'].groupby(['#account_id','myself']).size().unstack()
    view_userpage.rename(columns={False:'VIEW_page_others_home',True:'VIEW_page_my_home'}, inplace=True)
    ## 看了多少个别的用户的个人页
    view_userpage2 = data[(data['#event_name']=='view_user_homepage')&(data['myself']==False)]\
                    .groupby(['#account_id'])['target_uid'].nunique().to_frame(name='VIEW_page_others_home_nunique')
        
    df_features = pd.concat([view_moments
                            ,view_shop
                            ,view_skill
                            ,view_userpage
                            ,view_userpage2
                            ])
    
    return df_features

# 浏览&点击房间
def view_click_rooms(data):
    df_list = []
    for k,v in {'exposure':'VIEW','click':'CLICK'}.items():
        # 房间页浏览OR点击tab数量
        tabs = data[data['#event_name']=='flow_%s'%k].groupby(['#account_id','tab']).size().unstack()
        tabs = tabs.add_prefix('%s_rooms_TAB_'%v)
        # 房间页浏览OR点击房间数量
        rooms = data[data['#event_name']=='flow_%s'%k]\
                    .groupby(['#account_id'])['rid']\
                    .count()\
                    .to_frame(name='%s_rooms_roomCnt'%v)
        ## 房间页浏览OR点击unique房间数量
        rooms_unique = data[data['#event_name']=='flow_%s'%k]\
                            .groupby(['#account_id'])['rid']\
                            .nunique()\
                            .to_frame(name='%s_rooms_uniqueRoomCnt'%v)
        ## 房间页各tab浏览OR点击房间数量
        rooms_bytab = data[data['#event_name']=='flow_%s'%k].groupby(['#account_id','tab'])['rid'].count().unstack()
        rooms_bytab = rooms_bytab.add_prefix('%s_rooms_TAB_'%v)
        rooms_bytab = rooms_bytab.add_suffix('_roomCnt')
        ### 房间页各tab浏览OR点击unique房间数量
        rooms_bytab_unique = data[data['#event_name']=='flow_%s'%k].groupby(['#account_id','tab'])['rid'].nunique().unstack()
        rooms_bytab_unique = rooms_bytab_unique.add_prefix('%s_rooms_TAB_'%v)
        rooms_bytab_unique = rooms_bytab_unique.add_suffix('_uniqueRoomCnt')
        # 是否滑动页面 unique_room_cnt >5 则滑动页面
        if k=='exposure':
            for tab in data[data['#event_name']=='flow_exposure']['tab'].unique():
                rooms_bytab_unique['is_slide_TAB_%s'%tab] = np.where(rooms_bytab_unique['VIEW_rooms_TAB_%s_uniqueRoomCnt'%tab]\
                                                                     > 5,1,0)
    
        df = pd.concat([tabs, rooms, rooms_unique, rooms_bytab, rooms_bytab_unique])
        df_list.append(df)
        
    df_features = pd.concat(df_list)
    df_features = df_features.fillna(0)
    
    # 点击率
    df_features['RATE_click_rooms'] = df_features['CLICK_rooms_roomCnt']/df_features['VIEW_rooms_roomCnt']
    
    for tab in data[data['#event_name']=='flow_click']['tab'].unique(): 
        df_features['RATE_click_rooms_TAB_%s'%tab] = df_features['CLICK_rooms_TAB_%s_roomCnt'%tab] / \
                                                     df_features['VIEW_rooms_TAB_%s_roomCnt'%tab]
    
    
    return df_features

# 房间内行为
def in_room(data):
    # 进房次数
    enter_room = data[data['#event_name']=='enter_room'].groupby('#account_id').size().to_frame(name='CNT_enter_room')
    
    ## 进房类型
    enter_room_byType = data[data['#event_name']=='enter_room'].groupby(['#account_id','room_type']).size().unstack()
    enter_room_byType = enter_room_byType.add_prefix('enter_room_TYPE_')
    
    enter_room_uniqueType = data[data['#event_name']=='enter_room'].groupby(['#account_id'])['room_type'].nunique().to_frame(name='enter_room_uniqueType')
    
    ## 进房入口
    enter_room_byRefer = data[data['#event_name']=='enter_room'].groupby(['#account_id','refer']).size().unstack()
    enter_room_byRefer = enter_room_byRefer.add_prefix('enter_room_REFER_')
    
    enter_room_uniqueRefer = data[data['#event_name']=='enter_room'].groupby(['#account_id'])['refer'].nunique().to_frame(name='enter_room_uniqueRefer')
    
    # 房间停留时间
    room_duration = data[data['#event_name']=='exit_room'].groupby('#account_id')['#duration'].agg(['sum','mean','max','min'])
    room_duration = room_duration.add_prefix('DURATION_room_')
    
    room_duration_byType = data[data['#event_name']=='exit_room'].groupby(['#account_id','room_type'])['#duration'].agg(['sum','mean','max','min']).unstack()
    room_duration_byType.columns = ['_TYPE_'.join(col).strip() for col in \
                                    room_duration_byType.columns.values]
    room_duration_byType = room_duration_byType.add_prefix('DURATION_room_')
    
    # 创建房间
    create_room = data[data['#event_name']=='create_room'].groupby('#account_id').size().to_frame(name='CNT_create_room')
    
    create_room_byType = data[data['#event_name']=='create_room'].groupby(['#account_id','room_type']).size().unstack()
    create_room_byType = create_room_byType.add_prefix('create_room_TYPE_')
    
    create_room_uniqueType = data[data['#event_name']=='create_room']\
                            .groupby(['#account_id'])['room_type']\
                            .nunique()\
                            .to_frame(name='create_room_uniqueType')
    
    # 点击头像
    click_profile = data[data['#event_name']=='room_click'].groupby("#account_id").size().to_frame(name='CNT_room_click_profile')
    
    # 点击邀请
    click_invite = data[data['#event_name']=='room_invite'].groupby("#account_id").size().to_frame(name='CNT_room_click_invite')
    
    click_invite_byRoomType = data[data['#event_name']=='room_invite'].groupby(["#account_id",'room_type']).size().unstack()
    click_invite_byRoomType = click_invite_byRoomType.add_prefix('room_invite_ROOMTYPE_')
    
    click_invite_byGameType = data[data['#event_name']=='room_invite'].groupby(["#account_id",'game_type']).size().unstack()
    click_invite_byGameType = click_invite_byGameType.add_prefix('room_invite_GAMETYPE_')

    # 点击搜索
    click_search = data[data['#event_name']=='room_search_click'].groupby("#account_id").size().to_frame(name='CNT_room_search_click')
    
    # 聊天
    room_chat = data[data['#event_name']=='room_public_chat'].groupby('#account_id').size().to_frame(name='CNT_room_chat')
    
    room_chat_byType = data[data['#event_name']=='room_public_chat'].groupby(['#account_id','msg_type']).size().unstack()
    room_chat_byType = room_chat_byType.add_prefix('room_chat_TYPE')
    
    # 上麦
    on_mic = data[data['#event_name']=='on_mic'].groupby('#account_id').size().to_frame(name='CNT_on_mic')
    
    on_mic_byRoomType = data[data['#event_name']=='on_mic'].groupby(["#account_id",'room_type']).size().unstack()
    on_mic_byRoomType = on_mic_byRoomType.add_prefix('on_mic_ROOMTYPE_')
    
    on_mic_byGameType = data[data['#event_name']=='on_mic'].groupby(["#account_id",'game_type']).size().unstack()
    on_mic_byGameType = on_mic_byGameType.add_prefix('on_mic_GAMETYPE_')
    
    # 下麦
    off_mic_duration = data[data['#event_name']=='room_off_mic']\
                        .groupby(['#account_id'])['duration']\
                        .agg(['max','min','sum','mean'])
    off_mic_duration = off_mic_duration.add_prefix('DURATION_on_mic_')
    
    df_features = pd.concat([enter_room
                            ,enter_room_byType
                            ,enter_room_uniqueType
                            ,enter_room_byRefer
                            ,enter_room_uniqueRefer
                            ,room_duration
                            ,room_duration_byType
                            ,create_room
                            ,create_room_byType
                            ,create_room_uniqueType
                            ,click_profile
                            ,click_invite
                            ,click_invite_byRoomType
                            ,click_invite_byGameType
                            ,click_search
                            ,room_chat
                            ,room_chat_byType
                            ,on_mic
                            ,on_mic_byRoomType
                            ,on_mic_byGameType
                            ,off_mic_duration])
    return df_features

# 游戏相关
def game_match(data):
    # 游戏匹配
    game_match = data[data['#event_name']=='game_match'].groupby(['#account_id','game_type'])\
                .size()\
                .unstack()
    game_match = game_match.add_prefix('game_match_TYPE_')
    
    # 游戏匹配成功
    ## 次数
    game_match_success_cnt = data[data['#event_name']=='game_match_success']\
                            .groupby(['#account_id','game_type'])\
                            .size()\
                            .unstack()
    game_match_success_cnt = game_match_success_cnt.add_prefix('game_match_success_TYPE_')
    ## 时长
    game_match_success_duration = data[data['#event_name']=='game_match_success']\
                                .groupby(['#account_id'])['#duration']\
                                .agg(['max','min','sum','mean'])
    game_match_success_duration = game_match_success_duration.add_prefix('DURATION_game_match_success_')
    game_match_success_duration_by_type = data[data['#event_name']=='game_match_success']\
                                .groupby(['#account_id','game_type'])['#duration']\
                                .agg(['max','min','sum','mean'])\
                                .unstack()

    game_match_success_duration_by_type.columns = ['_TYPE_'.join(col).strip() for col in \
                                                   game_match_success_duration_by_type.columns.values]
    game_match_success_duration_by_type = game_match_success_duration_by_type.add_prefix('DURATION_game_match_success_')

    
    # 计算游戏匹配成功率
    gm = pd.concat([game_match
                   ,game_match_success_cnt
                   ,game_match_success_duration
                   ,game_match_success_duration_by_type])
    gm = gm.fillna(0)
    
    for game in data[data['#event_name']=='game_match_success']['game_type'].unique():
        gm['RATE_game_match_success_TYPE_%s'%game] = gm['game_match_success_TYPE_%s'%game]/gm['game_match_TYPE_%s'%game]
        
    return gm

################################################################################################################################

def add_features(data, features_list):
    features_dict = {
        'cue':count_unique_event(data),
        'cte':count_total_events(data),
        'cee':count_each_event(data),
        'get':get_event_time(data),
        'getbe':get_event_time_by_event(data),
        'ai':ai_guide(data),
        'cb':click_button(data),
        'vp':view_page(data),
        'vcr':view_click_rooms(data),
        'gm':game_match(data),
        'ir':in_room(data),
        'dur':app_duration(data)
                    }
    features_list_=[]
    for f in features_list:
        features_list_.append(features_dict[f])
    return pd.concat(features_list_)

################################################################################################################################

def fe(file1, file2, features_list):
    
    data = clean_data(file1)
    df = get_df(data)
    
    df_featured = df.join(add_features(data, features_list), how='left')
    
    # get churn ids
    churn_ids = get_churn_ids(file2)
    df_featured['churn'] = df_featured.index.map(lambda x: 1 if x in churn_ids else 0)
    
    return df_featured

################################################################################################################################
                            
def get_big_df(features_list):
    numbers = [int(x[:-4]) for x in os.listdir(path) if x.endswith('.csv')]
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
