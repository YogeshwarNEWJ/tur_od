# handling data
import numpy as np
import pandas as pd

import os
import io
from io import BytesIO, StringIO
import time
from push_blob import push_blob_f

# MY-SQL connection
import mysql.connector
# from mysql.connector import Error
# import pymysql

# Date-Time
import  datetime
from datetime import datetime, date, timedelta

# model deployment
import streamlit as st

# utils
import os
import joblib

# hide streamlit style
hide_streamlit_style = """
            <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            </style>
            """
st.markdown(hide_streamlit_style, unsafe_allow_html=True)

# connect with Azure-db
cnx = mysql.connector.connect(host = 'mysqlservernewjprod.mysql.database.azure.com', user = 'phantom@mysqlservernewjprod', password = 'Zurich$1', db = 'fb', port = 3306)
cursor = cnx.cursor(buffered=True)

# Find the value From Dictionary
def get_value(val,my_dict):
	for key ,value in my_dict.items():
		if val == key:
			return value

# Find the Key From Dictionary
def get_key(val,my_dict):
	for key ,value in my_dict.items():
		if val == value:
			return key

# Load Models
def load_model_n_predict(model_file):
	loaded_model = joblib.load(open(os.path.join(model_file),"rb"))
	return loaded_model

# Load Models
def load_transformer(model_file):
	transformed_data = joblib.load(open(os.path.join(model_file),"rb"))
	return transformed_data


def main():
    """ NEWJPLUS: FACEBOOK VIEWS PREDICTION MODEL"""
    st.title('TURING: NEWJ PREDICTION MODEL')
    menu = ["About", "How To Use Turing","Prediction", "Feedback"]
    choice = st.sidebar.selectbox('MENU', menu)
    golden = pd.read_csv("data/turing_data.csv")

    # Basic information about the project
    if choice == 'About':
        st.subheader("1. ABOUT:")
        st.text('Turing is views prediction model for NewjPlus Facebook page')
        st.image("images/newj_fb_page.png", width= 600)
        st.subheader("2. OBJECTIVE:")
        st.text('The objective is to predict views, the video can get over seven days from the date,\nthe video got published.')
        st.image("images/newj_fb_views.png", width=600)
        st.subheader("3. HOW TURING WORKS:")
        st.text('The prediction model is trained on 861 videos. The model predicts views based on several\n'
                'parameters. Each parameter is important to predict views, so giving correct input\n'
                'is very important to get an accurate prediction. Model try to predict views for the\n'
                'given combinations from user based on data on which it is trained.')

    elif choice == 'How To Use Turing':
        st.subheader("**1. Primary Category**")
        st.markdown("**Government:** All about Government's rules, announcements, policies, decisions, economy, budget and any such news")
        st.markdown("**Global news:** Any news/ info about the world is a part of this category. ")
        st.markdown("**Technology & Innovations:** This includes Science, Technology and Innovation related news, information, updates and anything that has science in it.")
        st.markdown("**Sports & Games:** All about the news, updates awards, achievements,  national/international tournaments or players in any sport or game. Life journey of a well known or budding sportsmen or player. Any information about rules/decisions in sports/games.")
        st.markdown("**Politics:** Everything that has info about a political party/individual and elections")
        st.markdown("**Entertainment:** All about Entertainment industry, bollywood, regional, television, digital. Also includes the videos of common people who are entertaining us through some dance or song")
        st.markdown("**Environment/Ecosystem:** All about flora-fauna, conservation, crisis,pollution, cleaning the surroundings and waste management. Any news or updates or stories about social/volunteer work for environment or wildlife/pets/street animals")
        st.markdown("**Lifestyle:** All about the way of living, religion, culture, sexuality,economy, philisophy,beliefs etc")
        st.markdown("**Places:** All the stories/ info about any place like city, country, or village. It also includes anything related to a religious/historical place.")
        st.markdown("**Judiciary & Crime:** All about judicial system, laws, policies, rules, norms, crime, violations, violence, court ruling, cases etc or any information about anyone or anything that's a part of judicial system or crime")
        st.markdown("**Rare:** Any other categories will fall into rare")

        st.subheader("**2. Video Type**")
        st.markdown("**Emotions:** 'funny', 'emotional', 'shocking/surprising', and 'scary' type of videos will fall into emotions")
        st.markdown("**Rare:** All other categories than 'informative', 'inspiring', 'explainers', 'day-specific' and 'emotions' will fall into Rare")

        st.subheader("**3. Background Music In First 3 Seconds**")
        st.markdown("**No Music:** If the background music is not starting in first 3 seconds then we call it as 'no music")
        st.markdown("**Relevant:** If the background music is matching with video then call it as relevant")
        st.markdown("**Low, Loud and Neutral:** If the background music is not relevant to the video then it can fall into 'Low', 'Loud' and 'Neutral' based on the loudness of the music")

        st.subheader("**4. Voice In First 3 Seconds**")
        st.markdown("**Text Only:** If the video do not have any voice in first 3 seconds but it consist of text then we call it as 'text only'")
        st.markdown("**Common Voice:** If the video consist of human voice in first 3 seconds and voice is not of the famous personality then it is a 'common voice'")
        st.markdown("**Voice Of Famous Personality:** If the video have voice of famous personality in first 3 seconds.")
        st.markdown("**Other:** If the video start with voice of 'crowd', 'animals', 'nature' then it falls into 'other' category")
        st.markdown("**Rare:** Anything else than above mentioned categories will fall into 'rare'")

        st.subheader("**5. Thumbnail**")
        st.markdown("**Rare:** If the thumbnail does not contain 'Object', 'Crowd', 'Famous Personality', or 'Commoner' then it is thumbnail.")


    # Prediction
    elif choice == 'Prediction':
        # dictionary of encoded variables
        d_background_music_type_first_3_seconds = {'loud': 0,
                                                  'relevant': 1,
                                                  'neutral': 2,
                                                  'low': 3,
                                                  'no music': 4}

        d_primary = {'sports & games': 0,
                                       'government': 1,
                                       'global news': 2,
                                       'technology & innovations': 3,
                                       'politics': 4,
                                       'places': 5,
                                       'environment/ecosystem': 6,
                                       'rare': 7,
                                       'entertainment': 8,
                                       'lifestyle': 9,
                                       'judiciary & crime': 10}

        d_video_type = {'informative': 0,
                        'inspiring': 1,
                        'explainers': 2,
                        'emotions': 3,
                        'rare': 4,
                        'day-specific': 5}

        d_voice_first_3_seconds = {'text only (to)': 0,
                                   'other': 1,
                                   'rare': 2,
                                   'common voice (optv)': 3,
                                   'voice of famous personality (fpv)': 4}

        d_thumbnail = {'object': 0,
                       'crowd': 1,
                       'famous personality': 2,
                       'commoner': 3,
                       'rare': 4}

        d_time_distribution = {'9 AM - 12 PM': 0,
                              '3 PM - 6 PM': 1,
                              '9 PM - 12 AM': 2,
                              '6 PM - 9 PM': 3,
                              '12 PM - 03 PM': 4,
                              '6 AM - 9 AM': 5}


        # OD
        basepath = '.'

        st.set_option('deprecation.showfileUploaderEncoding', False)

        st.header("stream app")

        def clean_cache():
            with st.spinner("Cleaning....."):
                os.system(f'rm {basepath}/*mp4')

        if st.button(label="Clean cache"):
            clean_cache()

        file = st.file_uploader("Upload file", type=["mp4"])

        show_f = st.empty()

        if not file:
            pass
            # show_f.info("Upload file")

        else:
            if isinstance(file, BytesIO):
                show_f.video(file)

                # os.system(f'rm *mp4')
                file_name = time.strftime("%Y%m%d-%H%M%S")

                with open(f'{basepath}/{file_name}.mp4', 'wb') as f:
                    f.write(file.read())
            if st.button(label="Upload BLOB"):
                push_blob_f(video_id=file_name, container='var', basepath='.')

        # Take user input GOLDEN DATA
        primary = st.selectbox('Primary Category', tuple(d_primary.keys()))
        video_type = st.selectbox('Video Type', tuple(d_video_type.keys()))
        voice_first_3_seconds = st.selectbox('Voice In First 3 Seconds', tuple(d_voice_first_3_seconds.keys()))
        background_music_type_first_3_seconds = st.selectbox('Background Music In First 3 Seconds', tuple(d_background_music_type_first_3_seconds.keys()))
        thumbnail = st.selectbox('Thumbnail', tuple(d_thumbnail.keys()))
        time_distribution = st.selectbox('Time Slot', tuple(d_time_distribution.keys()))

        published_date = st.date_input('Video Publishing Date')
        # st.text(type(published_date))
        # Any production date after today will be consider as today only
        def correct_date(date_):
            if date_ > (datetime.now()).date():
                return (datetime.now()).date()
            elif date_ < date(2019, 3, 18):
                return date(2019, 3, 18)
            else:
                return date_

        published_date = correct_date(published_date)
        #st.write(published_date)

        last_day_features = ['last_day_page_consumptions_by_consumption_type_other_clicks', 'last_day_page_fan_adds_by_paid_non_paid_unique_unpaid',
        'last_day_page_posts_impressions_nonviral_unique', 'last_day_page_fans', 'last_day_page_fan_removes_unique',
        'last_day_page_video_complete_views_30s_paid', 'last_day_page_video_complete_views_30s_repeat_views', 'last_day_page_video_views_10s']

        last_seven_day_features = ['last_7_days_page_fan_adds_by_paid_non_paid_unique_unpaid', 'last_7_days_page_impressions_nonviral',
        'last_7_days_page_impressions_nonviral_unique', 'last_7_days_page_posts_impressions_nonviral_unique',
        'last_7_days_page_actions_post_reactions_wow_total', 'last_7_days_page_actions_post_reactions_sorry_total',
        'last_7_days_page_fans', 'last_7_days_page_fan_removes_unique', 'last_7_days_page_video_complete_views_30s_repeat_views']

        last_day_features_values = []
        last_seven_day_features_values = []

        # read the page data from database
        page_sql = 'SELECT * FROM fb.pageinsightsdaily where pageName = "NEWJPLUS"'
        ld_df = pd.read_sql(page_sql, cnx)

        # last day features extraction
        for feature in last_day_features:
            last_day_features_values.append(
                (ld_df[ld_df['consolidated_end_time'] == published_date - timedelta(days=1)][feature[9:]]).reset_index(
                    drop=True)[0])
            cursor.close()

        # last seven days features extraction
        for feature in last_seven_day_features:
            value = 0
            for i in range(1,8):
                value = value + (ld_df[ld_df['consolidated_end_time'] == published_date - timedelta(days=i)][feature[12:]]).reset_index(
                    drop=True)[0]
            last_seven_day_features_values.append(value)
            cursor.close()

        # GET VALUES FOR EACH INPUT
        k_primary = get_value(primary, d_primary)
        k_video_type = get_value(video_type, d_video_type)
        k_voice_first_3_seconds = get_value(voice_first_3_seconds, d_voice_first_3_seconds)
        k_background_music_type_first_3_seconds = get_value(background_music_type_first_3_seconds,
                                                                d_background_music_type_first_3_seconds)
        k_thumbnail = get_value(thumbnail, d_thumbnail)
        k_time_distribution = get_value(time_distribution, d_time_distribution)

        # RESULT OF USER INPUT
        vectorized_result = [k_background_music_type_first_3_seconds, k_primary, k_video_type,
                             k_voice_first_3_seconds, k_thumbnail,
                             k_time_distribution] + last_day_features_values + last_seven_day_features_values

        # st.text(vectorized_result)
        sample_data = np.array(vectorized_result).reshape(1, -1)
        # print(sample_data)
        # st.write(sample_data)


        # from sklearn.preprocessing import MinMaxScaler

        tr = load_transformer("models/pkl_transform_1.pkl")
        transformed_sample_data = tr.transform(pd.DataFrame(data = sample_data, columns= ['visual_first_3_seconds', 'background_music_type_first_3_seconds',
       'Primary_Category_1_grouped', 'voice_first_3_seconds_grouped',
       'thumbnail_1_grouped', 'time_distribution',
       'last_day_page_fan_adds_by_paid_non_paid_unique_total',
       'last_day_page_fan_adds_by_paid_non_paid_unique_unpaid',
       'last_day_page_impressions_paid',
       'last_day_page_impressions_paid_unique',
       'last_day_page_posts_impressions_paid',
       'last_day_page_posts_impressions_viral',
       'last_day_page_posts_impressions_nonviral_unique',
       'last_day_page_actions_post_reactions_haha_total', 'last_day_page_fans',
       'last_day_page_video_complete_views_30s_repeat_views',
       'last_7_days_page_negative_feedback_by_type_hide_clicks',
       'last_7_days_page_impressions_nonviral_unique',
       'last_7_days_page_posts_impressions_nonviral_unique',
       'last_7_days_page_fans', 'last_7_days_page_fan_removes',
       'last_7_days_page_fan_removes_unique',
       'last_7_days_page_video_complete_views_30s_repeat_views']))


        # print(transformed_sample_data)
        # st.write(transformed_sample_data)

        if st.button("Make Prediction"):
            #prediction_label = {"low": 0, "average": 1, 'high': 2}
            model_predictor = load_model_n_predict("models/classification_xgb_newjplus_without_scaled.pkl")
            prediction = model_predictor.predict(transformed_sample_data)
            # st.text(prediction)
            #final_result = get_key(prediction, prediction_label)
            st.success("Predicted video category is --> {}".format(prediction[0].upper()))
            st.write('Low: 0-12800')
            st.write('Average: 12801-39000')
            st.write('High: Any number of views above 39000')

    else:
        feedback = st.subheader('For any feedback/suggestion,\nkindly email to [Yogeshwar Thosare] (mailto:yogeshwar.thosare@thenewj.com) or slack [@yogeshwar] (https://app.slack.com/client/TEERTFU84/DRPJGCL81)')



if __name__ == '__main__':
    main()