import streamlit as st
from src.data.get_tweets_from_user import TweetScraper
from datetime import date


st.set_page_config(
    page_title="Advanced Twitter Analytics", 
    page_icon=":eagle:", 
    layout="wide"
)

st.title("🚀 Twitter Status Dashboard")
st.markdown("🪓 Hack your Twitter growth with 🦅 Advanced Twitter Analytics")
st.sidebar.title("🦅 Advanced Twitter Analytics")

user_name = st.text_input("Enter Your User Name", "@parker_brydon")
user_name = user_name.strip('@')

start_date = st.sidebar.date_input(label='Start date', value=date(2022, 6, 1))

scraper = TweetScraper(user_name, start_time=f'{str(start_date)}T00:00:00Z')
user_timeline_df = scraper.get_user_timeline()
st.dataframe(user_timeline_df)
user_meta_data_df = scraper.get_user_meta_data()
st.dataframe(user_meta_data_df)
