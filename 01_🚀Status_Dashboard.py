import streamlit as st
from src.data.scrape_tweets_from_user import TweetScraper
from src.data.get_user_data import UserData
from datetime import date


st.set_page_config(
    page_title="Twitter Growth Analytics", 
    page_icon=":eagle:", 
    layout="wide"
)

st.title("🚀 Twitter Status Dashboard")
st.markdown("🪓 Hack your Twitter growth with 🦅Twitter Growth Analytics")
st.sidebar.title("🦅Twitter Growth Analytics")

user_name = st.text_input("Enter Your User Name", "@parker_brydon")
user_name = user_name.strip('@')

start_date = st.sidebar.date_input(label='Start date', value=date(2022, 6, 1))

scraper = TweetScraper(user_name, start_time=f'{str(start_date)}T00:00:00Z')
data_getter = UserData(scraper.user_id)
weekly_metrics_for_user = data_getter.get_weekly_metrics_for_user()
user_meta_data = data_getter.get_user_dimensions()
