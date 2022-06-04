import streamlit as st
from utils.get_tweets_from_user import TweetScraper
from datetime import date


st.set_page_config(
    page_title="TweetDash", 
    page_icon=":eagle:", 
    layout="wide"
)

st.title("🦅TweetDash🦅")

user_name = st.text_input("Enter Your User Name", "@elonmusk")
user_name = user_name.strip('@')

st.sidebar.selectbox(
    label="Choose Your ⭐️ Northstar Metric",
    options=["New Followers", "New Active Followers"],
    index=0
)

start_date = st.sidebar.date_input(label='Start date', value=date(2022, 6, 1))

scraper = TweetScraper(user_name, start_time=f'{str(start_date)}T00:00:00Z')
df = scraper.get_user_timeline()

st.dataframe(df)
