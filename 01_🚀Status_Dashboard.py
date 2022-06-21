import streamlit as st
from src.data.scrape_tweets_from_user import TweetScraper, get_user_id_from_user_name
from src.data.get_user_data import UserData
from datetime import date
import plotly.express as px


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

start_date = st.sidebar.date_input(label='Start Date', value=date(2022, 6, 1))

scraper = TweetScraper(user_name, start_time=f'{str(start_date)}T00:00:00Z')
user_id = get_user_id_from_user_name(user_name)
data_getter = UserData(user_id)
weekly_metrics_for_user = data_getter.get_weekly_metrics_for_user()
user_meta_data = data_getter.get_user_dimensions()


weekly_counts_per_user = weekly_metrics_for_user.melt(
    id_vars='week_begin_date', 
    value_vars=[
        'count_active_followers',
        'count_followers_that_liked',
        'count_followers_that_replied',
        'count_followers_that_retweeted',
        'count_followers_that_quoted'
    ], var_name='metric', value_name='count'
).sort_values(by='week_begin_date')

fig = px.line(
    weekly_counts_per_user,
    x='week_begin_date', 
    y='count', 
    color='metric',
    title='Weekly Follower Engagement'
)
st.plotly_chart(fig, use_container_width=True)