import streamlit as st
from src.data.get_user_data import UserData
from src.data.scrape_tweets_from_user import get_user_id_from_user_name
from datetime import date


st.set_page_config(
    page_title="Twitter Growth Analytics", 
    page_icon=":eagle:", 
    layout="wide"
)

st.title("👣 Follower Explorer")
st.markdown("🪓 Hack your Twitter growth with 🦅Twitter Growth Analytics")
st.sidebar.title("🦅Twitter Growth Analytics")

user_name = st.text_input("Enter Your User Name", "@parker_brydon")
user_name = user_name.strip('@')
user_id = get_user_id_from_user_name(user_name)


data_getter = UserData(user_id)
metrics_per_follower = data_getter.get_follower_metrics_for_user()

st.dataframe(metrics_per_follower)