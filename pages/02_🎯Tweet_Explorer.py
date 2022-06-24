import streamlit as st
from src.data.get_user_data import UserData
from src.data.scrape_tweets_from_user import get_user_id_from_user_name
from src.data.update_app_data import save_user_login_event


st.set_page_config(
    page_title="Twitter Growth Analytics", 
    page_icon=":eagle:", 
    layout="wide"
)

st.title("🎯 Tweet Explorer")
st.markdown("🪓 Hack your Twitter growth with 🦅Twitter Growth Analytics")
st.sidebar.title("🦅Twitter Growth Analytics")

if 'user_name' not in st.session_state:

    with st.expander('login / signup', expanded=True):
        with st.form("login_form"):
            st.write("Login / Sign Up")
            email = st.text_input('Enter Your Email')
            user_name = st.text_input("Enter Your Twitter User Name", "@parker_brydon")
            
            submitted = st.form_submit_button("Submit")
            if submitted:
                st.session_state['user_name'] = user_name
                save_user_login_event(user_name, email)

if 'user_name' in st.session_state:

    user_name = st.text_input("Enter Your User Name", st.session_state['user_name'])
    user_name = user_name.strip('@')
    user_id = get_user_id_from_user_name(user_name)

    data_getter = UserData(user_id)
    metrics_per_tweet = data_getter.get_tweet_metrics_for_user()

    st.dataframe(metrics_per_tweet)