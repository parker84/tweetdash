import streamlit as st
from src.data.scrape_tweets_from_user import TweetScraper, get_user_id_from_user_name, db_conn
from src.data.get_user_data import UserData
from src.data.update_app_data import save_user_login_event
from decouple import config
import plotly.express as px
import coloredlogs, logging
logger = logging.getLogger(__name__)
coloredlogs.install(level=config('LOG_LEVEL'))

st.set_page_config(
    page_title="Twitter Growth Analytics", 
    page_icon=":eagle:", 
    layout="wide"
)


st.title("🚀 Twitter Status Dashboard")
st.markdown("🪓 Hack your Twitter growth with 🦅Twitter Growth Analytics")
st.sidebar.title("🦅Twitter Growth Analytics")

if 'user_name' not in st.session_state:

    with st.expander('login / signup', expanded=True):
        with st.form("login_form"):
            st.write("Login / Sign Up")
            email = st.text_input('Enter Your Email'),
            user_name = st.text_input("Enter Your Twitter User Name", "@parker_brydon")
            
            submitted = st.form_submit_button("Submit")
            if submitted:
                st.session_state['user_name'] = user_name
                save_user_login_event(user_name, email)

if 'user_name' in st.session_state:
    user_name = st.text_input("Enter Twitter User Name", st.session_state['user_name'])
    user_name = user_name.strip('@')

    user_id = get_user_id_from_user_name(user_name)
    data_getter = UserData(user_id)
    is_new_user = data_getter.check_if_new_user()
    if is_new_user:
        with st.spinner(f'Scraping twitter data for user: {user_name}'):
            scraper = TweetScraper(user_name)
        st.success('Done!')
    data_getter = UserData(user_id)
    weekly_metrics_for_user = data_getter.get_weekly_metrics_for_user().sort_values(by='week_begin_date')
    weekly_metrics_for_user = weekly_metrics_for_user[
        ~weekly_metrics_for_user['count_followers_that_interacted'].isnull()
    ]
    user_meta_data = data_getter.get_user_dimensions()
    cohorted_metrics_for_user = data_getter.get_cohorted_metrics_for_user().sort_values(by='first_active_week_begin_date')
    cohorted_metrics_for_user = cohorted_metrics_for_user[
        ~cohorted_metrics_for_user['retention_rate_1w'].isnull()
    ].round(2)

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "Followers Count", user_meta_data['count_followers'].iloc[-1]
        )

        weekly_counts_per_user_melted = weekly_metrics_for_user.melt(
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
            weekly_counts_per_user_melted,
            x='week_begin_date', 
            y='count', 
            color='metric',
            title='Weekly Follower Engagement',
            template='ggplot2'
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.metric(
            "Active Followers Count", weekly_metrics_for_user['count_active_followers'].iloc[0]
        )

        weekly_counts_per_user_melted = weekly_metrics_for_user.melt(
            id_vars='week_begin_date', 
            value_vars=[
                'count_net_active_followers',
                'count_new_active_followers',
                'count_churned_active_followers'
            ], var_name='metric', value_name='count'
        ).sort_values(by='week_begin_date')

        fig = px.line(
            weekly_counts_per_user_melted,
            x='week_begin_date',
            y='count', 
            color='metric',
            title='Weekly Active Follower Changes',
            template='ggplot2'
        )
        st.plotly_chart(fig, use_container_width=True)

    with col3:
        st.metric(
            "1 Week Active Follower Retention", cohorted_metrics_for_user['retention_rate_1w'].iloc[-1]
        )
        cohorted_metrics_melted = cohorted_metrics_for_user.melt(
            id_vars='first_active_week_begin_date', 
            value_vars=[
                'retention_rate_1w',
                'retention_rate_4w'
            ], var_name='metric', value_name='%'
        ).sort_values(by='first_active_week_begin_date')

        fig = px.line(
            cohorted_metrics_melted,
            x='first_active_week_begin_date', 
            y='%', 
            color='metric',
            title='Retention Rates',
            template='ggplot2'
        )
        st.plotly_chart(fig, use_container_width=True)