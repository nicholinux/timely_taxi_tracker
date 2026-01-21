import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from db import get_connection
from taxi_etl import (
    get_daily_revenue,
    get_trips_per_day,
    get_trips_per_hour,
    get_avg_fare,
    get_avg_trip_duration
)

# Set page config
st.set_page_config(page_title="NYC Taxi Tracker", layout="wide")

# Title
st.title("🚕 NYC Taxi Analytics Dashboard")

# Sidebar for date selection
st.sidebar.header("Filters")
date_range = st.sidebar.date_input(
    "Select date range:",
    value=(datetime.now() - timedelta(days=30), datetime.now()),
    max_value=datetime.now()
)

if len(date_range) == 2:
    start_date = pd.Timestamp(date_range[0])
    end_date = pd.Timestamp(date_range[1])
else:
    start_date = pd.Timestamp(datetime.now() - timedelta(days=30))
    end_date = pd.Timestamp(datetime.now())


con = get_connection()

# Tabs for different views
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "Daily Revenue",
    "Trips per Day",
    "Trips per Hour",
    "Average Fare",
    "Trip Duration"
])

# Tab 1: Daily Revenue
with tab1:
    st.subheader("Daily Revenue Trends")
    revenue_df = get_daily_revenue(con, start_date, end_date)
    if not revenue_df.empty:
        st.line_chart(data=revenue_df.set_index('trip_date')['total_revenue'])
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Revenue", f"${revenue_df['total_revenue'].sum():,.2f}")
        with col2:
            st.metric("Average Daily Revenue", f"${revenue_df['total_revenue'].mean():,.2f}")
        with col3:
            st.metric("Max Daily Revenue", f"${revenue_df['total_revenue'].max():,.2f}")
        
        st.dataframe(revenue_df, use_container_width=True)
    else:
        st.info("No data available for the selected date range")

# Tab 2: Trips per Day
with tab2:
    st.subheader("Trip Volume by Day")
    trips_df = get_trips_per_day(con, start_date, end_date)
    if not trips_df.empty:
        st.bar_chart(data=trips_df.set_index('trip_date')['trip_count'])
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Trips", f"{trips_df['trip_count'].sum():,}")
        with col2:
            st.metric("Average Trips/Day", f"{trips_df['trip_count'].mean():,.0f}")
        with col3:
            st.metric("Max Trips in a Day", f"{trips_df['trip_count'].max():,}")
        
        st.dataframe(trips_df, use_container_width=True)
    else:
        st.info("No data available for the selected date range")

# Tab 3: Trips per Hour
with tab3:
    st.subheader("Average Trip Duration by Hour")
    hourly_df = get_trips_per_hour(con, start_date, end_date)
    if not hourly_df.empty:
        st.line_chart(data=hourly_df.set_index('pickup_hour')['avg_trip_minutes'])
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Average Duration (Overall)", f"{hourly_df['avg_trip_minutes'].mean():.1f} min")
        with col2:
            peak_hour = hourly_df.loc[hourly_df['avg_trip_minutes'].idxmax(), 'pickup_hour']
            st.metric("Peak Duration Hour", f"{int(peak_hour)}:00")
        
        st.dataframe(hourly_df, use_container_width=True)
    else:
        st.info("No data available for the selected date range")

# Tab 4: Average Fare
with tab4:
    st.subheader("Average Fare Trends")
    fare_df = get_avg_fare(con, start_date, end_date)
    if not fare_df.empty:
        st.line_chart(data=fare_df.set_index('trip_date')['avg_fare'])
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Overall Average Fare", f"${fare_df['avg_fare'].mean():,.2f}")
        with col2:
            st.metric("Max Average Fare", f"${fare_df['avg_fare'].max():,.2f}")
        with col3:
            st.metric("Min Average Fare", f"${fare_df['avg_fare'].min():,.2f}")
        
        st.dataframe(fare_df, use_container_width=True)
    else:
        st.info("No data available for the selected date range")

# Tab 5: Trip Duration
with tab5:
    st.subheader("Average Trip Duration")
    duration_df = get_avg_trip_duration(con, start_date, end_date)
    if not duration_df.empty:
        avg_duration = duration_df['avg_trip_minutes'].values[0]
        st.metric("Average Trip Duration", f"{avg_duration:.1f} minutes")
        st.info(f"For the period from {start_date.date()} to {end_date.date()}")
    else:
        st.info("No data available for the selected date range")
