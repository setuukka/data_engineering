import pandas as pd
import streamlit as st
import os

directory = "./polar_files/"
csv_path = os.path.join(directory, 'general_information.csv')

df = pd.read_csv(csv_path)
df['startTime'] = pd.to_datetime(df['startTime'])
df['stopTime'] = pd.to_datetime(df['stopTime'])
print(df.info())
st.title("Excersize data")
print(df.head(10))
#st.bar_chart(data = df, x = 'startTime', y = 'distance')