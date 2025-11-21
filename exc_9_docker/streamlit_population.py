
#b) Use streamlit and create an interactive web graph where you can select the countries 
#to be included in the population plot

#Read data:
import pandas as pd
df_visu = pd.read_csv('population_country_columns.csv')

import streamlit as st
st.title('Population plot')
#Select the column we wish to plot
columns = st.multiselect('Countries',(df_visu.drop(columns = ['year'])).columns, key = 'line_selector')

#Plot the line chart
st.line_chart(df_visu, x = 'year',y = columns, y_label='Population', x_label='Year')

#Extra task:
#Stacked barplot with streamlit and maplotli
#Streamlit can handle matplotlib and plotly graphics
columns = st.multiselect('Countries',(df_visu.drop(columns = ['year'])).columns, key = 'bar_selector')

import matplotlib.pyplot as plt
fig, ax = plt.subplots(figsize = (10,6))
bottom_line = 0
for country_name in columns:
     plt.bar(df_visu['year'],df_visu[country_name]/1000000, bottom = bottom_line, width = 4,label = country_name) #Don't define the color
     bottom_line = bottom_line + (df_visu[country_name].values)/1000000
#ax.set_xticks(np.arange(1952,2015,5)) #
plt.title('Population of Nordics [Data source?]')
plt.ylabel('Population [millions]')
plt.xlabel('Year')
plt.legend()
plt.grid()
st.pyplot(fig)
