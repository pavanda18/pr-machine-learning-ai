import plotly.graph_objects as go
import snowflake.connector as snowflake
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from PIL import Image


# Snowflake connection details
snowflake_user = "pavan57"
snowflake_password = "Welcome@1"
snowflake_account = "zocleqv-sl23908"
snowflake_database = "CK_DEMO"
snowflake_schema = "ESG_DEMO"

# Establish Snowflake connection
conn = snowflake.connect(
    user=snowflake_user,
    password=snowflake_password,
    account=snowflake_account,
    database=snowflake_database,
    schema=snowflake_schema
)

# Set page layout to wide
st.set_page_config(layout="wide")

st.markdown(
    """
    <style>
    .custom-selectbox-label {
        font-size: 16px;  # Modify the font size as needed
        font-weight: 600; # Modify the font weight as needed
        color: #007BFF;  # Modify the font color as needed
    }
    </style>
    """,
    unsafe_allow_html=True,
)

def get_company_filter_options():
    sql_query = "SELECT DISTINCT COMPANY_NAME FROM REPRISK_INFO ORDER BY COMPANY_NAME ASC"
    cursor = conn.cursor()
    cursor.execute(sql_query)
    filter_options = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return filter_options


# Set the title of the Streamlit app
st.markdown('<span style="font-size: 40px;font-weight: 800;">ESG Risk Intelligence Dashboard <span style="font-size: 17px;font-weight: 100;">(powered by <span>[RepRisk](https://www.reprisk.com/)</span>)</span></span>', unsafe_allow_html=True,)
st.markdown('<span style="font-size: 20px;margin-top: -24px;position: absolute;">Sample Data from Jan-2021 to Dec-2021 </span><br>', unsafe_allow_html=True,)

def format_company_name(company_name):
  """Formats the company name to be bold and 18px in size."""
  return f'<span style="font-weight: bold; font-size: 800px;">{company_name}</span>'
    
# Process company filters
company_filter_options = get_company_filter_options()   
selected_company = st.selectbox("Select Company", company_filter_options, key='company_filter', format_func=lambda x: x.split(' (')[0])    
if 'Oreal SA (L' in selected_company:
    selected_company = 'Oreal SA'

# Fetches the RRI Score to show in the gauge chart
cur1 = conn.cursor()
max_qry = F"SELECT DISTINCT COMPANY_NAME, REPRISK_RATING, (SELECT MAX(CURRENT_RRI) FROM REPRISK_INFO WHERE COMPANY_NAME LIKE '%{selected_company}%') AS RRI FROM REPRISK_INFO WHERE COMPANY_NAME LIKE '%{selected_company}%'"
cur1.execute(max_qry)
res = cur1.fetchone()
company_name = res[0]
reprisk_rating = res[1]
max_gauge_value = res[2]
cur1.close()


def main():
    # Gauge Chart
    max_threshold = ''
    if 0 <= max_gauge_value <= 25:        
        max_threshold = 'Low'
    elif 26 <= max_gauge_value <= 49:         
        max_threshold = 'Medium'
    elif 50 <= max_gauge_value <= 59:         
        max_threshold = 'High'
    elif 60 <= max_gauge_value <= 74:         
        max_threshold = 'Very High'
    else:         
        max_threshold = 'Extreme High'

    max_chart = go.Figure(go.Indicator(
        mode="gauge+number",
        value=max_gauge_value,
        domain={'x': [0, 1], 'y': [0, 1]},
        gauge={
            'axis': {'range': [0, 100]},
            'threshold': {
                'line': {'color': "red", 'width': 4},
                'thickness': 0.75,
                'value': max_gauge_value
            },
            'steps': [
                {'range': [0, 25], 'color': '#8cc640'},
                {'range': [25, 50], 'color': '#fced23'},
                {'range': [50, 60], 'color': '#fbb40f'},
                {'range': [60, 75], 'color': '#f6851e'},
                {'range': [75, 100], 'color': '#d52127'}
            ],
        },
    )).update_layout(
        title={
            'text': max_threshold,
            'y': 0.4,
            'x': 0.450,
            'font': {'size': 22}
        },
    )
    
    col1, col2, col3 = st.columns((2,3,1))
    with col1:
        # ESG Risk Rating Definition and Risk Range 
        st.markdown('<span style="font-size: 16px;font-weight: 600;">ESG Risk Rating</span>', unsafe_allow_html=True,)
        if 'A' in reprisk_rating:
            color = '#00b050'
        elif 'B' in reprisk_rating:
            color = '#ffed00'
        elif 'C' in reprisk_rating:
            color = '#cc9b00'
        else:
            color = '#ff0000'
        st.markdown('<span style="color: '+color+'; font-size: 41px;margin-top: -26px;position: absolute; font-weight: 600;">' + reprisk_rating + '</span><br>', unsafe_allow_html=True,)
        st.text('The RepRisk Rating ranges from AAA to D')
        image = Image.open('risk calibration.png')
        st.image(image)
    with col2:
        # Displays Gauge Chart 
        st.markdown('<span style="font-size: 16px;font-weight: 600;">ESG Risk Score (RRI)</span>', unsafe_allow_html=True,)
        st.plotly_chart(max_chart, use_container_width = True)
        # Shows the reference link
        st.write("Reference: Check out the [RepRisk Methodology](https://www.reprisk.com/news-research/resources/methodology)")
    with col3:
        # RepRisk Index Ranges Definition
        st.markdown('<span style="font-size: 16px;">RepRisk Index Ranges</span>', unsafe_allow_html=True,)
        st.text('0-25 - Low Risk')
        st.text('26-49 - Medium Risk')
        st.text('50-59 - High Risk')
        st.text('60-74 - Very High Risk')
        st.text('75-100 - Extremly High Risk')
        
    # Fetches the Daily RRI Score data (Line chart)
    query =F"SELECT CURRENT_RRI, PEAK_RRI, TREND_RRI, COUNTRY_SECTOR_AVERAGE, TIME_SLOT_NAME FROM REPRISK_INFO WHERE COMPANY_NAME LIKE '%{selected_company}%' "
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    df = pd.DataFrame(results, columns=['CURRENT_RRI', 'PEAK_RRI (TTM)', 'TREND_RRI', 'COUNTRY_SECTOR_AVERAGE', 'TIME_SLOT_NAME'])

    df['Datetime'] = pd.to_datetime(df['TIME_SLOT_NAME'])
    df.set_index('Datetime', inplace=True)

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df.index, y=df['CURRENT_RRI'], mode='lines', name='CURRENT_RRI', line=dict(color='blue')))
    fig.add_trace(go.Scatter(x=df.index, y=df['PEAK_RRI (TTM)'], mode='lines', name='PEAK_RRI (TTM)', line=dict(color='red')))
    fig.add_trace(go.Scatter(x=df.index, y=df['COUNTRY_SECTOR_AVERAGE'], mode='lines', name='COUNTRY_SECTOR_AVERAGE', line=dict(color='#6ae9ef')))

    fig.update_layout(
        title='ESG Daily RRI Score Trend',
        xaxis_title='Datetime',
        yaxis_title='Value'
    )
    st.plotly_chart(fig, use_container_width=True)

    # Fetches the Reason Analysis data (Bar chart)
    reason_graph = f"SELECT count(REASON) AS CNT, REASON FROM REASON_INFORMATION WHERE COMPANY_NAME LIKE '%{selected_company}%' GROUP BY REASON"
    reason_graph_df = pd.read_sql(reason_graph, conn)
    fig = go.Figure(data=[go.Bar(x=reason_graph_df['REASON'], y=reason_graph_df['CNT'])])
    fig.update_layout(
        title="ESG Analysis by Reason",
        xaxis_title="Reasons",
        yaxis_title="Count"
    )
    st.plotly_chart(fig, use_container_width=True)

    # Image: RepRisk’s Research Scope - 28 ESG Issues
    st.markdown('<span style="font-size: 16px;font-weight: 600;">RepRisk’s Research Scope - 28 ESG Issues</span>', unsafe_allow_html=True,)
    st.markdown('<a href="https://www.reprisk.com/content/static/reprisk-esg-issues-definitions.pdf"><img src="https://www.reprisk.com/media/pages/news-research/modules/resources/methodology/1838510258-1685451754/esg-issues.png" width="1200px"></a>', unsafe_allow_html=True)

    # Image: RepRisk’s Research Scope - 74 ESG Topic Tags
    st.markdown('<span style="font-size: 16px;font-weight: 600;">RepRisk’s Research Scope - 74 ESG Topic Tags</span>', unsafe_allow_html=True,)
    st.markdown('<a href="https://www.reprisk.com/media/pages/static/958363135-1685451762/reprisk-esg-topic-tags-definitions.pdf"><img src="https://www.reprisk.com/media/pages/news-research/modules/resources/methodology/3982915039-1685451754/esg-topic-tags.png" width="1200px"></a>', unsafe_allow_html=True)

    # Shows the ESG Analysis by Reason Report Table
    reason_qry = f'''
        SELECT INCIDENT_DATE DATE, LISTAGG(REASON, ',')
            WITHIN GROUP(ORDER BY REASON) AS REASON
        FROM 
            REASON_INFORMATION 
        WHERE 
            COMPANY_NAME LIKE '%{selected_company}%'
        GROUP BY 
            INCIDENT_DATE 
        ORDER BY 
            INCIDENT_DATE ASC
    '''
    reason_df = pd.read_sql(reason_qry, conn)
    with st.expander("ESG Analysis by Reason Report", expanded=True):
        st.dataframe(reason_df, use_container_width=True)

if __name__ == "__main__":
    main()
