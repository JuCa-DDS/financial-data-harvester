import streamlit as st
import datetime
import pandas as pd
import numpy as np 
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from data_scraper.scraper import Download_YahooFinance

class FinancialDashboardApp:
    def __init__(self):
        self.df = pd.DataFrame({'Col1':[1, 2, 3], 'Col2':[4, 5, 6]})
        self.csv = self.convert_df(self.df)
        self.scraper = Download_YahooFinance()
        self.current_report = None
        self.current_price = None

    @st.cache_data
    def convert_df(_self, df):
        return df.to_csv().encode('utf-8')
    
    def __generate_report(self, ticker):
        with st.spinner('Downloading info, please wait...'):
            try:
                if len(ticker) == 1:
                    info = self.scraper.get_info(ticker[0])
                    return info
                elif len(ticker) > 1:
                    info = self.scraper.get_info_multiple(ticker)
                    return info
            except Exception as e:
                st.error(f'Error con {ticker}: {str(e)}')
    
    def __generate_price(self, ticker, start_date, end_date):
        price = self.scraper.get_price(ticker, start_date, end_date)
        return price
    
    def render(self):
        st.title('Financial Data Harvester')

        col1, col2 = st.columns(2)
        col3, col4 = st.columns(2)
        col5, col6 = st.columns([3, 1])

        with col1:
            company = st.multiselect(
                'Select a company', 
                ['TSLA', 'AMZN', 'AAPL', 'JPM'], 
                default=['TSLA']
            )

        with col2:
            date_range = st.date_input(
                'Select Date Range for Visualization', 
                (
                    datetime.date(2025, 1, 1),
                    datetime.datetime.today(),
                ),
                    datetime.date(2020, 1, 1),
                    datetime.datetime.today()
            )

        with col3:    
            mostrar_rsi = st.checkbox('Show RSI', value=False)
            mostrar_sma = st.checkbox('Show SMA 50/200', value=False)
            mostrar_volumne = st.checkbox('Volumen', value=False)

        with col4:
            button_report = st.button('Generate Report')
            if button_report:
                self.current_report = self.__generate_report(company)
                self.current_price = self.__generate_price(company[0], date_range[0], date_range[1])

            if self.current_report is not None:
                st.download_button(
                        label='Download PDF Report',
                        data=self.csv,
                        file_name='data.csv',
                        mime='text/csv'
                )
        if len(company) == 1:
            with col5:
                if self.current_price is not None:
                    st.line_chart(
                        data=self.current_price,
                        x='Date',
                        y='Close'
                    )
            
            with col6:
                if self.current_price is not None:
                    st.metric(label='Trailing P/E', value=self.current_report['Trailing P/E'])
                    st.metric(label='Beta', value=self.current_report['Beta'])
                    st.metric(label='52 Week Change', value=self.current_report['52 Week Change'])
        
        elif len(company) > 1:
            if self.current_price is not None:
                st.line_chart(
                    data=self.current_price,
                    x='Date',
                    y='Close'
                )
                
                st.dataframe(self.current_report, hide_index=True)
 
def main():
    app = FinancialDashboardApp()
    app.render()

if __name__ == '__main__':
    main()