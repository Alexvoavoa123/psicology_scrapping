import streamlit as st
from spider import *
from downloader import FileDownloader
import streamlit.components as select_slider
import pandas
import base64
import time
from io import BytesIO
from st_on_hover_tabs import on_hover_tabs
import re

timestr = time.strftime("%Y%m%d-%H%M%S")

def scrapping_data(dataframe):
    spider = async_spider(dataframe)
   
    return spider [:15]




st.set_page_config(layout="wide")
def main():
    # values = ["Scrapping","About"]
    # st.sidebar.selectbox("Valores",values)
    st.markdown('<style>' + open('./style.css').read() + '</style>', unsafe_allow_html=True)
    st.markdown("""
                    <style>
                    .loading-container {
                        display: flex;
                        align-items: center;
                        margin-top: 20px;
                        justify-content: center;
                    }
                    .loading-container span {
                        font-size: 16px;
                        color: #333;
                        margin-right: 10px;
                    }
                    .loading {
                        width: 30px;
                        height: 30px;
                        border: 5px solid #f3f3f3;
                        border-top: 5px solid #3498db;
                        border-radius: 50%;
                        animation: spin 1s linear infinite;
                    }
                    @keyframes spin {
                        0% { transform: rotate(0deg); }
                        100% { transform: rotate(360deg); }
                    }
                    </style>
                """, unsafe_allow_html=True)

    with st.sidebar:
        tabs = on_hover_tabs(tabName=['Scrapping',"About"],
                            iconName=['dashboard', 'search'], default_choice=0,
                            styles = {'navtab': {'background-color':'#111',
                                                  'color': '#818181',
                                                  'font-size': '18px',
                                                  'transition': '.3s',
                                                  'white-space': 'nowrap',
                                                  'text-transform': 'uppercase'},
                                       'tabStyle': {':hover :hover': {'color': 'red',
                                                                      'cursor': 'pointer'}},
                                       'tabStyle' : {'list-style-type': 'none',
                                                     'margin-bottom': '30px',
                                                     'padding-left': '30px'},
                                       'iconStyle':{'position':'fixed',
                                                    'left':'7.5px',
                                                    'text-align': 'left'},
                                       },
                             key="1")

    if tabs == "Scrapping":

    
        st.title("Algorithms implementation + async web scrapping")
        dataframe_value = st.file_uploader("Upload excel file to scrappe the data")
        with st.form(key="scrapping"):
            st.markdown("**Click the button to scrape :)**")
        
            value = st.form_submit_button("Submit request")
        if value:
            if dataframe_value:

                loading_placeholder = st.empty()

                loading_placeholder.markdown("""
                    <div class="loading-container" id="loading-container">
                        <span>Extrayendo datos de administradores de fincas. Por favor espere...</span>
                        <div class="loading"></div>
                    </div>
                """, unsafe_allow_html=True)
            
            df = scrapping_data(dataframe_value)
            st.success("Successfully scrapped!")
            loading_placeholder.empty()
            st.dataframe(df)
            tab1,tab2,tab3 = st.tabs(["CSV","Excel","JSON"])

            with tab1:
                download = FileDownloader(df.to_csv(), file_ext=".csv").download()

            with tab2:
                towrite = BytesIO()
                df.to_excel(towrite, index=False, engine='openpyxl')
                towrite.seek(0)
                download = FileDownloader(towrite.read(), file_ext="xlsx").download_xlsx()

            with tab3:
                json_data = df.to_json(orient='records')
                download = FileDownloader(json_data, file_ext="json").download_json()

    else:
        st.title("About this web app")
        st.markdown("#### In this web application we web scrappe multiple psychologists web sites")
        st.markdown("#### Upload the excel file, click the button and wait for the result")
        # st.image(open_image(r"C:\Users\Cash\Proyectos\082024\parfoiç web scrapping\parfoiç_image.png"))
        st.markdown("*We will have the option to download the file in three diferrent file types of format:  excel, csv or json*")

if __name__=="__main__":
    main()
