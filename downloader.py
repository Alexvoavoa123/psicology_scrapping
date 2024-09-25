import streamlit as st
import pandas as pd
import streamlit.components as select_slider
import base64
import time
from io import BytesIO




timestr = time.strftime("%Y%m%d-%H%M%S")

class FileDownloader(object):
    def __init__(self, data, filename = "myfile", file_ext="txt"):
        super(FileDownloader, self).__init__()
        self.data = data
        self.filename = filename
        self.file_ext = file_ext

    def download(self):
        b64 = base64.b64encode(self.data.encode()).decode()
        new_filename = f"{self.filename}_{timestr}_{self.file_ext}"
        st.markdown("#### Download file ###")
        href = f'<a href="data: file/{self.file_ext};base64,{b64}" download="{new_filename}">Click Here!!</a>'
        st.markdown(href, unsafe_allow_html=True)


    def download_xlsx(self):
        b64 = base64.b64encode(self.data).decode()
        new_filename = f"{self.filename}_{time.strftime('%Y%m%d-%H%M%S')}.{self.file_ext}"
        st.markdown("#### Download file ###")
        href = f'<a href="data: application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64}" download="{new_filename}">Click Here!!</a>'
        st.markdown(href, unsafe_allow_html=True)


    def text_downloader(self):
        b64 = base64.b64encode(self.data.encode()).decode()
        new_filename = f"new_text_file{timestr}.txt"
        st.markdown("#### Download file ###")
        href = f'<a href="data: file/text;base64,{b64}" download="{new_filename}">Click Here!!</a>'
        st.markdown(href, unsafe_allow_html=True)

    def download_json(self):
        b64 = base64.b64encode(self.data.encode()).decode()
        new_filename = f"{self.filename}_{time.strftime('%Y%m%d-%H%M%S')}.{self.file_ext}"
        st.markdown("#### Download file ###")
        href = f'<a href="data: application/json;base64,{b64}" download="{new_filename}">Click Here!!</a>'
        st.markdown(href, unsafe_allow_html=True)

        # def main():
    # menu = ["Home", "CSV", "Excel","JSON","About"]

    # choice = st.sidebar.selectbox("Menu", menu)

    # if choice == "Home":
    #     st.subheader("Home")
    #     my_text = st.text_area("Your message")
    #     if st.button("Save"):
    #         st.write(my_text)
    #         FileDownloader(my_text).text_downloader()
    #         # download = FileDownloader(my_text).download()

    # elif choice == "CSV":
    #     df = pd.read_csv(r"C:\Users\Cash\Downloads\course_materials_learnstreamlit\course_materials_learnstreamlit\LearnStreamlit\Module01\data\iris.csv")
    #     st.dataframe(df)
    #     # csv_downloader(df)
    #     download = FileDownloader(df.to_csv(), file_ext=".csv").download()

    # elif choice == "Excel":
    #     df = pd.read_csv(r"C:\Users\Cash\Downloads\course_materials_learnstreamlit\course_materials_learnstreamlit\LearnStreamlit\Module01\data\iris.csv")
    #     st.dataframe(df)
    #     towrite = BytesIO()
    #     df.to_excel(towrite, index=False, engine='openpyxl')
    #     towrite.seek(0)
    #     download = FileDownloader(towrite.read(), file_ext="xlsx").download_xlsx()

    # elif choice == "JSON":
    #     df = pd.read_csv(r"C:\Users\Cash\Downloads\course_materials_learnstreamlit\course_materials_learnstreamlit\LearnStreamlit\Module01\data\iris.csv")
    #     st.dataframe(df)
    #     json_data = df.to_json(orient='records')
    #     download = FileDownloader(json_data, file_ext="json").download_json()
    # else:
    #     st.subheader("About")

