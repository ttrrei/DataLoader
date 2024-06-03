from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.common.by import By
import pandas as pd


class extractAnnc:

    def __init__(self):
        pass

    def get_annc_list(self, driver, random_uuid):

        code = []
        date = []
        psenstive = []
        title = []
        uuid = []

        elements = (driver.find_element(By.TAG_NAME, "announcement_data")
                    .find_element(By.TAG_NAME, "tbody")
                    .find_elements(By.TAG_NAME, 'tr'))

        for element in elements:
            info = element.find_elements(By.TAG_NAME, 'td')
            if len(info) != 0:
                uuid.append(random_uuid)
                code.append(info[0].text)
                date.append(info[1].text.replace("\n", " "))
                if info[2].text == '':
                    psenstive.append('True')
                elif info[2].text == ' ':
                    psenstive.append('False')
                else:
                    psenstive.append('Other')
                title.append(info[3].text.replace("\n", " "))

        data = {
            "UUID": uuid,
            "Code": code,
            "Date": date,
            "Psenstive": psenstive,
            "Title": title
        }

        return pd.DataFrame(data)
