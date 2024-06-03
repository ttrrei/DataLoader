from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.common.by import By
import pandas as pd
import gc


class extractFinance:

    def __init__(self):
        pass

    def get_financial_list(self, driver, code, random_uuid):

        data = []

        elements = driver.find_element(By.ID, "Main").find_elements(By.TAG_NAME, "tbody")

        for element in elements:
            pairs = element.find_elements(By.TAG_NAME, "tr")
            for pair in pairs:
                contents = pair.find_elements(By.TAG_NAME, "td")
                if len(contents) == 2:
                    row = [random_uuid, code,
                           contents[0].find_element(By.TAG_NAME, "span").text,
                           contents[1].text]
                    data.append(row)

        df = pd.DataFrame(data, columns=['UUID', 'CODE', 'TAG', 'INFO'])

        return df
