from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.common.by import By
import pandas as pd


class extractList:

    def __init__(self):
        pass

    def get_asx_list(self, driver, random_uuid):
        code = []
        sector = []
        mcap = []
        uuid = []

        elements = (driver.find_element(By.CLASS_NAME, "scroll-overlay")
                    .find_element(By.TAG_NAME, "tbody")
                    .find_elements(By.TAG_NAME, "tr"))

        for element in elements:
            uuid.append(random_uuid)
            code.append(element.find_element(By.TAG_NAME, "a").get_attribute("innerHTML"))
            sector.append(element.find_element(By.CLASS_NAME, "text-left").get_attribute("innerHTML"))
            infos = element.find_elements(By.CLASS_NAME, "text-right")
            lengths = len(infos)
            mcap.append(infos[lengths - 1].get_attribute("innerHTML"))

        data = {
            "UUID": uuid,
            "Code": code,
            "Sector": sector,
            "Mcap": mcap
        }

        return pd.DataFrame(data)
