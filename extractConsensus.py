from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.common.by import By
import pandas as pd


class extractConsensus:

    def __init__(self):
        pass

    def get_consensus_list(self, driver, random_uuid):
        code = []
        consensus = []
        buy = []
        hold = []
        sell = []
        uuid = []

        elements = (driver.find_element(By.ID, "sticky-table")
                    .find_element(By.TAG_NAME, "tbody")
                    .find_elements(By.TAG_NAME, 'tr'))
        for element in elements:
            info = element.find_elements(By.TAG_NAME, 'td')
            uuid.append(random_uuid)
            code.append(info[0].text)
            consensus.append(info[3].text)
            buy.append(info[4].text)
            hold.append(info[5].text)
            sell.append(info[6].text)

        data = {
            "UUID": uuid,
            "Code": code,
            "Consensus": consensus,
            "Buy": buy,
            "Hold": hold,
            "Sell": sell,
        }
        return pd.DataFrame(data)
