import pandas as pd

class extractShort:


    def __init__(self):
        pass

    def get_short_list(self, url, uuid):


        df = pd.read_csv(url)
        df.insert(0, 'BATCH_ID', uuid)
        return df