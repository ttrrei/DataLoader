import pandas as pd
import oracledb

class dbOperator:

    def __init__(self, dbConfig):

        self.connection = oracledb.connect(
            config_dir=dbConfig['config_dir'],
            user=dbConfig['user'],
            password=dbConfig[ 'password'],
            dsn=dbConfig['dsn'],
            wallet_location=dbConfig['wallet_location'],
            wallet_password=dbConfig['wallet_password']
        )

    def fetch_data_from_db(self, query):

        cursor = self.connection.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        ans = pd.DataFrame(result, columns=columns)
        return ans

    def write_data_to_db(self, dataFrame, tableName, columnName):
        placeholders = [f":{i}" for i in range(1, columnName.count(',') + 3)]
        values = ", ".join(placeholders)
        cursor = self.connection.cursor()
        datalist = dataFrame.values.tolist()

        cursor.executemany("insert into " + tableName + "( BATCH_ID, " + columnName + ")"
                           " values ( " + values + ")", datalist, batcherrors=True)
        cursor.connection.commit()