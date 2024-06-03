import requests
import json
import pandas as pd


class extractQuote:

    def __init__(self):
        pass

    def get_quote(self, url, code, random_uuid):
        ticker = []
        quote = []
        full_url = url + code

        try:

            response = requests.get(full_url, timeout=60)
            if response.status_code == 200:
                content = response.text
                sub_result = content.split("__APOLLO_STATE__", 1)[1][19:]
                information = sub_result.split(";</script>")[0]
                json_object = json.loads(information)

                quoteTag = f"ROOT_QUERY❖[\"financialStockSummaryQuote\"]❖{{\"symbol\":\"ASX_{code.upper()}\"}}"
                jObject = json_object.get(quoteTag, {}).get("data", {}).get("quote", {})
                quote.append([random_uuid, code, 'openPrice', jObject.get("lastPrice", ""),
                              jObject.get("providerUpdateTime", "")])
                quote.append([random_uuid, code, 'high', jObject.get("dayRange", {}).get("high", ""),
                              jObject.get("providerUpdateTime", "")])
                quote.append([random_uuid, code, 'low', jObject.get("dayRange", {}).get("low", ""),
                              jObject.get("providerUpdateTime", "")])
                quote.append([random_uuid, code, 'lastPrice', jObject.get("lastPrice", ""),
                              jObject.get("providerUpdateTime", "")])
                quote.append([random_uuid, code, 'previousClose', jObject.get("previousClose", ""),
                              jObject.get("providerUpdateTime", "")])
                quote.append(
                    [random_uuid, code, 'volume', jObject.get("volume", ""), jObject.get("providerUpdateTime", "")])
                quote.append(
                    [random_uuid, code, 'turnover', jObject.get("turnover", ""), jObject.get("providerUpdateTime", "")])
                quote.append(
                    [random_uuid, code, 'priceEarningsRatio', jObject.get("priceEarningsRatio", ""),
                     jObject.get("providerUpdateTime", "")])
                quote.append(
                    [random_uuid, code, 'outstandingShares', jObject.get("outstandingShares", ""),
                     jObject.get("providerUpdateTime", "")])
                quote.append([random_uuid, code, 'dividendYield', jObject.get("dividendYield", ""),
                              jObject.get("providerUpdateTime", "")])
                quote.append([random_uuid, code, 'marketCapitalisation', jObject.get("marketCapitalisation", ""),
                              jObject.get("providerUpdateTime", "")])

                tickerTag = f"ROOT_QUERY❖[\"financialStockHistoricalQuotes\"]❖{{\"interval\":\"FIVE_MINUTES_1_DAY\",\"symbol\":\"ASX_{code.upper()}\"}}"
                jArray = json_object.get(tickerTag).get("data").get("quotes")
                for temp_element in jArray:
                    ticker.append([random_uuid, code, temp_element["close"], temp_element["time"]])

        except Exception as e:
            print(f"Test case {code} failed with error: {e}")

        print(code, end=':    ')
        print(len(quote), end=',    ')
        print(len(ticker))
        return ticker, quote
