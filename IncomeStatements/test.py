import requests
symbol = 'ACTc2'
url = f'https://www.alphavantage.co/query?function=INCOME_STATEMENT&symbol={symbol}&apikey=BURCSC12I2PSYHNS'

r = requests.get(url)
print(r)
print(r.text)

if r.text == '{}':
    print('hi')
else:
    print('bye')

     