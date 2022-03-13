import os
import dotenv
import requests
import base64
import pandas as pd
import sqlalchemy as sq
import matplotlib.pyplot as plt

dotenv.load_dotenv()

companies = ['Infosys', 'Reliance', 'HDFC_Bank', 'Asian_Paints', 'Hindustan_Unilever', 'TCS', 'ICICI', 'HCL', 'Bharti_Airtel', 'Indusind', 'SBI', 'Larsen_and_Toubro', 'Tech_Mahindra', 'ITC', 'ONGC', 'Tata_Steel', 'NTPC', 'Mahindra_and_Mahindra', 'Bajaj_Finserv', 'Titan_Company', 'Nestle_India', 'Sun_Pharmaceuticals', 'HDFC_Corporation', 'Maruti_Suzuki', 'Kotak_Mahindra']

slugs = ['INFY.NS', 'RELIANCE.NS', 'HDFCBANK.NS', 'ASIANPAINT.BO', 'HINDUNILVR.NS', 'TCS.NS', 'ICICIBANK.NS', 'HCLTECH.NS', 'BHARTIARTL.BO', 'INDUSINDBK.BO', 'SBIN.NS', 'LT.NS', 'TECHM.NS', 'ITC.NS', 'ONGC.NS', 'TATASTEEL.NS', 'NTPC.NS', 'M&M.BO', 'BAJAJFINSV.NS', 'TITAN.NS', 'NESTLEIND.NS', 'SUNPHARMA.NS', 'HDFC.NS', 'MARUTI.NS', 'KOTAKBANK.NS']

class GetData:

    def scrape(self):
        for i in slugs:
            req = requests.get(f'https://finance.yahoo.com/quote/{i}/history/', headers={
                'User-Agent': 'Thunder Client (https://www.thunderclient.io)'
            })

            df = pd.read_html(req.content)[0]
            df = df.head(20).loc[:, ['Date', 'Open', 'Close*']]
            df.rename({'Close*': 'Close'}, inplace=True, axis=1)

            try:
                df['Open'] = df['Open'].replace(',', '', regex=True).astype(float)
                df['Close'] = df['Close'].replace(',', '', regex=True).astype(float)
            except:
                continue
            df = df.iloc[::-1].reset_index(drop=True)

            df.to_sql(f"{companies[slugs.index(i)].lower()}", db, if_exists='replace', schema='stock_data', index=False)
            print('Success:', companies[slugs.index(i)])

    def companies(self):
        companies = pd.read_sql('show tables;', db)
        for index, row in companies.iloc[:-1].itertuples():
            print(f'{index+1:{2}}. {" ".join(row.split("_")).title()}')

class View:

    def __init__(self, tname):
        self.tname = "_".join(tname.lower().split())

    def view_stock(self):
        stock_details = pd.read_sql(f'select * from {self.tname}', db)
        print(stock_details)

    def view_wallet(self):
        wallet_details = pd.read_sql(f'select * from {self.tname}', db)
        print(wallet_details)
        print("Balance in wallet: ", 10000 + round(wallet_details['effect'].sum()))

    def view_graph(self):
        stock_details = pd.read_sql(f'select * from {self.tname}', db)
        plots = []
        dates = []

        for index, row in stock_details.iterrows():
            plots.extend([row['Open'], row['Close']])
            dates.extend([' '.join(row['Date'].split()[:-1])[:-1], ' '.join(row['Date'].split()[:-1])[:-1]])

        dates = dates[1:]
        dates.append('{} {}'.format(dates[-1].split()[0], int(dates[-1].split()[1])+1))

        plt.figure()
        plt.xticks(rotation=30)
        plt.xlabel("Dates")
        plt.ylabel("Stock price")
        plt.title(self.tname)
        plt.plot(dates, plots)
        plt.show()
        print(u'\u2713')

class Transact:

    def __init__(self, type, stock):
        self.type = type
        self.stock = "_".join(stock.lower().split())
    
    def transact(self):
        stock_price = db.execute(f"select Close from {self.stock}").fetchall()[-1][0]
        if self.type == 'buy':
            try:
                balance = 10000 + db.execute("select sum(effect) from wallet").fetchall()[0][0]
            except TypeError:
                balance = 10000
            if balance > stock_price:
                db.execute(f"""insert into wallet values(
                    '{self.stock}', {0-stock_price}
                );""")
                print(f'Bought {self.stock}')
            else:
                print('Insufficient balance')
        elif self.type == 'sell':
            checker_buy = db.execute(f"select * from wallet where stock_name like '{self.stock}' and effect < 0").fetchall()
            checker_sell = db.execute(f"select * from wallet where stock_name like '{self.stock}' and effect > 0").fetchall()
            checker = len(checker_buy) > len(checker_sell)
            if checker:
                db.execute(f"""insert into wallet values(
                    '{self.stock}', {stock_price}
                );""")
                print(f'Sold {self.stock}')
            else:
                print('You do not own that stock')


if __name__ == "__main__":

    db = sq.create_engine(f"mysql+mysqlconnector://{os.environ.get('DB_USER')}:{base64.b64decode(os.environ.get('DB_PASS')).decode('utf-8')}@localhost")
    db.execute("create database if not exists stock_data;")
    db.execute("use stock_data;")
    db.execute("""create table if not exists wallet(
        stock_name varchar(30),
        effect float
    );""")

    print(
        """
        Welcome to MockinV, the perfect place to get started with stock trading.\n
        You will get an initial balance of Rs. 10,000 in your wallet which you can invest in stocks on the Sensex.\n
        Type `help` to view the list of commands available.
        """
    )
    inp = input('> ')
    print('')

    while inp.lower() != 'quit':
        try:
            if inp.lower() == 'help':
                help = {
                    "help": "Get a list of all commands",
                    "get": "Retreive and update stock data",
                    "companies": "Get a list of all the available companies",
                    "view <stock-name>": "View the prices of the stock for the last 20 days",
                    "graph <stock-name>": "Graph the opening and closing prices of the stocks",
                    "view wallet": "View the transaction history and wallet balance",
                    "buy <stock-name>": "Buy the stock at the latest closing price",
                    "sell <stock-name>": "Sell the stock at the latest closing price",
                    "quit": "Exit the program"
                }
                for i in help:
                    print('{0:<20} {1:<}'.format(i, help[i]))

            if inp.lower() == 'get':
                get_data = GetData()
                get_data.scrape()
            
            elif inp.lower() == 'companies':
                companies = GetData()
                companies.companies()
            
            elif inp.lower().split()[0] == 'view':
                view = View(inp.partition(' ')[2])
                if inp.lower().split()[1] == 'wallet':
                    view.view_wallet()
                else:
                    view.view_stock()

            elif inp.lower().split()[0] == 'graph':
                view = View(inp.partition(' ')[2])
                view.view_graph()
            
            elif inp.lower().split()[0] in ('buy', 'sell'):
                transact = Transact(inp.lower().split()[0], inp.partition(' ')[2])
                transact.transact()
        except Exception as e:
            print(f'Error: {e}')
        finally:
            print('')
            inp = input('> ')
            print('')

    print('Goodbye\n')