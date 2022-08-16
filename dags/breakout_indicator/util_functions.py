import io
import pandas as pd
import requests
from datetime import date,timedelta
from nsepy import get_history
from sqlalchemy import create_engine
import numpy as np
from scipy.optimize import minimize, LinearConstraint
import yfinance as yf
from sqlalchemy import exc
from airflow.hooks.base import BaseHook

conn = BaseHook.get_connection('postgres_local')
engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    
def read_sql(qry):
    
    conn = engine.connect()
    cursor = conn.execute(qry)
    data = pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
    conn.close()
    engine.dispose()
    
    return data


def find_grad_intercept(case, x, y):
    
    pos = np.argmax(y) if case == 'resistance' else np.argmin(y)
        
    # Form the points for the objective function
    X = x-x[pos]
    Y = y-y[pos]
    
    if case == 'resistance':
        const = LinearConstraint(
            X.reshape(-1, 1),
            Y,
            np.full(X.shape, np.inf),
        )
    else:
        const = LinearConstraint(
            X.reshape(-1, 1),
            np.full(X.shape, -np.inf),
            Y,
        )
    
    # Min the objective function with a zero starting point for the gradient
    ans = minimize(
        fun = lambda m: np.sum((m*X-Y)**2),
        x0 = [0],
        jac = lambda m: np.sum(2*X*(m*X-Y)),
        method = 'SLSQP',
        constraints = (const),
    )
    
    # Return the gradient (m) and the intercept (c)
    return ans.x[0], y[pos]-ans.x[0]*x[pos] 

def get_sym_list(ti):
    url = 'https://www1.nseindia.com/content/indices/ind_nifty100list.csv'
    s = requests.get(url).content
    index_constituents = pd.read_csv(io.StringIO(s.decode('utf-8')),on_bad_lines='skip')
    symbol_list = list((index_constituents['Symbol']).values)

    ti.xcom_push(key='symbol_list',value=symbol_list)


def update_data(ti):

    symbol_list = ti.xcom_pull(task_ids="get_symb_list",key="symbol_list")
    for sym in symbol_list:
        sql = """
                Select max("Date") from stock_ohlc where "Symbol" = '{0}'
                """.format(sym)

        max_date = read_sql(sql).values[0][0]

        if max_date == None:
            # Get historical data
            print("Inserting New data for {}".format(sym))
            data = get_history(symbol=sym,
                            start=date(1990,1,1),
                            end=date.today())
        else:
            #Update incremental data
            start_date = (max_date + timedelta(days=1))
            if start_date >= date.today():
                continue

            print("Updating New data for {0} from {1}".format(sym,start_date))      
            data = get_history(symbol=sym,
                            start=start_date,
                            end=date.today())
        
        data = data.reset_index()

        data.columns = ['Date', 'Symbol', 'Series', 'Prev Close', 'Open', 'High', 'Low', 'Last',
            'Close', 'VWAP', 'Volume', 'Turnover', 'Trades', 'Deliverable Volume',
            'Deliverble Perc']
            
        try:
            data.to_sql("stock_ohlc", engine, schema="public", if_exists='append', index=False)
        except exc.IntegrityError:
            print("Integrity error for {0} on {1}".format(sym,start_date))           

    return

def get_resistance_support(ti):

    symbol_list = ti.xcom_pull(task_ids="get_symb_list",key="symbol_list")
    resistance_candidates = []
    support_candidates = []
    for sym in symbol_list :
        print(sym)
        try:
            sql = """
                  Select "Date","Symbol","Open","High","Low","Close" from stock_ohlc \
                  where "Symbol" = '{0}'
                  """.format(sym)
            ohlc_data = read_sql(sql)
            ohlc_data['Date'] = pd.to_datetime(ohlc_data['Date'])
            ohlc_data = ohlc_data.sort_values(by='Date')
            
            ohlc_1yr = ohlc_data.tail(252).reset_index(drop=True).set_index('Date')
            ohlc_1yr = ohlc_1yr.reset_index()[['Date','Open','High','Low','Close']]
            
            m_res, c_res = find_grad_intercept(
                            'resistance', 
                            ohlc_1yr.index.values, 
                            ohlc_1yr.High.values,
                            )
            m_supp, c_supp = find_grad_intercept(
                            'support', 
                            ohlc_1yr.index.values, 
                            ohlc_1yr.Low.values,
                            )
            
            ohlc_1yr['Resistance'] =  m_res*ohlc_1yr.index + c_res
            ohlc_1yr['Support'] =  m_supp*ohlc_1yr.index + c_supp

            ohlc_1yr['Res(%)'] = round(((ohlc_1yr['Resistance'] / ohlc_1yr['Close']) -1)*100,2)
            ohlc_1yr['Sup(%)'] = round(((ohlc_1yr['Support'] / ohlc_1yr['Close']) -1)*100,2)
            
            latest_ohlc = ohlc_1yr.tail(1)

            if not latest_ohlc[np.abs(latest_ohlc['Res(%)']) <  3].empty:
                print("{} closer to resistance".format(sym))
                resistance_candidates.append(sym)

            if not latest_ohlc[np.abs(latest_ohlc['Sup(%)']) <  3].empty:
                print("{} closer to support".format(sym))
                support_candidates.append(sym)
        
        except Exception as e:
            print(sym,e)
        
        ti.xcom_push(key='resistance_list',value=resistance_candidates)
        ti.xcom_push(key='support_list',value=support_candidates)

