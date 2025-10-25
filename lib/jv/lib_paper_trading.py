#!/usr/bin/env python
# coding: utf-8

# # Place Orders via Broker's Web API
#  _Created by J. Vallee in 2025_

# **Goal**
# 
# - Permanently scan stocks and place orders every 2 minutes during market's open hours. 
# 

# # Context
# ## Environment

# **Tools**
# - [Anaconda Cloud](https://anaconda.com/app/)
# - Jupyter Lab 4.0.9
#    - Environment "conda_trading_env" 

# **Historical trading data subscription**
# - Alpaca's "Basic" plan
#   - for backtesting and developing strategies on historical data
#   - for learning how the API works 
# - Limitations
#   - not for real-time or high-frequency trading
#   - delayed consolidated data (from all US exchanges)
#      - any data query must have an end time that is at least 15 minutes in the past.
#      - SIP (Securities Information Processor) aggregates data from all US exchanges into a single feed. 
#   - real-Time non-consolidated data (from single exchange)
#      - Investors Exchange "IEX" 
#   - 200 requests per minute

# In[1]:
#get_ipython().system(' python --version ')  # for Jupyter Notebook
import subprocess
import sys
subprocess.Popen( [ sys.executable, '--version' ], text=True ) # for Python script

# ## Folders & Files
# Required folders and files
# ||||||||
# |--|--|--|--|--|--|--|
# |./data/alpaca/ |./log/alpaca/| ./cfg/credentials.cfg |./data/optim_ema_rsi_params.csv |./lib/c/c_signal_generator.so  |./lib/jv/c_signal_generator.py|./lib/jv/wrapper_c_signal_gen.py |

# ## Libraries
# **Install**
#! pip install matplotlib alpaca-py pandas-market-calendars ta! pip install --upgrade alpaca-py typing_extensions
# In[2]:

#! pip list | findstr "pandas|alpaca|typing_extensions" # Windows
#get_ipython().system(' pip list | grep -E "pandas|alpaca|typing_extensions|^ta"  # Linux')   # for Jupyter Notebook
pip_result = subprocess.Popen(
    [ sys.executable, '-m', 'pip', 'list' ], 
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    text=True
)
grep_result = subprocess.Popen(
    [ 'grep', '-E', 'pandas|alpaca|typing_extensions|^ta' ], 
    stdin=pip_result.stdout,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    text=True
)
pip_result.stdout.close()
print ( grep_result.communicate()[0] ) 


# **Built-in**
# - [*Alpaca*](https://alpaca.markets/sdks/python/getting_started.html) : [Python API](https://alpaca.markets/sdks/python/api_reference/data/stock.html), [REST API](https://docs.alpaca.markets/reference/stockbars), [FAQ](https://forum.alpaca.markets/)
# - [*datetime*](https://docs.python.org/3/library/datetime.html),  [*strftime()*](https://pynative.com/python-datetime-format-strftime/), [Time-zones *pytz*](https://medium.com/@turkanakarimova/dates-and-times-in-python-with-datetime-and-pytz-f2bcbeaf21d8)
# - [*Pandas*](https://pandas.pydata.org/docs/reference/index.html)
# - [*pandas-market-calendars* reference](https://pandas-market-calendars.readthedocs.io/en/latest/modules.html), [Basics](https://pandas-market-calendars.readthedocs.io/en/latest/usage.html)
# 
# For future use : [*pandas-datareader*](https://pandas-datareader.readthedocs.io/en/latest/index.html), [*schedule*](https://schedule.readthedocs.io/en/stable/examples.html), [scheduled job](https://www.tutorialspoint.com/python-script-that-is-executed-every-5-minutes#:~:text=One%20simple%20approach%20to%20running,with%20a%205%2Dminute%20interval.)

# In[3]:


# Generic
import pandas as pd                       # manipulate dataframes
import pytz                               # time zones
import pandas_market_calendars as mcal    # national holidays
import inspect                            # log caller function name
import random                             # simulate price volatility
from datetime import datetime, date, time, timedelta, timezone    # handle date & time
from time import sleep                    # set timers
from time import process_time_ns, perf_counter_ns # classic chrono vs CPU's calculation time
from zoneinfo import ZoneInfo
from sys import exit as halt_program      # stop whole process

# Historical data
from alpaca.data.historical.stock import StockHistoricalDataClient
from alpaca.data.requests         import StockBarsRequest, \
                                         StockLatestTradeRequest, StockLatestQuoteRequest
from alpaca.data.enums            import DataFeed
from alpaca.data.timeframe        import TimeFrame, TimeFrameUnit

# Real-Time data
from alpaca.trading.client        import TradingClient
from alpaca.trading.requests      import MarketOrderRequest, GetOrdersRequest, GetAssetsRequest, \
                                         TakeProfitRequest, StopLossRequest
from alpaca.trading.enums         import OrderSide, OrderClass, OrderStatus, QueryOrderStatus, \
                                         TimeInForce, AssetStatus, AssetClass
# Crypto Data
from alpaca.data.historical       import CryptoHistoricalDataClient
from alpaca.data.requests         import CryptoLatestOrderbookRequest


# **Custom**

# In[ ]:


from lib.jv.lib_api_orders        import *
from lib.jv.signal_gen_ema_rsi    import generate_signal


# The _C_ - version of _signal_generator_ requires these files :
# |||
# |--|--|
# |_c_signal_generator.py_ |contains _generate_signal()_ in _Python_|
# |_c_signal_generator.c_ |contains the function in _C_|
# |_c_signal_generator.so_ |is the compiled _C_ library|
# |_wrapper_c_signal_gen.py_ |loads the _C_ library to a _Python_ environment|

# In[6]:


# C-language function
import lib.jv.wrapper_c_signal_gen as c_signal_gen  # signal_generator in C-language

# ## Constants & Parameters
# **Load configuration file**
config_file_name = './cfg/cfg_paper_trading.py'
with open( config_file_name ) as config_file : exec( config_file.read() )

#called_via_import = ( __name__ == '__main__' ) 
#unit_test_enabled = called_via_import
unit_test_enabled = False

# In[17]:

display = lambda df_in : print( df_in.to_string( index=False ) ) 

def save_log( text_in, file_suffix='' ) :
    file_timestamp = f'{current_timestamp():%Y%m%d}'
    file_path = f'{ LOG_DIR }api_orders{ file_suffix }_{ file_timestamp }.log'
    with open( file_path, 'a' ) as log_file : log_file.write( text_in )
    if len( text_in ) > 0 :
        log( f'Log messages stored in : { file_path }' )


# **Log message**
# 
# Prints timestamped caller function's name and text message 

# In[18]:


def log( message_in, caller_name='' ) :
    global daily_log, clock_delay
    if caller_name == '' : caller_name = inspect.currentframe().f_back.f_code.co_name
    message_out = f'{current_timestamp():%H:%M:%S} {caller_name:>15}():  {message_in}'
    print( message_out )
    #print( f'clock_delay={str( timedelta( seconds=abs( clock_delay ) ) )} or {clock_delay} seconds')
    daily_log += message_out + '\n' 


# **Log exception**

# In[19]:


def log_exception( ex ) :
    caller_name = inspect.currentframe().f_back.f_code.co_name
    if hasattr( ex, 'message' ) :
        log( f'{ex.message}', caller_name )
    else :        
        log( f'{ex}', caller_name )


# **Timestamp**
# 
# - get current date and time
# - advance/retard clock in Debug mode

# In[20]:


def current_timestamp() : 
    global clock_delay
    return datetime.now( pytz.utc ) + timedelta( seconds=clock_delay )


# **Check function**

# In[21]:

if unit_test_enabled :
    print( '\nUnit test of : current_timestamp()' )
    log( f'Today is {current_timestamp():%A, %B %d}' )


# ## Exchange
# ### Market Info
# - [List of exchanges](https://en.wikipedia.org/wiki/List_of_major_stock_exchanges)
# - [Abbreviations](https://pandas-market-calendars.readthedocs.io/en/latest/calendars.html) : BSE = Bombay Stock Exchange, NYSE = New York's
# - [List of time zones](https://pynative.com/list-all-timezones-in-python/)

# **Open hours**
# |Market|local time|Alpaca time (UTC)|time in France|
# |--|--|--|--|
# |New York|09h30m - 16h|13h30m - 20h|15h30m - 22h|
# |India|09h15m - 15h30m|03h45m - 10h|05h45m - 12h|

# **Define function**

# In[22]:


def get_market_info() :
    try :    
        # Get exchange's local time 
        current_time = current_timestamp()
        exchange_time_zone = pytz.timezone( CALENDAR.tz.key )
        local_time = current_time.astimezone( exchange_time_zone )
        print( f'In {CALENDAR.tz.key} {local_time:on %A %d/%m/%y, it is %H:%M:%S} local time' )
    
        # Get local open hours
        open_local, close_local = CALENDAR.open_time, CALENDAR.close_time
        print( f'On trading days, {EXCHANGE} opens from {open_local:%H:%M} to {close_local:%H:%M} local time' )
    
        # Get UTC open hours
        opening_time, closing_time = get_today_endpoints()
        if opening_time is not None :        
            print( f'\n Today {current_time:%A %d/%m/%y, it is %H:%M:%S} UTC' )
            print( f' Today, market opens from {opening_time:%H:%M} to {closing_time:%H:%M} UTC' )
            # Check if market is open now
            if ( current_time >= opening_time ) & ( current_time <= closing_time ) : 
                print( '\nMarket is OPEN now' )    
        else :
            print( 'Market does not open today' )
        
    except Exception as ex :
        print( f'Could not get open hours {current_time:on %A %d/%m/%y at %H:%M:%S UTC }' )
        log_exception( ex )        
        market_is_open = False
        #assert market_is_open, 'Market is closed.' 


# In[23]:


def get_today_endpoints() : 
    current_time = current_timestamp()
    try : 
        period = CALENDAR.schedule( start_date=current_time, end_date=current_time )
        open_dt, close_dt = period['market_open'], period['market_close']
        #return open_dt, close_dt
        return np_to_utc( open_dt ), np_to_utc( close_dt )    
    except : 
        return None, None

np_to_utc = lambda dt : datetime.fromtimestamp( int( dt.values[0] ) / 1e9 ).astimezone( pytz.utc )
# **Check function**

# In[24]:


if unit_test_enabled :
    print( '\nUnit test of : get_calendar()' )
    CALENDAR = mcal.get_calendar( EXCHANGE )
    get_market_info()


# ### Sessions
# **Last Sessions**
# 
# Define function

# In[25]:


def get_last_sessions() :
    current_time = current_timestamp()
    period_time  = timedelta( days=7 ) # 1-week period
    schedule_window = CALENDAR.schedule( start_date = current_time - period_time, 
                                         end_date   = current_time )
    return schedule_window


# Check function

# In[26]:


if unit_test_enabled :
    print( '\nUnit test of : get_last_sessions()' )
    display( get_last_sessions() )


# **Next Sessions**

# In[27]:


if unit_test_enabled :
    print( '\nUnit test of : get_next_sessions()' )
    display( get_next_sessions( CALENDAR, current_timestamp() ) )


# ### Countdown
# **Define function**

# In[28]:


# Get time, in seconds, left until target date and time
def get_seconds_to_dt( list_DD_MO_YYYY, list_HH_MM_SS ) : 
    # Set target time
    day, month, year, hour, minutes, seconds = list_DD_MO_YYYY + list_HH_MM_SS
    target_dt = datetime( year, month, day, hour, minutes, seconds ).astimezone( pytz.utc )  

    # Apply TimeZone Offset
    target_dt += timedelta( seconds=USER_ZONE_OFFSET )    
    #log( f' Target date is {target_dt:%a %d-%b}' )
    
    # Set Current time
    curr_dt = datetime.now( pytz.utc )
    # Get delta time in seconds
    delta_seconds = get_delta_seconds( target_dt, curr_dt )
    # Print result
    delta_str = str( timedelta( seconds=abs( delta_seconds ) ) )
    if target_dt > curr_dt : 
        log( f'Target time "{target_dt:%a %d-%b %H:%M:%S}" is {delta_str} or {+delta_seconds} seconds ahead' )
    else :                    
        log( f'Target time "{target_dt:%a %d-%b %H:%M:%S}" is {delta_str} or {-delta_seconds} seconds behind' )
    return delta_seconds
    
#list_date, list_time = [ 22, 9, 2025 ], [ 17, 50, 20 ] 
#get_seconds_to_dt( list_date, list_time )


# In[29]:


def get_seconds_to_opening() :
    # Initialize conditions
    opening_today, closing_today = get_last_sessions().iloc[ -1 ]
    curr_time = current_timestamp()
    opens_later_today = ( curr_time < opening_today )
    is_open_now = ( opening_today < curr_time < closing_today )

    # Get debug delay to advance clock
    if opens_later_today : 
        opening = opening_today.to_pydatetime()
        log('Market opens_later_today')
    elif is_open_now :     
        opening = curr_time   
        log('Market is_open_now')
    else : # market opens in a future day  
        opening, _ = get_next_sessions( CALENDAR, curr_time ).iloc[ 0 ]
        log('Market opens in a future day')
    open_datetime = opening.timetuple()
    open_date, open_time = open_datetime[:3], open_datetime[3:6]
    list_date, list_time = list( reversed( open_date ) ), list( open_time )
    time_to_opening = get_seconds_to_dt( list_date, list_time )
    #log(f'time_to_opening={str( timedelta( seconds=time_to_opening ) )} or {time_to_opening} sec.')
    return time_to_opening


# **Check function**

# In[30]:


if unit_test_enabled :
    print( '\nUnit test of : get_seconds_to_opening()' )
    get_seconds_to_opening() 


# ### Trading account

# **Define function**

# In[31]:


def get_trading_client() :
    api_key, api_secret = get_credentials( CREDENTIALS_PATH )    
    try :
        # Get client instance for paper trading
        TRADING_CLIENT = TradingClient( api_key, api_secret, paper=True )
        # Get account info
        account = TRADING_CLIENT.get_account()    
        log( f'Account Status: {account.status}' )    
        # Get available capital
        log( f'{get_balance( TRADING_CLIENT )}' )
        return TRADING_CLIENT
    except Exception as ex :
        log( f'***** ERROR : Could not instantiate TradingClient' )
        log_exception( ex )
        halt_program()        
        return TRADING_CLIENT


# **Check function**

# In[32]:


if unit_test_enabled :
    print( '\nUnit test of : get_trading_client()' )
    TRADING_CLIENT = get_trading_client()


# **Available capital**

# In[33]:


if unit_test_enabled :
    print( '\nUnit test of : get_balance()' )
    print( get_balance( TRADING_CLIENT ) )


# ## Data
# Goal : Concatenate historical and real-time data

# #### Client
# Get client instance for historical data
# 
# **Define function**

# In[34]:


def get_data_client() :
    api_key, api_secret = get_credentials( CREDENTIALS_PATH )  
    try :
        DATA_CLIENT = StockHistoricalDataClient( api_key, api_secret )
        return DATA_CLIENT
    except Exception as ex :
        log( f'***** ERROR : Could not instantiate Historical Data Client\n{ex.message}' )
        halt_program()


# **Check function**

# In[35]:


if unit_test_enabled :
    print( '\nUnit test of : get_data_client()' )
    DATA_CLIENT = get_data_client()


# ### Historical 
# Historical data

# Cf. Alpaca API : [Historical bars](https://docs.alpaca.markets/reference/stockbars#:~:text=adjustment,for%20the%20stocks.)

# **Define function**

# In[36]:


def get_historical_data( verbose=False) :
    # Define time window
    window_start, window_end = get_time_window()

    # Create request
    request = StockBarsRequest( symbol_or_symbols=TICKERS, start=window_start, end=window_end,
                timeframe=TimeFrame( HIST_INTERVAL, TimeFrameUnit.Minute ) )  # Minute Hour Day

    # Submit request & get Pandas
    try :
        data = DATA_CLIENT.get_stock_bars(request).df
    except Exception as ex :
        log( f'***** ERROR : Could not get historical data' )
        log_exception( ex )
        halt_program()
    #print( f'{ len( data ) } records fetched at {current_timestamp().time():%Hh%M} (UTC)' ) 

    # Extract Close prices & set time as index 
    data = data[[ 'close' ]].reset_index( level=0 )  
    #print( f'\t last record timestamp : {data.index.max().time():%Hh%M}' )

    # Filter in open-market bars
    last_sessions = get_last_sessions()
    opening_time, closing_time = last_sessions.iloc[ -1 ]
    data = data.between_time( opening_time.time(), closing_time.time() )
    #print( f'\t {len( data )} records during market hours' )

    # Pivot Panda's column 'symbol' to get 1 column per ticker
    data = data.pivot( columns='symbol', values='close' )
    
    # Filter in N most recent bars
    data = data[ -WINDOW_SIZE: ] 
            
    # Log data info
    dates = data.index
    date_first, date_last = f'{dates.min():%B %d %Hh%M}', f'{dates.max():%B %d %Hh%M}'
    log( f'{WINDOW_SIZE:>4} most recent records from {date_first} to {date_last}' )

    # Display data
    if verbose :
        print( 'History sample (10 first tickers) :' )
        display( data.iloc[ :, :10 ].round( 2 ) )
        
    return data


# In[37]:


def get_time_window( nb_days=5 ) :
    window_end   = current_timestamp() - timedelta( minutes=HIST_MIN_DELAY )
    window_size  = timedelta( days=nb_days ) # N-day window > 3-day long weekends
    window_start = window_end - window_size        
    #print( 'Target period :' )
    #print( f'\t from {window_start:%B %d %Hh%M} \n\t to   {window_end:%B %d %Hh%M}' )
    return window_start, window_end


# **Check function**

# Clock is changed to different times (including close hours) for this test

# In[38]:


if unit_test_enabled :
    print( '\nUnit test of : get_historical_data()' )
    clock_delay = get_seconds_to_dt( get_last_weekday( 'Friday' ), [ 14, 1, 59 ] )
    historical_data = get_historical_data( verbose=True )
    #print( str(data.round(2) ).replace('+00:00','') )
    clock_delay = 0 # back to normal clock time


# **Plot data**
# 
# Plot chart of prices' variation

# In[39]:


if unit_test_enabled :
    print( '\nUnit test of : plot_variation_prices()' )
    # Chart shows weekend's inactivity 
    plot_variation_prices( historical_data.iloc[ :, :5 ], nb_last_records=100, hide_closed_hours=False )  
    # Chart hides idle periods
    plot_variation_prices( historical_data.iloc[ :, :5 ], nb_last_records=100 )  


# ### Real-Time
# Real-Time data
# 
# Goal : Get latest trading price for all tickers to complete the historical data

# **Define function**

# In[40]:


# Normal waiting time 
def normal_wait_until_next_run() :
    #log( 'started' )
    seconds_before_interval_ends = get_seconds_before_next_run()
    sleep( seconds_before_interval_ends )

def normal_wait_until( open_dt ) :
    #log( 'started' )
    nb_secs_before_open_dt  = ( open_dt - current_timestamp() ).seconds     
    nb_secs_before_open_str = str( timedelta( seconds=nb_secs_before_open_dt ) )
    log( f'Idle until {open_dt:%H:%M:%S} in {nb_secs_before_open_str} or {nb_secs_before_open_dt} sec.' )
    sleep( nb_secs_before_open_dt + 1 )
    
# Reduced waiting time for simulation
def simul_wait_until_next_run() :
    global clock_delay
    #log( 'started' )
    # Advance clock of N seconds
    delay_to_next_run =  get_seconds_before_next_run()
    # no offset for 0-delay before next run 
    if delay_to_next_run > 0 : clock_delay += delay_to_next_run - 1   
    
    #log( f'Time changed by {clock_delay} seconds' )
    # Call normal function 
    normal_wait_until_next_run()

def simul_wait_until( open_dt ) :
    global clock_delay
    #log( 'started' )
    # Advance clock of N seconds
    clock_delay = get_seconds_to_opening() - 1
    clock_delay_str = str( timedelta( seconds=clock_delay ) )
    log( f'Clock advanced by {clock_delay_str} or {clock_delay} seconds' )
    # Call normal function 
    normal_wait_until( open_dt )

# Choose function's definition
def get_prototypes( execution_mode ) :   # Choose normal or simulation functions
    if execution_mode == 'accelerated' : 
        return simul_wait_until, simul_wait_until_next_run, simul_get_ltps, simul_place_order
    else : 
        return normal_wait_until, normal_wait_until_next_run, normal_get_ltps, normal_place_order


# In[41]:


def normal_get_ltps() : 
    # Wait for next interval
    #wait_until_next_run( INTERVAL, clock_delay )
    
    # Get next date index: remove fraction of seconds
    ltp_time = current_timestamp().replace( microsecond=0 )
    
    # Create request
    request_params = StockLatestTradeRequest( symbol_or_symbols=TICKERS )

    # Submit request
    trades = DATA_CLIENT.get_stock_latest_trade( request_params=request_params ) 

    # Convert dictionary of trades to Pandas
    dict_trades = [ dict( trade ) for trade in trades.values() ]
    df_trades = pd.DataFrame( dict_trades )
    df_trades[ 'timestamp' ] = ltp_time  # set unique index for all tickers
    df_ltps = df_trades.pivot( index='timestamp', columns='symbol', values='price' )
    #log( f'{ltps.iloc[0].values}' ) # print ltp values
    str_ltps = [ f'{i:#.1f}' for i in df_ltps.iloc[ 0 ][ :10 ] ]
    log( f'{" ".join( str_ltps )} ...' )

    return df_ltps

# Random variation of prices to simulate volatility
def simul_get_ltps() :     
    MAX_PCT_DELTA = 6    # max price variation
    df_ltps = normal_get_ltps()
    
    random_factor = 1 + random.randint( -MAX_PCT_DELTA, +MAX_PCT_DELTA ) / 100 
    df_ltps = df_ltps * random_factor
    
    str_ltps = [ f'{i:#.1f}' for i in df_ltps.iloc[ 0 ][ :10 ] ]
    log( f'{" ".join( str_ltps )} ... (random)' )

    return df_ltps
        
get_ltps = normal_get_ltps


# In[42]:


def get_seconds_before_next_run() :
    current_time = current_timestamp()
    current_seconds = 60 * current_time.minute + current_time.second
    seconds_after_interval_started = current_seconds % INTERVAL 
    #log(f'{seconds_after_interval_started:#4d} seconds after current interval started')
    if seconds_after_interval_started > 0 :
        seconds_before_interval_ends = INTERVAL - seconds_after_interval_started        
    else : 
        seconds_before_interval_ends = 0
    #log( f'{seconds_before_interval_ends:#4d} seconds before next run' )
    return seconds_before_interval_ends 


# **Check function**

# In[43]:


if unit_test_enabled :
    print( '\nUnit test of : simul_get_ltps()' )
    ltps = simul_get_ltps()
    print(ltps)


# ### Generated
# **Save Results**
# 
# Daily files with log messages, historical data & performance records

# In[44]:


def save_results( daily_log, daily_history, daily_chrono, suffix='' ) :
    try : 
        # Daily historical data
        save_df_to_csv( f'hist{ suffix }_', daily_history, 'Historical Data' )
        # Chronometer records for performance measures
        chrono_results = get_chrono_results( daily_chrono )
        save_df_to_csv( f'chrono_ms{ suffix }_', chrono_results, 'Chronometer Records in ms' )
        # Daily updated orders
        daily_orders = get_daily_orders( current_timestamp().date() )
        save_df_to_csv( f'orders{ suffix }_', daily_orders, 'Orders Summary' )
    finally :
        # Log execution
        save_log( daily_log, suffix )    


# ## Orders
# **Bracket Orders**
# 
# This type of order is a single order request that implies 3 : &nbsp; entry &nbsp; + &nbsp; target (profit-taking) &nbsp; + &nbsp; stop-loss (loss-limiting)
# 
# **Reference** : [Types](https://docs.alpaca.markets/docs/orders-at-alpaca#order-types) &emsp; [Lifecyle status & diagram](https://docs.alpaca.markets/docs/orders-at-alpaca#order-lifecycle) &emsp; [Fields of order object](https://docs.alpaca.markets/docs/brokerapi-trading#order-properties) &emsp; [Creation parameters](https://docs.alpaca.markets/reference/createorderforaccount) &emsp; [Validity - Time in force](https://docs.alpaca.markets/docs/orders-at-alpaca#time-in-force) &emsp; [Example](https://forum.alpaca.markets/t/bracket-order-code-example-with-alpaca-py-library/12110)

# ### Place
# Place an order

# **Define function**

# In[45]:


def normal_place_order( current_price, ticker, quantity, signal ) :

    MIN_OFFSET = 0.015   # Minimum delta compared to base price ~= LTP 
    target   = round( ( 1 + signal * TARGET_PCT   / 100) * current_price, 2 )
    stoploss = round( ( 1 - signal * STOPLOSS_PCT / 100) * current_price - MIN_OFFSET, 2 ) 
    log( f'{ticker}: [target (take_profit.limit_price),  stoploss] = [{target}, {stoploss}]' )

    # Get position
    if signal == +1 :
        position = OrderSide.BUY  
    else : # Case signal == -1
        position = OrderSide.SELL
    
    # Set request parameters
    order_request = MarketOrderRequest(
        symbol        = ticker,
        qty           = quantity,
        side          = position,
        time_in_force = TimeInForce.GTC,  # Good unTil Cancelled != DAY that expires at close time
        order_class   = OrderClass.BRACKET,
        take_profit   = TakeProfitRequest( limit_price=target ),
        stop_loss     = StopLossRequest( stop_price=stoploss ),
    )    
    # Submit order request
    try : 
        submitted_order = TRADING_CLIENT.submit_order( order_data=order_request )
        log( f'{ticker}: submitted order \t ID = {submitted_order.id}' + 5*('=') )
        return submitted_order        
    except Exception as ex :
        log( f'{ticker}: ***** ERROR : Could not place order\n{eval( str( ex ) )}' )
        return None

wait_until, wait_until_next_run, get_ltps, place_order = get_prototypes( 'normal' )


# In[46]:


# Random variation of prices to simulate volatility
def simul_place_order( curr_price, ticker, quantity, signal ) :

    MIN_OFFSET = 0.015   # Minimum delta compared to base price ~= LTP 
    target   = round( ( 1 + signal * TARGET_PCT   / 100) * curr_price, 2 )
    stoploss = round( ( 1 - signal * STOPLOSS_PCT / 100) * curr_price - MIN_OFFSET, 2 ) 
    log( f'{ticker}: [target (take_profit.limit_price),  stoploss] = [{target}, {stoploss}]' )

    # Get position
    if signal == +1 :
        position = OrderSide.BUY  
    else : # Case signal == -1
        position = OrderSide.SELL
    
    # Set request parameters
    order_request = MarketOrderRequest(
        symbol        = ticker,
        qty           = quantity,
        side          = position,
        time_in_force = TimeInForce.GTC,  # Good unTil Cancelled != DAY that expires at close time
        order_class   = OrderClass.BRACKET,
        take_profit   = TakeProfitRequest( limit_price=target ),
        stop_loss     = StopLossRequest( stop_price=stoploss ),
    )    
    # Submit order request
    try : 
        submitted_order = TRADING_CLIENT.submit_order( order_data=order_request )
        log( f'{ticker}: submitted order \t ID = {submitted_order.id}' + 5*('=') )
        return submitted_order        
    except Exception as ex :
        log( f'{ticker}: ***** ERROR : Could not place order' )
        # 2nd try to place order using base price
        dict_ex = eval( str( ex ) )
        log( f'{ticker}: {dict_ex}' )
        if ( 'base_price' in dict_ex.keys() ) :
            base_price = float( dict_ex[ 'base_price' ] )
            log( f'{ticker}: 2nd try to place order using base price' )
            random_factor = 1 - signal * random.randint( 2, 4 ) / 100 
            curr_price    = base_price * random_factor

            submitted_order = normal_place_order( curr_price, ticker, quantity, signal )
            return submitted_order        
        else :
            return None

single_order_to_df = lambda order : pd.DataFrame( dict( order ), index=[''] )
# **Check function**

# In[47]:


if unit_test_enabled :
    print( '\nUnit test of : get_ltps()' )
    ticker       = 'BRK.B' #'WOLF' # Wolfspeed Inc. LTP = $2.33 
    signal       = +1
    ticker_ltps  = get_ltps()[ ticker ].values[0]
    #submitted_order = place_order( ticker_ltps, ticker, QUANTITY, signal )


# ### List
# #### By status
# List orders by status 
# 
# - status for executed orders : [ 'filled', 'canceled', 'rejected', 'expired' ]
# - status for pending execution : [ 'accepted', 'new', 'pending new', 'held', 'partially filled' ]
# - max list size is 500 (according to Alpaca's API)
# 
# **Define function**

# In[48]:


def get_orders_by_status( status='all', verbose=True ) : # all pending canceled filled
    # Set target status
    if status == 'pending' : 
        order_statuses = [ 'accepted', 'new', 'held', 'partially filled' ]
    else : 
        order_statuses = [ status ]
    
    # Submit request to list all Ordersget_orders_by_status
    filter_all = GetOrdersRequest( status=QueryOrderStatus.ALL, limit=500 )
    all_orders_unformatted = TRADING_CLIENT.get_orders( filter=filter_all )

    try :
        # Convert Orders list to 1 Pandas
        all_orders = list_of_dicts_to_df( all_orders_unformatted ).set_index( 'id' )
        
        # Select Pandas' columns
        all_orders = all_orders[[ 'symbol', 'type', 'status', 'position_intent', 'limit_price', 'stop_price', 
                          'qty', 'created_at', 'updated_at', 'expires_at', 'canceled_at', 'time_in_force' ]]
        # Remove redundant key prefixes
        all_orders = remove_prefixes( all_orders)
    
        # Filter orders by status
        if status == 'all' :         
            # Count orders per status
            if verbose :
                orders_counts = all_orders[ 'status' ].value_counts()
                display( pd.DataFrame( orders_counts ) )
            return all_orders
        else :
            subset_orders = all_orders[ all_orders[ 'status' ].isin( order_statuses ) ]            
            subset_orders = remove_prefixes( subset_orders )
            return subset_orders
            
    except Exception as ex :
        print( f'***** ERROR : Could not get a list of orders as a Pandas' )
        log_exception( ex )
        return None    


# In[49]:


def remove_prefixes( orders ) :
    orders.loc[ :, 'type' ]            = orders[ 'type' ]           .str.replace( 'OrderType', '' )
    orders.loc[ :, 'status' ]          = orders[ 'status' ]         .str.replace( 'OrderStatus', '' )
    orders.loc[ :, 'position_intent' ] = orders[ 'position_intent' ].str.replace( 'PositionIntent', '' )
    orders.loc[ :, 'time_in_force' ]   = orders[ 'time_in_force' ]  .str.replace( 'TimeInForce', '' )    
    return orders


# **Check function**

# In[50]:


if unit_test_enabled :
    print( '\nUnit test of : get_orders_by_status( all )' )
    all_orders = get_orders_by_status( 'all' ) # all pending canceled filled
    display( all_orders.head() )


# In[51]:


if unit_test_enabled :
    print( '\nUnit test of : get_orders_by_status( pending )' )
    pending_orders = get_orders_by_status( 'pending', verbose=True ) # all pending canceled filled
    display( pending_orders.head() )


# #### Daily Orders

# **Define function**

# In[52]:


def get_daily_orders( date_in ) :
    all_orders   = get_orders_by_status( 'all', verbose=False )
    daily_mask   = ( all_orders[ 'created_at' ].apply( datetime.date ) == date_in )
    return all_orders [ daily_mask ]


# **Check function**
# 
# Save to file

# In[53]:


def save_df_to_csv( file_prefix, df_in, text_in='Pandas', timestamp=None ) :
    if len( df_in ) > 0 :
        if timestamp is None :
            timestamp = current_timestamp()            
        file_suffix = f'{timestamp:%Y%m%d}'
        file_path = CSV_DIR + file_prefix + file_suffix + '.csv'
        df_in.to_csv( file_path )
        log( f'{text_in} stored in : {file_path}' )
    else : 
        log( f'No file created for empty {text_in}' )


# In[54]:


if unit_test_enabled :
    print( '\nUnit test of : save_df_to_csv()' ) 
    date_to_check = datetime.today().date() - timedelta( days=1 )
    daily_orders = get_daily_orders( date_to_check )
    save_df_to_csv( 'orders_', daily_orders, 'Orders Summary', date_to_check )
    display( daily_orders.head() )    


# Read from file

# In[57]:


def read_df_from_csv( csv_path, index_column=None, timestamp_index=False ) :
    try :
        if index_column is None :
            df_out = pd.read_csv( csv_path )
        elif timestamp_index == False :
            #df_out = df_out.set_index( index_column )
            df_out = pd.read_csv( csv_path, index_col=index_column )
        else : 
            df_out = pd.read_csv( csv_path, index_col=index_column,  parse_dates=True )
        return df_out
    except Exception as ex :
        log( f'***** ERROR : Could not read Pandas from {csv_path}' )
        log_exception( ex )
        return pd.DataFrame()


# In[58]:


if unit_test_enabled :
    print( '\nUnit test of : read_df_from_csv()' ) 
    daily_orders_path = f'{CSV_DIR}orders_{date_to_check:%Y%m%d}.csv'
    daily_orders = read_df_from_csv( daily_orders_path, index_column='id' )
    display( daily_orders.head() )    


# Count daily orders' status by type and position intent

# In[60]:


if unit_test_enabled :
    print( '\nUnit test of : daily_orders' ) 
    try : 
        orders_count = daily_orders[[ 'position_intent', 'type', 'status' ]].value_counts()
        display( pd.DataFrame( orders_count ).sort_values( by=[ 'position_intent', 'type' ] ) )
    except : pass    


# ### Get info
# #### By id
# **Define function**

# In[61]:


def check_bracket( order_id ) :
    order = TRADING_CLIENT.get_order_by_id( order_id )
    # convert to pandas
    selected_cols = [ 'symbol', 'type', 'status', 'position_intent', 'limit_price', 'stop_price', 
                          'qty', 'created_at', 'expires_at', 'time_in_force' ]
    o_dict = dict( order )
    if o_dict['legs'] is None :
        o_pandas = pd.DataFrame( o_dict, index=[ 'id' ] )
    else : 
        o_pandas = pd.DataFrame( o_dict ).set_index( 'id' )

    has_child_orders = ( o_pandas[ 'legs' ].iloc[0] != None )
    if has_child_orders :
        o_legs = o_pandas[ 'legs' ] 
        # print( 'This is a bracket order linked to 1 target order & 1 stoploss order' )
    o_pandas = o_pandas[ selected_cols ].drop_duplicates() 

    if has_child_orders :
        list_of_dicts = []
        for order in o_legs :
            order_dict = dict( order )
            list_of_dicts.append( order_dict )
        leg_pandas = pd.DataFrame( list_of_dicts ).set_index( 'id' )
        leg_pandas = leg_pandas[ selected_cols ] 
        
        o_pandas = pd.concat( [ o_pandas, leg_pandas ] )
        o_pandas = remove_prefixes( o_pandas )
        
    return o_pandas


# **Check function**

# In[62]:


if unit_test_enabled :
    print( '\nUnit test of : save_df_to_csv()' )
    display( check_bracket( 'cd6dcf94-b936-4577-befa-da305d9514e8' ) )


# #### Last bracket
# **Define function**

# In[63]:


def check_last_bracket() :
    all_orders = get_orders_by_status( 'all', verbose=False )
    bracket_orders = all_orders[ all_orders[ 'type' ]=='market' ]
    nb_bracket_orders = len( bracket_orders )
    if  nb_bracket_orders == 0 : 
        print( 'List of bracket orders is empty' )  
    else :
        last_bracket_order_id = bracket_orders.index[0]
        return check_bracket( last_bracket_order_id ) 


# **Check function**

# In[64]:


if unit_test_enabled :
    print( '\nUnit test of : check_last_bracket()' )
    display( check_last_bracket() )


# ### Cancel
# #### By id
# **Define function**

# In[65]:


def cancel_order( order_id ) :
    # Check for bracket orders that may be canceled
        nb_pending_bracket_orders, _ = list_pending_bracket_orders()
        if nb_pending_bracket_orders > 0 :         
            try : 
                # Submit cancel request
                TRADING_CLIENT.cancel_order_by_id( order_id )
                # Get canceled order info
                order_info = TRADING_CLIENT.get_order_by_id( order_id )   
                sleep( 1 ) # wait until request is processed
                nb_pending_bracket_orders_after, _ = list_pending_bracket_orders()
                nb_canceled_orders = nb_pending_bracket_orders - nb_pending_bracket_orders_after
                if nb_canceled_orders == 1 :
                    print( f'order {order_id} was canceled' )
                else :
                    print( f'***** ERROR: order {order_id} could not be canceled' )
                    print( f'For more info run : check_bracket( "{order_id}" )' )
                return order_info
            except Exception as ex :
                log( f'***** ERROR : Could not cancel order {order_id}' )
                log_exception( ex )


# In[66]:


def list_pending_bracket_orders( verbose=False ) :    
    pending_orders = get_orders_by_status( 'pending', verbose=False ) 
    nb_bracket_orders = len( pending_orders )
    if nb_bracket_orders == 0 : 
        if verbose : print( 'List of orders in pending state is empty' )   
        return nb_bracket_orders, None
    else :
        pending_bracket_orders = pending_orders[ pending_orders[ 'type' ]=='market' ]
        nb_pending_bracket_orders = len( pending_bracket_orders )
        if verbose : 
            if  nb_pending_bracket_orders == 0 :         
                #display( pending_orders )
                print( 'List of bracket orders in pending state is empty' )        
            else :
                print( 'Pending bracket orders' )
                selected_cols = [ 'symbol', 'type', 'status', 'position_intent',
                                  'limit_price', 'stop_price', 'qty', 'canceled_at' ]
                display( pending_bracket_orders[ selected_cols ] ) 
        return nb_pending_bracket_orders, pending_bracket_orders


# **Check function**

# In[68]:


if unit_test_enabled :
    print( '\nUnit test of : cancel_order()' )
    canceled_order = cancel_order( '3913be09-d94c-4a76-a144-41135bd93927' )


# #### Last pending order
# **Define function**

# In[69]:


def cancel_last_pending_order() :
    nb_orders, orders = list_pending_bracket_orders( verbose=True )
    if nb_orders > 0 :
        last_order_id = orders.index[0]
        cancel_order( last_order_id ) 


# **Check function**

# In[70]:


if unit_test_enabled :
    print( '\nUnit test of : cancel_last_pending_order()' )
    #cancel_last_pending_order()
    pass


# #### All pending orders
# **Define function**

# In[71]:


def cancel_pending_brackets() :
    nb_orders, orders = list_pending_bracket_orders( verbose=True )
    while True : 
        if nb_orders == 0 :
            break
        last_order_id = orders.index[0]
        cancel_order( last_order_id ) 
        nb_orders, orders = list_pending_bracket_orders( verbose=False )


# **Check function**

# In[72]:


if unit_test_enabled :
    print( '\nUnit test of : cancel_pending_brackets()' )
    cancel_pending_brackets()
    pass


# ### Order Book
# In Alpaca Free Tier, 
# - only last exchange's quote is available.
# - an N-depth quote is available for **crypto-currencies** only

# #### Last exchange's quote
# **Define function**

# In[73]:


def get_last_quote() :
    # Set request
    request_params = StockLatestQuoteRequest(
        symbol_or_symbols=TICKERS,
        feed=DataFeed.IEX # from the IEX feed
    )    
    try:
        # Submit request
        dict_of_quotes = DATA_CLIENT.get_stock_latest_quote(request_params)

        # Convert response to pandas
        dict_of_dicts = { key: dict(quote) for key, quote in dict_of_quotes.items() }
        df_out = pd.DataFrame.from_dict( dict_of_dicts, orient='index' )
        selected_cols = [ 'bid_price', 'bid_size', 'ask_price', 'ask_size', 'timestamp' ]
        df_out = df_out[ selected_cols ].astype( { 'bid_size':int, 'ask_size':int } )
        df_out[ 'timestamp' ] = df_out[ 'timestamp' ] \
                                                .apply( lambda x: x.strftime( '%d/%m/%y-%Hh%Mm%Ss' ) )
        return df_out

    except Exception as ex :
        print( f'***** ERROR : Could not get last quote\n{ex}' )
        return None  


# **Check function**

# In[74]:


if unit_test_enabled :
    print( '\nUnit test of : get_last_quote()' )
    display( get_last_quote().head() )


# #### 5-depth quote
# - Get latest snapshot of order book
# - Available only for Crypto-assets
# 
# **Define function**

# In[75]:


def get_5_depth_quote() :
    depth = 5
    # Create request
    request_params = CryptoLatestOrderbookRequest( symbol_or_symbols=CRYPTO_TICKERS )
    try:
        # Submit request
        all_tickers_quotes = CRYPTO_DATA_CLIENT.get_crypto_latest_orderbook( request_params )    
        # Convert dictionary of quotes to pandas
        if len( all_tickers_quotes ) > 0 : 
            tickers_quotes = pd.DataFrame()
            for ticker in CRYPTO_TICKERS :
                # Convert dictionary of quotes to pandas
                one_ticker_dict = all_tickers_quotes[ ticker ]
                one_ticker_bids = pd.DataFrame( one_ticker_dict.model_dump()[ 'bids' ] )[ :depth ]
                one_ticker_asks = pd.DataFrame( one_ticker_dict.model_dump()[ 'asks' ] )[ :depth ]
                one_ticker_quotes = pd.concat( [ one_ticker_bids, one_ticker_asks ], axis=1 ) 
                new_headers = [ 'bid_price', 'bid_size', 'ask_price', 'ask_size' ]
                one_ticker_quotes.columns = new_headers
                one_ticker_quotes[ 'ticker' ] = ticker
                one_ticker_quotes = one_ticker_quotes[ [ 'ticker' ] + new_headers ]
                one_ticker_quotes[ 'timestamp' ] = one_ticker_dict.timestamp     
                # Concatenate ticker pandas 1 by 1
                tickers_quotes = pd.concat( [ tickers_quotes, one_ticker_quotes ], ignore_index=True )        
            return tickers_quotes    
    except Exception as ex :
        log( f'***** ERROR : Could not get 5-depth quote\n{ex}' )
        return None  


# **Check function**

# In[76]:


if unit_test_enabled :
    print( '\nUnit test of : get_5_depth_quote()' )
    CRYPTO_TICKERS = [ 'BTC/USD', 'ETH/USD' ]
    CRYPTO_DATA_CLIENT = CryptoHistoricalDataClient()
    display( get_5_depth_quote() )


# ### Open Positions
# #### List all

# In[77]:


if unit_test_enabled :
    print( '\nUnit test of : get_all_positions()' )
    positions = TRADING_CLIENT.get_all_positions()
    display( list_of_dicts_to_df( positions ) )


# #### Liquidate
# Close all open positions
# 
# **Define function**

# In[78]:


def liquidate_all_open_positions() :
    positions = TRADING_CLIENT.get_all_positions()       # Get all open positions
    nb_positions = len( positions )
    print( f'{nb_positions} positions to liquidate' )
    if nb_positions > 0 :
        # Submit liquidation 
        responses = TRADING_CLIENT.close_all_positions(  # Close all open positions 
                        cancel_orders=True )             # Cancel all open orders first
        if len( responses ) > 0 :
            # Convert responses to Pandas with columns id & status
            list_tuples = [ [ resp.body.id, resp.status ] for resp in responses ]
            df_tuples = pd.DataFrame( list_tuples, columns=[ 'id', 'status'] ).set_index( 'id' )
            # Get Pandas with info of liquidated orders
            all_orders = get_orders_by_status( 'all', verbose=False ) 
            selected_orders = all_orders[ all_orders.index.isin( df_tuples.index ) ]
            # Concatenate columns of both Pandas 
            selected_orders = pd.concat( [ df_tuples, selected_orders ], axis=1 )
            display( selected_orders )    


# **Check function**

# In[79]:


if unit_test_enabled :
    print( '\nUnit test of : liquidate_all_open_positions()' )
    #liquidate_all_open_positions()
    pass


# ## Pre-Process

# In[81]:


def get_strategy_params() :
    # Filter columns
    selected_cols = [ 'date', 'ticker', 'metric_type', 'slow_window', 'fast_window', 'long_entry', \
                      'short_entry', 'opt_params' ]
    parameters = pd.read_csv( STRATEGY_PARAMS_PATH, converters={ 'opt_params':eval } )[ selected_cols ]
    last_update = parameters[ 'date' ][ -1: ].values[ 0 ]
    #print( 'Last update on optimized parameters :', last_update )
    
    # Filter rows
    date_and_metric_masks = ( parameters[ 'date' ]==last_update ) & ( parameters[ 'metric_type' ]=='delta ratio' )
    parameters = parameters[ date_and_metric_masks ].drop( columns=[ 'date', 'metric_type' ] )
    parameters[ 'ticker' ] = parameters[ 'ticker' ].str.replace( '-', '.' ) # Adapt tickers to Alpaca standard 
    # Todo adapt code with index = ticker
    #parameters[ selected_cols ].set_index( 'ticker' )
    return parameters

get_strategy_params().round( 2 ).head(13)


# ## Daily Process

# In[83]:

def get_chrono_results( chrono_list_of_records ) : # input in nano-s & output in mili-s
    chrono = pd.DataFrame( chrono_list_of_records, columns=CHRONO_COLS ).set_index( 'ticker' )
    chrono[ 'delta_signal' ]     = chrono[ 'signal_generated_time' ] - chrono[ 'ltp_received_time' ]
    chrono[ 'delta_signal_cpu' ] = chrono[ 'signal_generated_cpu' ]  - chrono[ 'ltp_received_cpu' ]
    chrono[ 'delta_order' ]      = chrono[ 'order_submitted_time' ]  - chrono[ 'ltp_received_time' ]
    chrono[ 'delta_order_cpu' ]  = chrono[ 'order_submitted_cpu' ]   - chrono[ 'ltp_received_cpu' ]
    chrono[ chrono.columns ] = ( chrono[ chrono.columns ] / 1e6 ).round( 1 ) 
    return chrono


# In[84]:


if unit_test_enabled :
    print( '\nUnit test of : get_chrono_results()' )
    daily_chrono = [ ['NVDA', *[57909497369, 1629441737304739], *[57957208836, 1629441816779205], *[ None, None ] ] ]
    display( get_chrono_results( daily_chrono ) )


# ## Daily One-Shot

# In[85]:


def run_daily_one_shot( daily_history, closing_time ) :
    #log( 'One-shot st7f1c35e7-c173-40cc-b19d-ad54c0a9e523arted' )
    #chrono_start = [ process_time_ns(), perf_counter_ns() ] # Un-comment to measure performance 1/2
    ltps = complete_history( closing_time ) # Complete truncated historical LTPs due to Alpaca's feed delay
    daily_history = get_historical_data()

    # Concatenate both    
    daily_history = pd.concat( [ daily_history, ltps ] )

    # Display daily_history
    print( 'History sample (10 first tickers) :' )
    display( daily_history.iloc[ :, :10 ].round( 2 ) )
    
    # log( get_chrono( *chrono_start ) ) # Un-comment to measure performance 2/2
    #log( 'One-shot completed' )
    return daily_history


# In[86]:


# Get last trading prices to complete truncated historical data due to Alpaca's feed delay 
def complete_history( closing_time ) :
    nb_truncated_records = round_up( HIST_MIN_DELAY / HIST_INTERVAL)
    log( f'Recovering {nb_truncated_records} records to complete historical data (15-minute gap)' )
    ltps = get_ltps()
    for i in range( nb_truncated_records - 1 ) :        
        sleep( 1 )
        if not f_market_is_still_open( closing_time ) : 
            next_opening, closing_time = get_next_sessions( CALENDAR, current_timestamp() ).iloc[ 0 ]
            wait_until( next_opening )
        wait_until_next_run()
        ltps = pd.concat( [ ltps, get_ltps() ] )

    # Get data info
    dates = ltps.index
    date_first, date_last = f'{dates.min():%B %d %Hh%M}', f'{dates.max():%B %d %Hh%M}'
    log( f'{len( ltps ):>4} additional  records from {date_first} to {date_last}' )   
        
    return ltps

# _ = complete_history()  # Un-comment to check function


# ## Scan Trades

# In[89]:


def get_tickers_items( items, text_in ) :
    nb_items = len( items.index )
    message  = f'{nb_items:>3} tickers with {text_in}'
    if nb_items > 0 :
        tickers_with_items = list( set( items[ 'symbol' ].values ) )
        tickers_with_items.sort()
        message += f'\n\t [{" ".join( tickers_with_items )}]'
    else :
        tickers_with_items = []
    log( message ) 

    return tickers_with_items


# In[92]:


def log_signal( signal_record ) :
    # Get string from Pandas row as field1=value1 field2=value2 ... 
    signal_results = [ f'{k}={int( v )}' if 'Signal' in k else f'{k}={v:.1f}' \
              for k, v in signal_record.to_dict().items() ]
    log( ' '.join( signal_results ) )


# In[93]:


def f_market_is_open_today() :
    current_time = current_timestamp()
    period_time  = timedelta( days=7 ) # 1-week period
    schedule_window = CALENDAR.schedule( start_date = current_time - period_time, 
                                         end_date   = current_time )
    last_open_date  = schedule_window.index[ -1 ].date()    
    is_open_today   = ( last_open_date == current_time.date() )
    return is_open_today
    
def f_market_is_still_open( close_dt ) :
    curr_time = current_timestamp()    
    is_open_now = ( curr_time <= close_dt) 
    return is_open_now


# # Annex
# ## Monitor orders
# 
# This table shows the impact of cancellations during the lifecycle of a Bracket Order (Market and Leg orders)
# 
# |Author|Action|Object|Market|Stop-Loss|Target|Cancellable*|
# |--|--|--|--|--|--|--|
# |Trader|submits|Bracket|new|-|-|Bracket|
# |Broker|holds|legs|-|held|new|Bracket|
# |Broker|validates|market|pending new|-|-|Bracket|
# |Exchange|accepts|market|accepted|-|-|Bracket|
# |Exchange|fills|market|filled**|-|-|Legs|
# |Exchange|accepts|legs|-|accepted|accepted|Legs|
# |Trader|liquidates|open position|new (opposite open position)|canceled*|canceled*|-|
# |Exchange|fills|1 leg|closed position|filled / auto-canceled|auto-canceled / filled|-|
# 
# - \* : Trader may cancel an order to end its process
# - NB : _partially filled_ status is not covered by this table


# ### Compare Prices
# 
# Compare prices in open positions vs. pending orders.
# 
# **_qty_available_** = nb of owned stocks - nb reserved in open positions
# - prevents from creating an order with "insufficient qty available for order".
#     - 0 : no new order is required to close the position
#     - positive/negative : a new sell/buy order would be required

# In[ ]:


def compare_prices() :
    # Get prices from limit & stop orders per ticker
    pending_orders = get_orders_by_status( 'pending' )
    pending_limit_orders = pending_orders[ pending_orders[ 'type' ]== 'limit' ].set_index( 'symbol' ) \
                                                        [[ 'position_intent', 'limit_price', 'qty' ]]
    pending_stop_orders  = pending_orders[ pending_orders[ 'type' ]== 'stop'  ].set_index( 'symbol' ) \
                                                        [[ 'position_intent', 'stop_price', 'qty' ]]
    pending_orders_prices = pd.merge( pending_limit_orders, pending_stop_orders, how='outer', 
                                                        on=[ 'symbol','position_intent','qty' ] )
    # Get market prices from open positions per ticker
    positions = TRADING_CLIENT.get_all_positions()
    open_positions = list_of_dicts_to_df( positions ).set_index( 'symbol' ) \
                                                        [[ 'side', 'market_value', 'qty_available' ]]
    # Join pandas
    position_vs_order_prices = pd.merge( open_positions, pending_orders_prices, how='outer', on=[ 'symbol' ] ) \
                [[ 'side', 'position_intent', 'limit_price', 'market_value', 'stop_price', 'qty', 'qty_available' ]]
    position_vs_order_prices = position_vs_order_prices. \
        astype( { 'limit_price':float, 'market_value':float, 'stop_price':float } ). \
        sort_values( by='side' )
    return round( position_vs_order_prices, 1 )
