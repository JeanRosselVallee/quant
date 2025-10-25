# ## Constants

# In[7]:


EXCHANGE = 'NYSE' # JPX NYSE EUREX BSE ASX  
CREDENTIALS_PATH = './cfg/credentials.cfg'
CSV_DIR = './data/alpaca/'
LOG_DIR = './log/alpaca/'
STRATEGY_PARAMS_PATH = './data/optim_ema_rsi_params.csv'
USER_ZONE_NAME   = 'Europe/Paris'
USER_ZONE_OFFSET = int( datetime.now( tz=ZoneInfo( USER_ZONE_NAME ) ).utcoffset().total_seconds() )


# ## Parameters

# **Assets**
# 
# Stocks list

# In[9]:


TICKERS_PROPOSED  = [ 'NVDA','MSFT','AAPL','AMZN','GOOGL','GOOG','META','AVGO',
             'TSLA','BRK.B','JPM','WMT','ORCL','LLY','V','MA','NFLX',
             'XOM','COST','JNJ','PLTR' ]
#TICKERS_PROPOSED  = [ TICKERS_PROPOSED[1] ] # Debug
#TICKERS_PROPOSED = [ 'PLTR', 'NVDA', 'WMT', 'XOM', 'V', 'WOLF' ] # F Ford
TICKERS = TICKERS_PROPOSED


# **History**
# 
# Historical Data

# In[10]:


HIST_INTERVAL = 2 #15 # minutes
HIST_MIN_DELAY = 15
RSI_WINDOW_SIZE = 14  # = RSI window
WINDOW_SIZE = RSI_WINDOW_SIZE  # debug value of moving window of historical data


# **Orders**
# 
# Parameters to place orders

# In[11]:


QUANTITY     = 1 # quantity of assets per ticker
TARGET_PCT   = 4 # percentage of market price
STOPLOSS_PCT = 2 # percentage


# **Runtime**

# Daily process

# In[12]:


# Initialize values in seconds
INTERVAL  = HIST_INTERVAL * 60
IDLE_TIME = 1


# **Performance**

# In[13]:


CHRONO_COLS = [ 'ticker', 'ltp_received_time', 'ltp_received_cpu', 'signal_generated_time', 
                'signal_generated_cpu', 'order_submitted_time', 'order_submitted_cpu' ]


# ## Debug
# By default main process is launched normally and functions are not checked previously

# In[14]:


mode, unit_test_enabled = 'normal', True 


# Advanced Clock Time

# In[15]:


clock_delay = 0 # Advances/retards clock of N seconds to reduce waiting time in debug mode


# # Custom Functions
# ## Log

# **Daily Log**
# 
# The contents of this variable are stored in a file every evening

# In[16]:


daily_log = ''  # global variable
