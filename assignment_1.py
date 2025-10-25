#!/usr/bin/env python
# coding: utf-8

# # Place Orders via Broker's Web API
#  _Created by J. Vallee in 2025_

# **Goal**
# 
# - Permanently scan stocks and place orders every 2 minutes during market's open hours. 
# 
# **Organisation of the NoteBook**
# 
# - Custom functions are defined and tested before the execution of the main process that calls them
# - For tests during closed hours, a retarded/advanced clock simulates any date and time
# - In the Annex, may be handled the resulting pending orders and open positions from the main process

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

# **Custom libraries**
#from lib.jv.lib_paper_trading import *

lib_file_name = './lib/jv/lib_paper_trading.py'
with open( lib_file_name ) as lib_file : exec( lib_file.read() )


# ## Constants
#with open( config_file_name ) as config_file : exec( config_file.read() )


# # Main Process

# ## Pre-Process
# **Define function**

# In[80]:


def pre_process() :        
    CALENDAR = mcal.get_calendar( EXCHANGE )
    CLIENTS = [ get_data_client(), get_trading_client() ] # handle data & orders
    ORDER_PARAMS = { # required to place orders
        'QUANTITY'     : 1 ,
        'TARGET_PCT'   : 4 ,
        'STOPLOSS_PCT' : 2
    }    

    # Get strategy's parameters per ticker
    STRATEGY_PARAMS = get_strategy_params()
    # Display parameters
    print( 'Optimized parameters per ticker :')
    temporary_cols  = [ 'fast_window', 'long_entry', 'short_entry' ] 
    selected_cols   = [ 'ticker', 'slow_window' ] + temporary_cols
    display( STRATEGY_PARAMS[ selected_cols ].round( 1 ) )
    STRATEGY_PARAMS = STRATEGY_PARAMS.drop( columns=temporary_cols )
    
    # Set moving window size for all tickers
    MAX_SLOW_WINDOW_SIZE = round_up( STRATEGY_PARAMS[ 'slow_window' ].max() )
    WINDOW_SIZE = max( MAX_SLOW_WINDOW_SIZE, RSI_WINDOW_SIZE )    
    log( f'Max window size for all tickers : {WINDOW_SIZE}' )    
    STRATEGY_PARAMS = STRATEGY_PARAMS.drop( columns=[ 'slow_window' ] )

    # Get tickers list = intersection of 3 sets
    # 1. Set proposed by user in section "Variables"
    TICKERS_USER       = set( [ t.replace( '-', '.' ) for t in TICKERS_PROPOSED ] ) # Alpaca standard 
    # 2. Set issued from optimization process 
    TICKERS_OPTIMIZED  = set( list (STRATEGY_PARAMS[ 'ticker' ].values ) )
    # 3. Set of available tickers in exchange
    search_request = GetAssetsRequest( asset_class=AssetClass.US_EQUITY )
    TRADING_CLIENT = CLIENTS[ 1 ]
    assets_object  = TRADING_CLIENT.get_all_assets( search_request )
    assets_df      = pd.DataFrame( [ dict( asset ) for asset in assets_object ] )
    status_mask    = ( assets_df[ 'status' ] == AssetStatus.ACTIVE )
    TICKERS_AVAILABLE  = set( list( assets_df[ status_mask ][ 'symbol' ].values ) )
    # 4. List obtained 
    TICKERS            = list( TICKERS_USER & TICKERS_OPTIMIZED & TICKERS_AVAILABLE )    
    log( f'Selected tickers: {TICKERS}' )
    TICKERS_EXCLUDED   = list( ( TICKERS_USER | TICKERS_OPTIMIZED ) - TICKERS_AVAILABLE )
    log( f'Excluded tickers: {TICKERS_EXCLUDED}' )
    
    return TICKERS, CALENDAR, CLIENTS, STRATEGY_PARAMS, WINDOW_SIZE, ORDER_PARAMS


# ## Daily Process
# Calls run_daily_one_shot() & scan_trades()

# In[82]:


def daily_process() :
    daily_history, daily_chrono = [], []  # chrono measures performance    
    current_time = current_timestamp()
    log( f'Today is {current_time.date():%a %d-%b}' )    
    
    try :
        if f_market_is_open_today() :  
            # Conditions on open times
            opening_time, closing_time = get_today_endpoints() # Replace by other existing f ?
            log( f' Today, market opens from {opening_time:%H:%M} to {closing_time:%H:%M} UTC' )
            market_opens_later    = ( current_time <= opening_time )        
            # Wait for opening time
            if market_opens_later : wait_until( opening_time )
            # Wait for next run
            elif f_market_is_still_open( closing_time ) : wait_until_next_run()
        
            # Run daily one-shot
            if f_market_is_still_open( closing_time ) : 
                daily_history = run_daily_one_shot( daily_history, closing_time )
            
            # Run job regularly before market closes        
            while f_market_is_still_open( closing_time ) :
                sleep( IDLE_TIME ) # avoids multiple runs during 1 second
                wait_until_next_run()        
                daily_history, daily_chrono = scan_trades( daily_history, daily_chrono )            
                #log( f'{daily_history[-1:].values}' )

    except Exception as ex :
        log( '***** ERROR : Daily process was interrupted' )
        log_exception( ex )        
        return daily_history, daily_chrono
    
    except KeyboardInterrupt: 
        log( 'Process interrupted by user.' ) 
        #log( f'{len(daily_history)} history records & {len(daily_chrono)} perf records')
        daily_process.interrupted = True
        return daily_history, daily_chrono
    
    log( 'Market is closed' )    
    return daily_history, daily_chrono


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
# 
# For better performance :
# - no custom function is called inside the for-loop of _scan_trades()_ -beside _log()_ & _chrono()_
# - _generate_signals_c()_ is coded in C-language

# In[87]:


def scan_trades( daily_history, daily_chrono ) :   
    # Update moving window on historical data
    daily_history = update_daily_history( daily_history )
    chrono_ltps_received = [ process_time_ns(), perf_counter_ns() ] # for performance
    moving_window = daily_history[ -WINDOW_SIZE: ]    
    # display( moving_window[ -10: ] ) # Un-comment to debug

    # Get tickers with neither open positions nor pending orders
    tickers_to_process = get_tickers_to_process()    
    # Scan tickers
    for ticker in tickers_to_process :
        try :
            # Get ticker's strategy parameters
            mask = ( STRATEGY_PARAMS[ 'ticker' ]==ticker )
            params_i = STRATEGY_PARAMS[ mask ][ 'opt_params' ].values[0]
            # Get ticker's moving window ('Close' column required to generate signal)
            slow_window_size = round_up( params_i[ 0 ] )
            window_size_i = max( RSI_WINDOW_SIZE, slow_window_size )  
            log( f'{ticker:<6}: window size={window_size_i}' )                
            window_i = moving_window[[ ticker ]].rename( columns={ ticker:'Close' } )[ -window_size_i: ]
            # Choose signal generator's version : Python or C
            #window_i = generate_signal( window_i, RSI_WINDOW_SIZE, params_i, True, 'Close' )
            window_i = c_signal_gen.generate_signals_c( window_i, RSI_WINDOW_SIZE, params_i, True, 'Close' )
            chrono_signal_generated = [ process_time_ns(), perf_counter_ns() ] # performance
            # Get signal value
            last_record_i = window_i.iloc[ -1 ]
            signal_value = last_record_i[ 'Signal' ]
            if signal_value in [ -1, +1 ] :
                # Log signal record with indicators' values
                log_signal( last_record_i )
                # Place order
                current_price = last_record_i[ 'Close' ]
                new_order = place_order( current_price, ticker, QUANTITY, signal_value )
                chrono_order_submitted = [ process_time_ns(), perf_counter_ns() ] # performance
                if new_order is not None :
                    check_bracket( new_order.id )            
            else : 
                chrono_order_submitted  = [ None, None ] # performance
            # Log performance of blocks "generate signal", "place order" and both  
            daily_chrono.append( [ ticker, *chrono_ltps_received, *chrono_signal_generated, 
                                     *chrono_order_submitted ] )
        except Exception as ex :
            log( f'{ticker}: ***** ERROR : Could not process ticker' )
            log_exception( ex )            
            #print('scan_trades(): ', len(daily_history), len(daily_chrono))
            return daily_history, daily_chrono
        except KeyboardInterrupt: 
            scan_trades.interrupted = True
            log('Process interrupted by user')
            return daily_history, daily_chrono
            
    return daily_history, daily_chrono


# Get tickers with open_positions & pending execution orders

# In[88]:


def get_tickers_to_process() : 
    # ToDo : chrono delta ltp - signal - place order (return chrono start/go inside functions)
    
    # Get tickers_with open positions 
    open_positions = list_of_dicts_to_df( TRADING_CLIENT.get_all_positions() ) 
    tickers_open_positions = get_tickers_items( open_positions, 'open positions' )
    
    # Get tickers with pending target (limit) & stoploss orders
    pending_orders = get_orders_by_status( 'pending' )
    tickers_pending = []
    for type_i in [ 'market', 'limit', 'stop' ] :
        pending_orders_i = pending_orders[ pending_orders[ 'type' ]==type_i ]
        tickers_pending += get_tickers_items( pending_orders_i, f'pending {type_i} orders' )

    # Get tickers without open positions or pending orders
    tickers_to_skip = set( tickers_pending )
    tickers_to_process = list( set( TICKERS ) - tickers_to_skip )

    nb_tickers_to_process = len( tickers_to_process )
    log( f'{nb_tickers_to_process:>3} tickers to scan \n\t [{" ".join( tickers_to_process )}]' )
    return tickers_to_process


# In[90]:


if unit_test_enabled :
    print( '\nUnit test of : get_tickers_to_process()' )
    print( get_tickers_to_process() )


# In[91]:


def update_daily_history( daily_history ) :
    #chrono_start = [ process_time_ns(), perf_counter_ns() ]  # Un-comment to measure performance 1/2
    # Get last trading prices
    ltps = get_ltps()
    # Append to daily historical data
    daily_history = pd.concat( [ daily_history, ltps ] )
    #log( get_chrono( *chrono_start ) ) # Un-comment to measure performance 2/2
    return daily_history



# **Check function**
# 
# Check _daily_process() = daily_one_shot()_ + _daily_process() execution_

# In[94]:


if unit_test_enabled :
    print( '\nUnit test of : daily_process()' )
    clock_delay = get_seconds_to_dt( get_last_weekday( 'Thursday' ), [ 19, 24, 59 ] )
    wait_until, wait_until_next_run, get_ltps, place_order = get_prototypes( 'accelerated' )
    TICKERS, CALENDAR, CLIENTS, STRATEGY_PARAMS, WINDOW_SIZE, ORDER_PARAMS = pre_process()
    DATA_CLIENT, TRADING_CLIENT = CLIENTS
    QUANTITY, TARGET_PCT, STOPLOSS_PCT = ORDER_PARAMS.values()
    
    try :
        daily_history, daily_chrono = daily_process()
    except KeyboardInterrupt: 
        log('Process interrupted by user')
        
    clock_delay = 0


# In[95]:


if unit_test_enabled and ( daily_history is not None ) : 
    print( '\nUnit test of : daily_history' )
    #display( daily_history.T )
    plot_variation_prices( daily_history, nb_last_records=30 )


# ## Execution

# ### Normal execution 
# **Normal** execution mode with actual time is set by default

# In[96]:


execution_mode = 'normal'
clock_delay, file_suffix = 0 , ''


# **Optional time simulation** 
# 
# For an execution with optional **simulated time**, select a weekday & a time-scenario for these simulated modes :
# - **advanced** mode, launches on different dates (previous weekdays) & times
# - **accelerated** mode, idem + reduces waiting time during the whole process

# In[97]:


execution_mode = 'accelerated'  #  Uncomment & set: advanced or accelerated
time_scenario = 'PRE_CLOSE' 
weekday = 'Wednesday' # Monday Friday Saturday

TIME_SCENARII = {
    'CLOSED_AM' :   [ 8,  8,  8],  # Closed AM hours
    'PRE_OPEN' :    [10, 10, 10],  # Closed before opening
    'OPEN_MIDDAY' : [13, 29, 54],  # Open hours
    'PRE_CLOSE' :   [19, 44, 50]   # Open before closing
}
if execution_mode in [ 'advanced', 'accelerated' ] : 
    simulated_date = get_last_weekday( weekday ) 
    simulated_time = TIME_SCENARII[ time_scenario ] 
    clock_delay = get_seconds_to_dt( simulated_date, simulated_time )
    file_suffix = f'_{execution_mode}'


# ### Launch

# In[ ]:


try :
    # Pre-process
    TICKERS, CALENDAR, CLIENTS, STRATEGY_PARAMS, WINDOW_SIZE, ORDER_PARAMS = pre_process()
    DATA_CLIENT, TRADING_CLIENT = CLIENTS
    QUANTITY, TARGET_PCT, STOPLOSS_PCT = ORDER_PARAMS.values()
    daily_log, daily_history, daily_chrono = '', [], []
    wait_until, wait_until_next_run, get_ltps, place_order = get_prototypes( execution_mode )
    daily_process.interrupted, scan_trades.interrupted = False, False # Function's attributes
    
    while True :    # Repeat everyday
        
        # Daily process
        daily_history, daily_chrono = daily_process()
        if daily_process.interrupted or scan_trades.interrupted : 
            raise KeyboardInterrupt
            
        # Log results
        save_results( daily_log, daily_history, daily_chrono, file_suffix )
        daily_log, daily_history, daily_chrono = '', [], []
        
        # Wait for next opening day
        idle_time = get_seconds_to_opening() - 2*60*60 # time to 2 hours before market opens
        if execution_mode == 'accelerated' : 
            clock_delay = idle_time # advance clock by idle time
        else : 
            sleep( idle_time )      # do nothing during idle time
        
except SystemExit :
    log('Program terminated due to critical error')
    save_results( daily_log, daily_history, daily_chrono, file_suffix )
except KeyboardInterrupt: 
    log('Process interrupted by user')
    save_results( daily_log, daily_history, daily_chrono, file_suffix )


# ### Check Results

# Set date to check 'YYYYMMDD'

# In[ ]:


exec_date = '20251010'


# Log File

# In[ ]:


#get_ipython().system(' grep "submitted" ./log/alpaca/api_orders_{ exec_date }.log | cut -d":" -f 4 | sort | tr "\\n" " "')
print( 'Run command line in a terminal' )
print( 'grep "submitted" ./log/alpaca/api_orders_{ exec_date }.log | cut -d":" -f 4 | sort | tr "\\n" " "')
 

# In[ ]:


#get_ipython().system(' grep "ERROR" ./log/alpaca/api_orders_{ exec_date }.log | cut -d":" -f 4  | sort | uniq -c #tr "\\n" " "')
print( 'Run command line in a terminal' )
print( 'grep "ERROR" ./log/alpaca/api_orders_{ exec_date }.log | cut -d":" -f 4  | sort | uniq -c #tr "\\n" " "')


# Historical Data

# In[ ]:


daily_history_path = f'./data/alpaca/hist_{ exec_date }.csv'
daily_history = read_df_from_csv( daily_history_path, index_column='timestamp', timestamp_index=True )
print( daily_history.info( verbose=False ) )
daily_history.round( 1 ).head( 1 )


# Orders Summary

# In[ ]:


daily_orders_path = f'./data/alpaca/orders_{ exec_date }.csv'
daily_orders = read_df_from_csv( daily_orders_path, index_column='id' )
print( daily_orders[[ 'type', 'status' ]].groupby(by=[ 'type' ]).value_counts() )
daily_orders.head( 1 )


# Performance

# In[ ]:


chrono_path = f'./data/alpaca/chrono_ms_{ exec_date }.csv'
chrono = read_df_from_csv( chrono_path, index_column='ticker' )                           
chrono = chrono[[ 'delta_signal', 'delta_order', 'delta_signal_cpu', 'delta_order_cpu' ]] 
signal_with_order = ~( chrono[ 'delta_order' ].isna() )
print( 'Stats & sample of signals followed by 1 order' )
display( chrono[ signal_with_order ].describe().loc[[ 'min', 'mean', 'max' ]].round( 1 ) )
display( chrono[ signal_with_order ].head(1) )
print( 'Stats & sample of signals followed by NO order' )
display( chrono[ ~signal_with_order ].describe().loc[[ 'min', 'mean', 'max' ]].round( 1 ) )
display( chrono[ ~signal_with_order ].head(1) ) 


# **End Process**

# In[ ]:


assert False, 'Everything OK: End of process'  # prevents the execution of following cells


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

# In[ ]:


#_, _, _, TRADING_CLIENT, _, _, _ = pre_process() 


# In[ ]:


#get_ipython().system(' grep -H "submitted" ./log/alpaca/api_orders_202510*.log')
print( 'Run command line in a terminal' )
print( 'grep -H "submitted" ./log/alpaca/api_orders_202510*.log')

# ### Check pending Market orders

# In[ ]:


nb_pending_bracket_orders, pending_bracket_orders =  list_pending_bracket_orders()
print( f'{nb_pending_bracket_orders} pending_bracket_orders' )
display( pending_bracket_orders )


# ### Cancel single Bracket

# In[ ]:


_ = cancel_order( '34909003-18fd-42e6-b493-b51ed0782e20' )


# ### Cancel Brackets

# In[ ]:


#cancel_pending_brackets()


# ### Check all pending orders

# In[ ]:


pending_orders = get_orders_by_status( 'pending' )
print( f'{len( pending_orders )} pending_orders' )
print( pending_orders[[ 'type', 'status' ]].groupby(by=[ 'type' ]).value_counts() ) 
pending_orders


# ### Custom search

# Get Pandas with all orders

# In[ ]:


all_orders = get_orders_by_status( 'all' )
all_orders.head( 1 )


# Select a ticker

# In[ ]:


orders_1_ticker = all_orders[ all_orders [ 'symbol' ] == 'GOOG' ]  # Search by ticker
orders_1_ticker.head(1)


# Get counts

# In[ ]:


orders_1_ticker[[ 'type', 'position_intent', 'status' ]].groupby(by=[ 'type', 'position_intent' ]).value_counts() 


# Get Bracket count by date

# In[ ]:


ost = os[ os[ 'type' ]=='market' ]
ost[ 'created_at' ].apply( datetime.date ).value_counts().sort_index( ascending=False )


# ### Check daily orders

# In[ ]:


get_daily_orders( date( 2025, 10, 10 ) )
#get_daily_orders( current_timestamp().date() )


# ## Monitor Positions
# The position of an account only changes when a trade is filled 
# - Open = you hold shares or are short
# - Closed = you hold no shares
# 
# ### Check Open Positions

# In[ ]:


positions = TRADING_CLIENT.get_all_positions()
nb_positions = len( positions )
if nb_positions > 0 :
    open_pos = list_of_dicts_to_df( positions ) \
        [[ 'symbol', 'side', 'market_value', 'qty_available', 'unrealized_plpc']]
    print( f'{nb_positions} open positions' )
    display( open_pos )


# ### Compare Prices
# 
# Compare prices in open positions vs. pending orders.
# 
# **_qty_available_** = nb of owned stocks - nb reserved in open positions
# - prevents from creating an order with "insufficient qty available for order".
#     - 0 : no new order is required to close the position
#     - positive/negative : a new sell/buy order would be required

# In[ ]:

compare_prices()


# ### Liquidate Positions
# A position is opened when the market order of a bracket is filled
# - if an exit leg is then canceled, the other leg is automatically canceled 
# - the position remains open and you should close it manually or liquidate it
#     - during closed hours, orders are queued for execution at the next trading session
#     - between 4am & 8pm, limit orders (TIF=day & extended_hours=True) may be executed

# In[ ]:


#liquidate_all_open_positions()


# ## Plot variation prices

# In[ ]:


plot_variation_prices( daily_history, nb_last_records=30 )


# **Theory**
# - Derivative contracts
#     - **Future** : both, buyer and seller, are compelled to trade at strike price on expiration date
#     - **Option** : its writer is compelled to trade while its holder has 2 options, trade or lose the premium.
# - **Futures Market** : has 2 advantages over Live Market
#     - it's open 24/5
#     - it requires no minimum account balance 
