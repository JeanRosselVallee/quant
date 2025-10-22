#!/usr/bin/env python
# coding: utf-8

# - Developed by J. Vallee in 2025
# - Anaconda Navigator 2.6.5 (Cloud)
# - Environment "conda_trading_env"

# Built-In Libraries
import pandas as pd                       # manipulate dataframes
import pytz                               # time zones
from datetime import datetime, timedelta    # handle dates
from time import process_time_ns, perf_counter_ns # classic chrono vs CPU's calculation time

# Lambda functions
np_to_utc = lambda dt : datetime.fromtimestamp( int( dt.values[0] ) / 1e9 ).astimezone( pytz.utc )
round_up = lambda x : -int( -x // 1 )
list_of_dicts_to_df = lambda items : pd.DataFrame( [ item.model_dump() for item in items ] )

# Custom Functions

def get_next_sessions( CALENDAR, current_timestamp ) :
    #current_time = current_timestamp()
    start_time = current_timestamp + timedelta( days=1 )
    end_time   = start_time + timedelta( days=7 ) # 1-week period
    schedule_window = CALENDAR.schedule( start_date=start_time, end_date=end_time )
    #display( schedule_window )  # Debug
    return schedule_window

    
# Get delta time in seconds
def get_delta_seconds( dt_A, dt_B ) :
    if dt_A > dt_B : 
        delta_seconds = + ( dt_A - dt_B ).total_seconds()
    else :
        delta_seconds = - ( dt_B - dt_A ).total_seconds()  
    return int( delta_seconds )

def get_credentials( credentials_path ) :
    credentials = pd.read_csv( credentials_path )
    api_key, api_secret = credentials.values[0]    
    return api_key, api_secret

def get_balance( TRADING_CLIENT ) :
    account   = TRADING_CLIENT.get_account()     
    total     = float( account.equity )
    cash      = float( account.cash )
    long_pos  = float( account.long_market_value )
    short_pos = float( account.short_market_value )
    appended_text = ''
    if long_pos > 0  : appended_text += f'Long Positions = {long_pos:>7,.2f}$ '
    if short_pos > 0 : appended_text += f'Short Positions = {short_pos:>7,.2f}$ '
    if total != cash : appended_text += f'Total = {total:>10,.2f}$ '
    return f'Cash = {cash:>10,.2f}$ ' + appended_text
    #return f'Cash = {cash:>10,.2f}$  Long Positions = {long_pos:>7,.2f}$  Short Positions = {long_pos:>7,.2f}$  Total = {total:>10,.2f}$'

def get_data_info( data, nb_records ) :
    dates = data.index
    avg_interval = int( dates.diff().mean().total_seconds() // 60 ) # in minutes
    print( f'{nb_records} most recent records ({avg_interval}-minute average interval)\n \
    from {dates.min():%B %d %Hh%M} \n\t to   {dates.max():%B %d %Hh%M}' )    
    display( data.describe().T[[ 'min', 'mean', 'max', 'std' ]].round(2).T )    

def get_last_weekday( day_name ) :
    i_weekday = { 'Monday':0, 'Tuesday':1, 'Wednesday':2, 'Thursday':3, 'Friday':4, 'Saturday':5, 'Sunday':6 }
    target_weekday = i_weekday[ day_name ] 
    # Get offset to target weekday
    today = datetime.today()
    current_weekday = today.weekday()
    if current_weekday == target_weekday : 
        offset = 7
    else : 
        #offset = ( current_weekday - target_weekday ) % 7
        offset = 7 + ( current_weekday - target_weekday ) % 7  # at least 1 week ago
    # Get weekday's date    
    target_date = today - timedelta( days=offset )
    target_date_tuple = target_date.timetuple()[:3]
    target_date_list = list( reversed( target_date_tuple ) )    
    return target_date_list
#get_last_weekday( 'Friday' )

def get_chrono( chrono_start, cpu_start ) : # in nano-seconds
    duration = process_time_ns() - chrono_start
    cpu_time = perf_counter_ns() - cpu_start
    ns_per_ms = 1e6
    if duration < ns_per_ms : # less time than 1 ms
        duration_str  = f'{duration:>10,}'.replace( ',', ' ' ) + ' ns'
        cpu_time_str  = f'{cpu_time:>10,}'.replace( ',', ' ' ) + ' CPU ns'
    else : 
        duration_str  = f'{round( duration/ns_per_ms, 1 ):>5,}'.replace( ',', ' ' ) + ' ms'
        cpu_time_str  = f'{round( cpu_time/ns_per_ms, 1 ):>5,}'.replace( ',', ' ' ) + ' CPU ms'
    return (f'Performance: {duration_str} - {cpu_time_str}') 

def plot_variation_prices( data, nb_last_records=10, hide_closed_hours=True ) :
    target_data = data[ -nb_last_records-1 : ]
    #display( target_data )
    if hide_closed_hours :
        tickers     = target_data.columns
        target_data = target_data.reset_index()
        timestamps  = target_data[ 'timestamp' ].dt.strftime('%d/%m %Hh%M')
        target_data = target_data.diff()
        target_data[ 'timestamp' ] = timestamps
        target_data.plot( x='timestamp', y=tickers, title='Price Variation', figsize=[ 15, 4 ] ) 
    else :
        target_data.diff().plot( title='Price Variation', figsize=[ 15, 4 ] )   
    #display( target_data )
  

