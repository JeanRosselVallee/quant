#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct  1 11:20:10 2025

@author: Jean Vallee
"""

# This Python library has been translated to C-language library c_signal_generator.c


import sys
import pandas as pd
import numpy as np
import ta

def generate_signal( data, rsi_window, optim_param_list, verbose, close_column ) :
    
   
    long_entry, short_entry  = optim_param_list[2], optim_param_list[3]
    data = generate_rsi_signal( data, rsi_window, long_entry, short_entry, verbose, close_column )

    slow_window, fast_window = optim_param_list[0], optim_param_list[1]    
    data = generate_ema_signal( data, slow_window, fast_window, verbose, close_column )

    data[[ 'EMA_Signal', 'RSI_Signal' ]] = data[[ 'EMA_Signal', 'RSI_Signal' ]].replace( np.nan, 0 )
    data[ 'Signal' ] = np.sign( data[ 'EMA_Signal' ] + data[ 'RSI_Signal' ] )
    
    return data


def generate_ema_signal( data, slow_window, fast_window, verbose=False, close_column = 'Close' ) :
    # generates a signal based on 2 EMA
    # if at crossover,
    #    EMA slow < EMA fast => go Short (Sell)
    #    EMA slow > EMA fast => go Long  (Buy)
    
    try:
        
        # Check arguments' values
        if ( (len(data) == 0) | (slow_window < fast_window) ):
            if verbose :
                print( '**ERROR** in generate_EMA_Pre_Signal(), check values in')
                print( 'len(data) == 0 and (slow_window < fast_window' )
                print( 'len(data), slow_window, fast_window = ', len(data), slow_window, fast_window )   
            return pd.DataFrame([])

        # Get 2 EMA's
        data['slow_EMA'] = ta.trend.ema_indicator( data[close_column], window=slow_window)        
        data['fast_EMA'] = ta.trend.ema_indicator( data[close_column], window=fast_window)    
  
        # Signal conditions        
        fast_EMA_exceeds = ( data['fast_EMA'] > data['slow_EMA'] ) 
        slow_EMA_exceeds = ( data['slow_EMA'] > data['fast_EMA'] ) 
        
        # Separate Prices' curve in segments of 2 types : longfirst_test_date & short signal
        long_signal  = np.where( fast_EMA_exceeds, +1.0, 0 )
        short_signal = np.where( slow_EMA_exceeds, -1.0, 0 )        
        data['EMA_Pre_Signal'] = np.add( long_signal, short_signal )

        # Make Signal start at the same date as slow_EMA
        slow_ema_start_date = data[ data['slow_EMA'].isna() ].last_valid_index()
        data.loc[ :slow_ema_start_date, 'EMA_Pre_Signal' ] = np.nan 

        # Crossovers

        # Shift Signal & Handle leading Nulls (replace them with 1st valid value
        data['Shifted_Pre'] = data['EMA_Pre_Signal'].shift().bfill()         
        # Crossover Detection = signal's shifts
        data['CrossOvers'] = ( data['EMA_Pre_Signal'] != data['Shifted_Pre'] ) 
        # Assign a different number to each segment
        data['Segment_Id'] = data['CrossOvers'].cumsum()
        # Number items in each segmfirst_test_datefirst_test_dateent 
        data['Items_Count'] = data.groupby('Segment_Id')['EMA_Pre_Signal'].cumsum()
        
        # Signal Detection on CrossOvers = EMA_Pre_Signal's shifts
        data[ 'EMA_Signal' ] = np.sign( data['EMA_Pre_Signal'] - data['Shifted_Pre'] ) 

        # Drop temporary columns
        data.drop( columns=[ 'Segment_Id', 'Shifted_Pre', 'CrossOvers', 'Items_Count' ], inplace=True ) 
        
        return data
    
    except Exception as ex:
        ###############
        if verbose :
            print('**ERROR** in generate_ema_signal()')
        ###############
        print(sys._getframe().f_code.co_name, ex)
        return pd.DataFrame([])


def generate_rsi_signal( data, rsi_window, long_entry, short_entry, verbose, close_column = 'Close' ):
    # generates a signal based only on the RSI
    # in this function we DO NOT have stop loss, target, exit levels in terms os RSI value
    # if RSI < long_entry => BUY
    # if RSI > short_entry => SELL
    
    try:
        
        if ((len(data) == 0) | (rsi_window <= 2)  | (long_entry <= 0)  | (long_entry >= 100) | (short_entry <= 0)  | (short_entry >= 100) | (short_entry <= long_entry) ):
            ###############
            if verbose :
                print( '**ERROR** in generate_rsi_signal(), check values in')
                print( 'len(data)==0), rsi_window<=2, long_entry<=0 or >=100, short_entry<=0 or >=100 or <=long_entry' )
                print( 'len(data), rsi_window, long_entry, short_entry = ', len(data), rsi_window, long_entry, short_entry )            
            ###############
            return pd.DataFrame([])
        
        rsi = ta.momentum.rsi(close = data[close_column], window = rsi_window, fillna=True)
        
        data['RSI'] = rsi
        
        data['buy_signal'] = np.where(data['RSI'] < long_entry, 1.0, 0)
        data['sell_signal'] = np.where(data['RSI'] > short_entry, -1.0, 0)
        data['RSI_Signal'] = data['buy_signal'] + data['sell_signal']
        
        ###############
        data.drop(columns=['buy_signal', 'sell_signal'], inplace=True)        
        ###############
        return data
    
    except Exception as ex:
        ###############
        if verbose :
            print('**ERROR** in generate_rsi_signal()')
        ###############
        print(sys._getframe().f_code.co_name, ex)
        return pd.DataFrame([])
