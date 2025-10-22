#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct  1 21:18:09 2025

@author: jean vallee
"""

import pandas as pd
import numpy as np
import ctypes as ct
import os
import sys

# --- 1. Load C Library ---
# --- IMPORTANT: Compile signals.c first using 'gcc -shared -o signals.so -fPIC signals.c' ---
def load_c_lib( lib_rel_path ) : # Load a C library as a ctypes.CDLL object
    try:
        # Get the absolute path to the library file
        lib_path = os.path.join(os.getcwd(), lib_rel_path)
        
        # Load the shared library
        c_library = ct.CDLL(lib_path)
        #c_library = ct.cdll.LoadLibrary(lib_path)
        
        return c_library
    except OSError as e:
        print(f"Error loading library: {e}")
        sys.exit()

lib = load_c_lib( './lib/c/c_signal_generator.so' )
    #'./lib/c/c_lib_reversals.so' )

# Define argument and return types for the C functions
if lib:
    # Ctypes definition for a NumPy array pointer (double*)
    ND_POINTER_DOUBLE = np.ctypeslib.ndpointer(dtype=np.float64, ndim=1, flags='C_CONTIGUOUS')
    
    # 1. EMA Crossover Signal Function Definition
    lib.generate_ema_crossover_signal.argtypes = [
        ND_POINTER_DOUBLE, # close_prices
        ct.c_int,          # length
        #ct.c_int,          # slow_window
        #ct.c_int,          # fast_window
        ct.c_double, # slow_window
        ct.c_double, # fast_window
        ND_POINTER_DOUBLE, # ema_signal_out
        ND_POINTER_DOUBLE, # slow_ema_out
        ND_POINTER_DOUBLE, # fast_ema_out
    ]
    
    # 2. RSI Threshold Signal Function Definition
    lib.generate_rsi_threshold_signal.argtypes = [
        ND_POINTER_DOUBLE, # close_prices
        ct.c_int,          # length
        ct.c_int,          # rsi_window
        ct.c_double,       # long_entry
        ct.c_double,       # short_entry
        ND_POINTER_DOUBLE, # rsi_out
        ND_POINTER_DOUBLE, # rsi_signal_out
    ]
    
    # 3. Final Aggregation Function Definition
    lib.aggregate_final_signal.argtypes = [
        ND_POINTER_DOUBLE, # ema_signal
        ND_POINTER_DOUBLE, # rsi_signal
        ct.c_int,          # length
        ND_POINTER_DOUBLE, # final_signal_out
    ]    


# --- 2. Combined Python Wrapper Function ---

def generate_signals_c(data, rsi_window, optim_param_list, verbose, close_column='Close'):
    """
    Python wrapper that calls the C signal generation logic, mirroring the original Python structure.
    """
    if lib is None:
        return data

    try:
        # Check arguments' values (simplified Python side validation)
        length = len(data)
        if length == 0:
            return data
            
        slow_window, fast_window, long_entry, short_entry = (
            optim_param_list[0], optim_param_list[1], optim_param_list[2], optim_param_list[3]
        )
        
        # --- Data Preparation for C ---
        close_prices = data[close_column].to_numpy(dtype=np.float64)

        # Prepare C output arrays (initialized to NaN for proper handling)
        ema_signal_out = np.full(length, np.nan, dtype=np.float64)
        slow_ema_out = np.full(length, np.nan, dtype=np.float64)
        fast_ema_out = np.full(length, np.nan, dtype=np.float64)
        
        rsi_out = np.full(length, np.nan, dtype=np.float64)
        rsi_signal_out = np.full(length, np.nan, dtype=np.float64)
        
        final_signal_out = np.full(length, np.nan, dtype=np.float64)
        
        # --- Execute C Functions ---

        # 1. EMA Signal (Replaces generate_ema_signal)
        lib.generate_ema_crossover_signal(
            close_prices, length, slow_window, fast_window, 
            ema_signal_out, slow_ema_out, fast_ema_out
        )

        # 2. RSI Signal (Replaces generate_rsi_signal)
        lib.generate_rsi_threshold_signal(
            close_prices, length, rsi_window, long_entry, short_entry, 
            rsi_out, rsi_signal_out
        )
        
        # 3. Aggregate Signal (Replaces final aggregation logic)
        lib.aggregate_final_signal(
            ema_signal_out, rsi_signal_out, length, final_signal_out
        )

        # --- Assign Results Back to DataFrame ---
        data['slow_EMA'] = slow_ema_out
        data['fast_EMA'] = fast_ema_out
        data['EMA_Signal'] = ema_signal_out
        data['RSI'] = rsi_out
        data['RSI_Signal'] = rsi_signal_out
        data['Signal'] = final_signal_out
        
        return data

    except Exception as ex:
        if verbose:
            print('**ERROR** in generate_signals_c()')
            # Fallback to sys._getframe().f_code.co_name in C is challenging, 
            # so we just print the Python function name and error.
            print(f"Error: {sys._getframe().f_code.co_name}, {ex}")
        return pd.DataFrame([])

# --- 3. Demonstration ---
if __name__ == '__main__':
    # Generate dummy data for 50 days
    np.random.seed(42)
    data = pd.DataFrame({
        'Close': np.cumsum(np.random.randn(50) + np.sin(np.linspace(0, 10, 50)) * 5) + 100
    })
    
    # [slow_window, fast_window, long_entry(RSI), short_entry(RSI)]
    optim_params = [20, 10, 30.0, 70.0]
    rsi_win = 14

    print("Data processing initiated...")
    df_signals = generate_signals_c(
        data.copy(), rsi_win, optim_params, verbose=True, close_column='Close'
    )
    
    print("\n--- First 10 Rows of Results from C Logic ---")
    print(df_signals.head(30))
    print(f"\nLast Signal: {df_signals['Signal'].iloc[-1]}")

