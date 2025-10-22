// c_signal_generator.h
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <stdbool.h>

// Run Shell commands in a terminal, under folder containing the C script

// 1. compile library *.c -> *.so
// gcc -shared -o c_signal_generator.so c_signal_generator.c

// 2. list functions in library
// nm -g c_signal_generator.so | awk '/ T /{ print $3 }'

// Check updated lines N° 143, 194, 220 which are related to NAN's replacement by 0.0


// --- Helper Functions ---

// Function to calculate Exponential Moving Average (EMA)
void calculate_ema(const double *close_prices, int length, int window, double *output) {
    if (length <= 0 || window <= 0 || window > length) return;

    double alpha = 2.0 / (window + 1.0);
    double initial_ema = 0.0;
    int i;
    
    // 1. Calculate the initial Simple Moving Average (SMA)
    for (i = 0; i < window; i++) {
        initial_ema += close_prices[i];
    }
    initial_ema /= window;
    
    // Set the initial EMA value (at index window - 1)
    output[window - 1] = initial_ema;

    // 2. Calculate subsequent EMAs: EMA_t = (Close_t * alpha) + (EMA_y * (1 - alpha))
    for (i = window; i < length; i++) {
        output[i] = (close_prices[i] * alpha) + (output[i - 1] * (1.0 - alpha));
    }
    
    // Fill leading NaNs for the non-calculated points
    for (i = 0; i < window - 1; i++) {
        output[i] = NAN;
    }
}

// Function to calculate Relative Strength Index (RSI)
void calculate_rsi(const double *close_prices, int length, int window, double *output) {
    if (length <= 0 || window <= 0 || window > length) return;

    // Temporary arrays for change, gain, and loss
    double *change = (double*)malloc(length * sizeof(double));
    double *gain = (double*)malloc(length * sizeof(double));
    double *loss = (double*)malloc(length * sizeof(double));
    
    if (!change || !gain || !loss) {
        // Handle memory allocation failure
        free(change); free(gain); free(loss);
        return;
    }

    // 1. Calculate Price Change, Gain, and Loss
    change[0] = NAN; 
    gain[0] = 0.0;
    loss[0] = 0.0;
    for (int i = 1; i < length; i++) {
        change[i] = close_prices[i] - close_prices[i-1];
        gain[i] = fmax(0.0, change[i]);
        loss[i] = fmax(0.0, -change[i]); // Note: Loss is always positive
    }

    // 2. Calculate Initial Average Gain (AVG) and Average Loss (AVL) (Simple Average)
    double sum_gain = 0.0;
    double sum_loss = 0.0;
    for (int i = 1; i <= window; i++) {
        sum_gain += gain[i];
        sum_loss += loss[i];
    }
    double avg_gain = sum_gain / window;
    double avg_loss = sum_loss / window;
    
    // 3. Calculate Subsequent AVG and AVL (Wilder's Smoothing)
    double alpha_rsi = 1.0 / window; // Smoothing factor
    
    // Calculate the first RSI value
    double rs = (avg_loss == 0.0) ? (avg_gain > 0.0 ? 1e10 : 0.0) : avg_gain / avg_loss;
    output[window] = 100.0 - (100.0 / (1.0 + rs));

    // Iterate for all subsequent points
    for (int i = window + 1; i < length; i++) {
        // Wilder's smoothing: AVG_t = (AVG_y * (1 - alpha)) + (Gain_t * alpha)
        avg_gain = (avg_gain * (1.0 - alpha_rsi)) + (gain[i] * alpha_rsi);
        avg_loss = (avg_loss * (1.0 - alpha_rsi)) + (loss[i] * alpha_rsi);

        // Calculate RS and RSI
        rs = (avg_loss == 0.0) ? (avg_gain > 0.0 ? 1e10 : 0.0) : avg_gain / avg_loss;
        output[i] = 100.0 - (100.0 / (1.0 + rs));
    }

    // Fill leading NaNs (RSI needs window + 1 points to start)
    for (int i = 0; i <= window; i++) {
        output[i] = NAN;
    }
    
    free(change);
    free(gain);
    free(loss);
}


// --- Main C Signal Functions exposed to Python ---

// Generates the EMA crossover signal (1, -1, 0)
void generate_ema_crossover_signal(
    const double *close_prices, 
    int length, 
    //int slow_window, 
    //int fast_window, 
    double slow_window, 
    double fast_window, 
    double *ema_signal_out,
    double *slow_ema_out,
    double *fast_ema_out
) {
    // Input validation (simplified, Python handles most)
    if (slow_window <= fast_window) {
        for(int i=0; i<length; i++) ema_signal_out[i] = NAN;
        return;
    }

    // 1. Calculate slow and fast EMAs
    calculate_ema(close_prices, length, slow_window, slow_ema_out);
    calculate_ema(close_prices, length, fast_window, fast_ema_out);

    // 2. Generate EMA_Pre_Signal and Crossover Signal
    double current_pre_signal;
    double prev_pre_signal = NAN;
    
    for (int i = 0; i < length; i++) {
        if (isnan(slow_ema_out[i]) || isnan(fast_ema_out[i])) {
//            ema_signal_out[i] = NAN;
            ema_signal_out[i] = 0.0;
        } else {
            // Determine current Pre_Signal
            if (fast_ema_out[i] > slow_ema_out[i]) {
                current_pre_signal = 1.0;
            } else if (slow_ema_out[i] > fast_ema_out[i]) {
                current_pre_signal = -1.0;
            } else {
                current_pre_signal = 0.0;
            }

            // Calculate Crossover Signal: np.sign(current_pre_signal - prev_pre_signal)
            if (isnan(prev_pre_signal) || prev_pre_signal == 0.0) {
                 // No signal on the first valid bar or if previous was 0
                ema_signal_out[i] = 0.0; 
            } else if (current_pre_signal != prev_pre_signal) {
                // Signal change detected (Crossover)
                double signal_change = current_pre_signal - prev_pre_signal;
                if (signal_change > 0.0) {
                    ema_signal_out[i] = 1.0;  // Cross up (Buy)
                } else {
                    ema_signal_out[i] = -1.0; // Cross down (Sell)
                }
            } else {
                ema_signal_out[i] = 0.0; // No crossover
            }
            
            // Update previous signal for the next iteration
            prev_pre_signal = current_pre_signal;
        }
    }
}


// Generates the RSI threshold signal (1, -1, 0)
void generate_rsi_threshold_signal(
    const double *close_prices, 
    int length, 
    int rsi_window, 
    double long_entry, 
    double short_entry, 
    double *rsi_out,
    double *rsi_signal_out
) {
    // 1. Calculate RSI
    calculate_rsi(close_prices, length, rsi_window, rsi_out);

    // 2. Generate RSI_Signal: +1 (RSI < long_entry), -1 (RSI > short_entry), 0 (otherwise)
    for (int i = 0; i < length; i++) {
        if (isnan(rsi_out[i])) {
//            rsi_signal_out[i] = NAN;
            rsi_signal_out[i] = 0.0;
        } else if (rsi_out[i] < long_entry) {
            rsi_signal_out[i] = 1.0;  // BUY
        } else if (rsi_out[i] > short_entry) {
            rsi_signal_out[i] = -1.0; // SELL
        } else {
            rsi_signal_out[i] = 0.0; // HOLD
        }
    }
}


// Aggregates EMA and RSI signals: np.sign( EMA_Signal + RSI_Signal )
void aggregate_final_signal(
    const double *ema_signal, 
    const double *rsi_signal, 
    int length, 
    double *final_signal_out
) {
    for (int i = 0; i < length; i++) {
        // Replace NaN with 0.0 before aggregation (equivalent to data['EMA_Signal'].replace(np.nan, 0))
/*        double ema = isnan(ema_signal[i]) ? 0.0 : ema_signal[i];
        double rsi = isnan(rsi_signal[i]) ? 0.0 : rsi_signal[i];
        
        double sum = ema + rsi;
*/        
        double sum = ema_signal[i] + rsi_signal[i];
        
        if (sum > 0.0) {
            final_signal_out[i] = 1.0;
        } else if (sum < 0.0) {
            final_signal_out[i] = -1.0;
        } else {
            final_signal_out[i] = 0.0;
        }
    }
}

