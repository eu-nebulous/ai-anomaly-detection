import os
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler
import configparser
from aiad.ml import distance_between_points, tCell, NSA
import logging

def remove_outliers_iqr(df, lower_bound_value, upper_bound_value, lower_quantile=0.10, upper_quantile=0.90):
    """
    Remove outliers using the IQR method combined with manual lower and upper bounds.

    Parameters:
    - df: DataFrame containing the data to clean.
    - lower_bound_value: Dictionary with the minimum values for each metric {column: value}.
    - upper_bound_value: Dictionary with the maximum values for each metric {column: value}.
    - lower_quantile: Lower quantile to use for IQR calculation (default 0.10).
    - upper_quantile: Upper quantile to use for IQR calculation (default 0.90).    

    Returns:
    - DataFrame without outliers.
    """
    for column in df.columns:
        # Calculate the IQR-based bounds
        Q1 = df[column].quantile(lower_quantile)
        Q3 = df[column].quantile(upper_quantile)
        
        # Skip columns where quantiles are NaN
        if pd.isna(Q1) or pd.isna(Q3):
            logging.warning(f"remove_outliers_iqr -> Skipping column {column} due to NaN quantiles")
            continue
            
        IQR = Q3 - Q1

        # Skip columns with zero IQR (no variance)
        if IQR == 0:
            logging.info(f"remove_outliers_iqr -> Skipping column {column} due to zero IQR")
            continue

        # IQR-based lower and upper bounds
        lower_bound_iqr = Q1 - 1.5 * IQR
        upper_bound_iqr = Q3 + 1.5 * IQR

        # Apply manual bounds if they exist for this column
        lower_bound = lower_bound_iqr
        upper_bound = upper_bound_iqr

        if column in lower_bound_value:
            lower_bound = max(lower_bound, 0)                           # Ensure non-negative lower bounds
            lower_bound = max(lower_bound, lower_bound_value[column])   # Use the more restrictive lower bound
        if column in upper_bound_value:
            upper_bound = min(upper_bound, upper_bound_value[column])   # Use the more restrictive upper bound

        logging.info(f"remove_outliers_iqr -> column {column} lower_bound: {lower_bound}, upper_bound: {upper_bound}")

        # Filter the data using the calculated and manual bounds
        outlier_filter = (df[column] >= lower_bound) & (df[column] <= upper_bound)
        df = df[outlier_filter]

    # Drop rows with NaN values that may result from filtering
    df = df.dropna()
    return df

def preprocess_df(df, lags_n, diffs_n, smooth_n):
    """Given a pandas dataframe preprocess it to take differences, add smoothing, lags and finally abs values. 
    """
    if diffs_n >= 1:
        # take differences
        df = df.diff(diffs_n).dropna()
    if smooth_n >= 2:
        # apply a rolling average to smooth out the data a bit
        df = df.rolling(smooth_n).mean().dropna()
    if lags_n >= 1:
        # for each dimension add a new columns for each of lags_n lags of the differenced and smoothed values for that dimension
        df_columns_new = [f'{col}_lag{n}' for n in range(lags_n+1) for col in df.columns]
        df = pd.concat([df.shift(n) for n in range(lags_n + 1)], axis=1).dropna()
        df.columns = df_columns_new
    # sort columns to have lagged values next to each other for clarity when looking at the feature vectors
    df = df.reindex(sorted(df.columns), axis=1)
    
    # take absolute values as last step
    df = abs(df)
    
    return df    

def nsa_train(df, num_samples_to_lag, num_samples_to_diff, num_samples_to_smooth, 
    max_t_cells = 500, max_attempts = 10, percentage = 0.05, algorithm = 2, n_neighbors = 5, anomaly_bit_max = 1):
    
    #################################
    # Train / Re-Train
    #################################

    # preprocess/featurize training data
    df_preprocessed = preprocess_df(
        df,
        num_samples_to_lag,
        num_samples_to_diff,
        num_samples_to_smooth
    )

    # Create class label 0 is "normal"
    y_train = pd.Series([0] * len(df_preprocessed))
    
    # fit model using the fit method of NSA
    
    dimension = df_preprocessed.shape[1]
    
    # max_t_cells are the max number of detectors
    myNSA = NSA(max_t_cells, max_attempts, dimension, percentage, algorithm, n_neighbors, num_samples_to_lag, num_samples_to_diff, num_samples_to_smooth)
    myNSA.fit(df_preprocessed.values, y_train)
            
    return myNSA

def nsa_inference(application_name, myNSA, df, anomaly_bit_max = 1):
    
    #dims = df.columns

    anomaly_radio_threshold = float(myNSA.self_radio)    # or np.sqrt(myNSA.dimension) * myNSA.percentage
    logging.info(f"anomaly_radio_threshold {anomaly_radio_threshold}")

    # initialize dictionary for storing distance scores
    distance_scores2 = {
        't' : [],
        'distance_score': []
    }

    # initialize dictionary for storing anomaly bits
    anomaly_bits2 = {
        't' : [],
        'anomaly_bit': []
    }

    logging.info(f"rows df {df.shape[0]} df_timestamp_min {df.index.min()} df_timestamp_max {df.index.max()}")
    
    row_number = 0
    buffer_size = myNSA.num_samples_to_lag * 2 + myNSA.num_samples_to_diff + myNSA.num_samples_to_smooth

    for t, row in df.iterrows():

        if row_number >= buffer_size:

            #################################
            # Inference / Scoring
            #################################

            # get a buffer of recent data
            # print(f'row {row}')
            # print(f'row_number {row_number}')
            # print(f't {t}')
            # print(f'buffer_size {buffer_size}')
            # print(f'df {df}')
            df_recent = df.loc[(t-buffer_size):t]

            # preprocess/featurize recent data
            df_recent_preprocessed = preprocess_df(
                df_recent,
                myNSA.num_samples_to_lag,
                myNSA.num_samples_to_diff,
                myNSA.num_samples_to_smooth
            )
            
            # take most recent feature vector
            X = df_recent_preprocessed.tail(1).values

            # get prediction
            y_pred = myNSA.predict(X)
            
            # save distance score
            distance_scores2['t'].append(t)
            distance_scores2['distance_score'].append(myNSA.min_distance_self)
            
            # get anomaly bit
            if y_pred[0] == 1:
                anomaly_bit = anomaly_bit_max
            else:
                anomaly_bit = anomaly_bit_max if myNSA.min_distance_self >= anomaly_radio_threshold else 0
                # if myNSA.min_distance_self >= anomaly_radio_threshold:
                    # logging.info('adjust anomaly bit *************************************** PCF')
                    # logging.info(f'myNSA.min_distance_self {myNSA.min_distance_self} anomaly_radio_threshold {anomaly_radio_threshold}')
            
            # save anomaly bit
            anomaly_bits2['t'].append(t)
            anomaly_bits2['anomaly_bit'].append(anomaly_bit)
            
            #logger.info(f"t {t} distance_score {myNSA.min_distance_self} anomaly_bit {anomaly_bit}")

        else:
        
            row_number += 1
        
    # create dataframe of distance scores
    df_distance_scores = pd.DataFrame(data=zip(distance_scores2['t'],distance_scores2['distance_score']),columns=['time_idx','distance_score']).set_index('time_idx')

    # create dataframe of anomaly bits
    df_anomaly_bits = pd.DataFrame(data=zip(anomaly_bits2['t'],anomaly_bits2['anomaly_bit']),columns=['time_idx','anomaly_bit']).set_index('time_idx')

    return df_distance_scores, df_anomaly_bits   

#def predict_with_aiad(data_filename, next_prediction_time=None):
def train_aiad(data_filename, lower_bound_value, upper_bound_value, next_prediction_time=None):
    # Load configuration properties
    # Determine the directory of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Construct the full path to the ini file
    config_path = os.path.join(script_dir, 'aiad.ini')

    config = configparser.ConfigParser()
    config.read(config_path)

    ai_nsa_num_samples_to_lag = config.getint('DEFAULT', 'ai_nsa_num_samples_to_lag')
    ai_nsa_num_samples_to_diff = config.getint('DEFAULT', 'ai_nsa_num_samples_to_diff')
    ai_nsa_num_samples_to_smooth = config.getint('DEFAULT', 'ai_nsa_num_samples_to_smooth')
    ai_nsa_max_t_cells = config.getint('DEFAULT', 'ai_nsa_max_t_cells')                         # max detectors / t_cells: 500
    ai_nsa_max_attempts = config.getint('DEFAULT', 'ai_nsa_max_attempts')                       # max attempts to obtain a new detector: 20
    ai_nsa_percentage = config.getfloat('DEFAULT', 'ai_nsa_percentage')                         # radio = sqrt(#metrics) * nsa_percentage = sqrt(#metrics) * 0.05
    ai_nsa_algorithm = config.getint('DEFAULT', 'ai_nsa_algorithm')                             # KNeighborsClassifier:1, KDTree: 2, BallTree: 3
    ai_nsa_n_neighbors = config.getint('DEFAULT', 'ai_nsa_n_neighbors')                         # neighbors: 5
    ai_nsa_anomaly_rate = config.getfloat('DEFAULT', 'ai_nsa_anomaly_rate')                     # 10% (10 percentage)
    lower_quantile = config.getfloat('DEFAULT', 'lower_quantile')
    upper_quantile = config.getfloat('DEFAULT', 'upper_quantile')
    
    if next_prediction_time is None:
        data_filename = './datasets/_ApplicationPaula-model2.csv'

    if not os.path.exists(data_filename):
        raise FileNotFoundError(f"The file '{data_filename}' does not exist.")
    if os.path.getsize(data_filename) == 0:
        raise ValueError(f"The file '{data_filename}' is empty.")

    # Load and sanitize data
    train_data = pd.read_csv(data_filename)
    logging.info(f'Data {data_filename} loaded.')
    logging.info(train_data.head())

    # if next_prediction_time is not None:
        # current_time = int(pd.Timestamp.now().timestamp())
        # oldest_acceptable_time_point = current_time - (
                # config.getint('DEFAULT', 'number_of_days_to_use_data_from') * 24 * 3600
                # + config.getint('DEFAULT', 'prediction_processing_time_safety_margin_seconds'))
        # # number_of_minutes_to_infer * 60 ... 60 values in 1 minute
        # newest_acceptable_time_point = current_time - (
                # config.getint('DEFAULT', 'number_of_minutes_to_infer') * 60)

        # logging.info(f'current_time {current_time} oldest_acceptable_time_point {oldest_acceptable_time_point} newest_acceptable_time_point {newest_acceptable_time_point}')
        # train_data = train_data[train_data['ems_time'] > oldest_acceptable_time_point]
        # train_data = train_data[train_data['ems_time'] < newest_acceptable_time_point]
        # logging.info("Data after filtering by time")
        # logging.info(train_data.head())

        # if len(train_data) == 0:
            # logging.info("NO data remained after filtering.")
            # return None
    

    # Drop the Timestamp column to avoid it being used in predictions
    train_data.drop(columns=['Timestamp'], inplace=True)
    train_data.set_index('ems_time', inplace=True)
    
    train_data = remove_outliers_iqr(train_data, lower_bound_value, upper_bound_value, lower_quantile, upper_quantile)

    train_data.index = pd.to_datetime(train_data.index, unit='s')
    train_data = train_data.resample('1s').mean().interpolate()  # 1-second intervals
    train_data.index = train_data.index.astype('int64') // 10**9  # Convertir a segundos
    logging.info("Data after resampling and interpolation: ***************. Training data sample.")
    logging.info(train_data.head())
    
    if train_data.empty:
        logging.error("train_data is empty after resampling and interpolation. Check data preparation steps.")
        return None, None

    # Scale the data
    scaler = MinMaxScaler(feature_range=(0, 1))
    train_data_scaled = scaler.fit_transform(train_data)
    train_data_scaled = pd.DataFrame(train_data_scaled, index=train_data.index, columns=train_data.columns)
    
    logging.info("Scaled training data sample.")
    logging.info(train_data_scaled.head())
   
    myNSA = nsa_train(train_data_scaled, ai_nsa_num_samples_to_lag, ai_nsa_num_samples_to_diff, ai_nsa_num_samples_to_smooth, 
                        ai_nsa_max_t_cells, ai_nsa_max_attempts, ai_nsa_percentage, ai_nsa_algorithm, ai_nsa_n_neighbors)   
    
    return scaler, myNSA

def inference_aiad(scaler, myNSA, application_name, data_filename, next_prediction_time=None):
    # Load configuration properties
    # Determine the directory of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Construct the full path to the ini file
    config_path = os.path.join(script_dir, 'aiad.ini')
    config = configparser.ConfigParser()
    config.read(config_path)
    ai_graph = config.getboolean('DEFAULT', 'ai_graph')
    anomaly_bit_max = config.getint('DEFAULT', 'anomaly_bit_max')                              # Assigned value to anomaly_bit   
    
    if next_prediction_time is None:
        data_filename = './datasets/_ApplicationPaula-test2.csv'

    if not os.path.exists(data_filename):
        raise FileNotFoundError(f"The file '{data_filename}' does not exist.")
    if os.path.getsize(data_filename) == 0:
        raise ValueError(f"The file '{data_filename}' is empty.")

    # Load and sanitize data
    test_data = pd.read_csv(data_filename)
    logging.info(f'Data {data_filename} loaded.')
    logging.info(test_data.head())    

    # if next_prediction_time is not None:
        # current_time = int(pd.Timestamp.now().timestamp())
        # # number_of_minutes_to_infer * 60 ... 60 values in 1 minute
        # newest_acceptable_time_point = current_time - (
                # config.getint('DEFAULT', 'number_of_minutes_to_infer') * 60)

        # logging.info(f'current_time {current_time} newest_acceptable_time_point {newest_acceptable_time_point}')

        # test_data = test_data[test_data['ems_time'] >= newest_acceptable_time_point]
        # logging.info("Data after filtering by time")
        # logging.info(test_data.head())

        # if len(test_data) == 0:
            # logging.info("NO data remained after filtering.")
            # return None

    # Drop the Timestamp column to avoid it being used in predictions
    test_data.drop(columns=['Timestamp'], inplace=True)

    test_data.set_index('ems_time', inplace=True)
    test_data.index = pd.to_datetime(test_data.index, unit='s')
    test_data = test_data.resample('1s').mean().interpolate()  # 1-second intervals
    test_data.index = test_data.index.astype('int64') // 10**9  # Convertir a segundos
    logging.info("Data after resampling and interpolation: ***************. Testing data sample.")
    logging.info(test_data.head())
    
    test_data_scaled = scaler.transform(test_data)
    test_data_scaled = pd.DataFrame(test_data_scaled, index=test_data.index, columns=test_data.columns)

    logging.info("Scaled testing data sample.")
    logging.info(test_data_scaled.head())
    
    df_distance_scores, df_anomaly_bits  = nsa_inference(application_name, myNSA, test_data_scaled, anomaly_bit_max)
    
    # join distance scores to test_data
    df_final = test_data.join(df_distance_scores, how='outer')

    #join anomaly bits to raw df
    df_final = df_final.join(df_anomaly_bits, how='outer')
    
    df_final = df_final.dropna()

    logging.info(f'NSA df_final')
    logging.info(df_final)

    if ai_graph:        
        figsize = (16,6)
        
        df_final2 = test_data_scaled.join(df_distance_scores, how='outer')

        #join anomaly bits to raw df
        df_final2 = df_final2.join(df_anomaly_bits, how='outer')
        
        df_final2 = df_final2.dropna()
      

        # PCF el eje y escala mal
        title = f'03.{application_name} NSA Normalized Raw Data to Predict'
        ax = plt.gca()
        dims = test_data.columns
        data = df_final2[dims].set_index(pd.to_datetime(df_final2.index, unit='s'))
        data.plot(title=title, figsize=figsize)
        plt.savefig('images/' + title.replace('|', '.') + '.png', dpi=1200)
        plt.close()
            
        title = f'03.{application_name} NSA Distance Score'
        ax = plt.gca()
        data = df_final2[['distance_score']].set_index(pd.to_datetime(df_final2.index, unit='s'))
        data.plot(title=title, figsize=figsize)
        plt.savefig('images/' + title.replace('|', '.') + '.png', dpi=1200)
        plt.close()
            
        title = f'03.{application_name} NSA Anomaly Bit'
        ax = plt.gca()
        data = df_final2[['anomaly_bit']].set_index(pd.to_datetime(df_final2.index, unit='s'))
        data.plot(title=title, figsize=figsize)
        plt.savefig('images/' + title.replace('|', '.') + '.png', dpi=1200)
        plt.close()

        title = f'03.{application_name} NSA (Distance Score, Anomaly Bit)'
        ax = plt.gca()
        data = df_final2[['distance_score', 'anomaly_bit']].set_index(pd.to_datetime(df_final2.index, unit='s'))
        data.plot(title=title, figsize=figsize)
        plt.savefig('images/' + title.replace('|', '.') + '.png', dpi=1200)
        plt.close()

        title = f'03.{application_name} NSA Combined (Normalized Raw, Score, Bit)'
        ax = plt.gca()
        data = df_final2.set_index(pd.to_datetime(df_final2.index, unit='s'))
        data.plot(title=title, figsize=figsize)
        plt.savefig('images/' + title.replace('|', '.') + '.png', dpi=1200)
        plt.close()
    
    # Window_anomaly_rate to give an alarm
    window_anomaly_rate = df_final['anomaly_bit'].sum() * 100 / (df_final.shape[0] * anomaly_bit_max)

    logging.info(f'The "window anomaly rate" within the last period ({df_final.index.min()}, {df_final.index.max()}) was {window_anomaly_rate}%')
    logging.info(f'Another way to think of this is that {window_anomaly_rate}% of the observations during the last window were considered anomalous based on the latest trained model.')
    
    logging.info('myNSA has finished.\n')

    results = {
        "window_anomaly_rate": window_anomaly_rate,
        "data": df_final
    }

    return results
