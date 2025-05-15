import os
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler
import numpy as np
from sklearn.cluster import KMeans
from scipy.spatial.distance import cdist
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
    
    logging.info(f'NSA model was created!!!')
            
    return myNSA

def nsa_inference(application_name, myNSA, df, anomaly_bit_max = 1):
    
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

    logging.info(f"nsa_inference rows df {df.shape[0]} df_timestamp_min {df.index.min()} df_timestamp_max {df.index.max()}")
    
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
            
            #logging.info(f"t {t} distance_score {myNSA.min_distance_self} anomaly_bit {anomaly_bit}")

        else:
        
            row_number += 1
        
    # create dataframe of distance scores
    df_distance_scores = pd.DataFrame(data=zip(distance_scores2['t'],distance_scores2['distance_score']),columns=['time_idx','distance_score']).set_index('time_idx')

    # create dataframe of anomaly bits
    df_anomaly_bits = pd.DataFrame(data=zip(anomaly_bits2['t'],anomaly_bits2['anomaly_bit']),columns=['time_idx','anomaly_bit']).set_index('time_idx')

    return df_distance_scores, df_anomaly_bits   

def kmeans_train(df, num_samples_to_lag, num_samples_to_diff, num_samples_to_smooth, max_iterations = 100):
    
    #################################
    # Train / Re-Train
    #################################
 
    n_clusters_per_dimension = 2   
    
    # initialize an empty kmeans model for each dimension
    models = {
        dim: {
            'model' : KMeans(n_clusters=n_clusters_per_dimension, max_iter=max_iterations, n_init=2),
            'num_samples_to_lag': num_samples_to_lag,
            'num_samples_to_diff': num_samples_to_diff,
            'num_samples_to_smooth': num_samples_to_smooth
        } for dim in df.columns
    }

    # loop over each dimension/model
    for dim in df.columns:
        
        # get training data
        df_dim_train = df[[dim]]
        
        # preprocess/featurize training data
        df_dim_train_preprocessed = preprocess_df(
            df_dim_train,
            models[dim]['num_samples_to_lag'],
            models[dim]['num_samples_to_diff'],
            models[dim]['num_samples_to_smooth']
        )

        # fit model using the fit method of kmeans
        models[dim]['model'].fit(df_dim_train_preprocessed.values) 
        
        # get cluster centers of model we just trained
        cluster_centers = models[dim]['model'].cluster_centers_

        # get training scores, needed to get min and max scores for normalization at inference time
        train_raw_anomaly_scores = np.sum(cdist(df_dim_train_preprocessed.values, cluster_centers, metric='euclidean'), axis=1)
        # save min and max anomaly score during training, used to normalize all scores to be 0,1 scale
        models[dim]['train_raw_anomaly_score_min'] = min(train_raw_anomaly_scores)
        models[dim]['train_raw_anomaly_score_max'] = max(train_raw_anomaly_scores)

    logging.info(f'kmeans models were created!!!')
    
    return models

def kmeans_inference(application_name, models, df, anomaly_bit_max, ai_kmeans_dim_anomaly_score):

    # initialize dictionary for storing anomaly scores for each dim
    anomaly_scores = {
        dim: {
            't' : [],
            'anomaly_score': []
        } for dim in df.columns
    }

    # initialize dictionary for storing anomaly bits for each dim
    anomaly_bits = {
        dim: {
            't' : [],
            'anomaly_bit': []
        }
        for dim in df.columns
    }

    logging.info(f"kmeans_inference rows df {df.shape[0]} df_timestamp_min {df.index.min()} df_timestamp_max {df.index.max()}")
    
    # for each dimension make predictions
    for dim in df.columns:    
        
        row_number = 0
        buffer_size = models[dim]['num_samples_to_lag'] * 2 + models[dim]['num_samples_to_diff'] + models[dim]['num_samples_to_smooth']
        
        # loop over each row of data in dataframe
        for t, row in df.iterrows():
        
            if row_number >= buffer_size:

                #################################
                # Inference / Scoring
                #################################
                
                # get a buffer of recent data
                df_dim_recent = df[[dim]].loc[(t-buffer_size):t]

                # preprocess/featurize recent data
                df_dim_recent_preprocessed = preprocess_df(
                    df_dim_recent,
                    models[dim]['num_samples_to_lag'],
                    models[dim]['num_samples_to_diff'],
                    models[dim]['num_samples_to_smooth']
                )

                # take most recent feature vector
                X = df_dim_recent_preprocessed.tail(1).values
                
                # get the existing trained cluster centers
                cluster_centers = models[dim]['model'].cluster_centers_

                # get anomaly score based on the sum of the euclidean distances between the 
                # feature vector and each cluster centroid
                # print('X')
                # print(X)
                #logging.info(f'X {X}')
                # print('cluster_centers')
                # print(cluster_centers)
                #logging.info(f'cluster_centers {cluster_centers}')
                raw_anomaly_score = np.sum(cdist(X, cluster_centers, metric='euclidean'), axis=1)[0]

                # normalize anomaly score based on min-max normalization
                # https://en.wikipedia.org/wiki/Feature_scaling#Rescaling_(min-max_normalization)
                # the idea here is to convert the raw_anomaly_score we just computed into a number on a
                # [0, 1] scale such that it behaves more like a percentage. We use the min and max raw scores
                # observed during training to achieve this. This would mean that a normalized score of 1 would
                # correspond to a distance as big as the biggest distance (most anomalous) observed on the 
                # training data. So scores that are 99% or higher will tend to be as strange or more strange
                # as the most strange 1% observed during training.
                
                # normalize based on scores observed during training the model
                train_raw_anomaly_score_min = models[dim]['train_raw_anomaly_score_min']
                train_raw_anomaly_score_max = models[dim]['train_raw_anomaly_score_max']
                #logging.info(f'dim {dim} train_raw_anomaly_score_min {train_raw_anomaly_score_min} train_raw_anomaly_score_max {train_raw_anomaly_score_max}')
                train_raw_anomaly_score_range = train_raw_anomaly_score_max - train_raw_anomaly_score_min
                
                # normalize
                anomaly_score = (raw_anomaly_score - train_raw_anomaly_score_min) / train_raw_anomaly_score_range
                
                # The Netdata Agent does not actually store the normalized_anomaly_score since doing so would require more storage space
                # for each metric, essentially doubling the amount of metrics that need to be stored. Instead, the Netdata Agent makes
                # use of an existing bit (the anomaly bit) in the internal storage representation used by netdata. So if the 
                # normalized_anomaly_score passed the ai_kmeans_dim_anomaly_score (dimension anomaly score threshold) netdata will flip the
                # corresponding anomaly_bit from 0 to 1 to signify that the observation the scored feature vector is considered "anomalous". 
                # All without any extra storage overhead required for the Netdata Agent database! Yes it's almost magic :)

                # get anomaly bit
                anomaly_bit = anomaly_bit_max if anomaly_score >= ai_kmeans_dim_anomaly_score else 0

                #logging.info(f'anomaly_score {anomaly_score}')
                #logging.info(f'anomaly_bit {anomaly_bit}')
                
                # save anomaly score
                anomaly_scores[dim]['t'].append(t)
                anomaly_scores[dim]['anomaly_score'].append(anomaly_score)

                # save anomaly bit
                anomaly_bits[dim]['t'].append(t)
                anomaly_bits[dim]['anomaly_bit'].append(anomaly_bit)
                
            else:
            
                row_number += 1

    # create dataframe of anomaly scores
    df_anomaly_scores = pd.DataFrame()
    for dim in anomaly_scores:
        df_anomaly_scores_dim = pd.DataFrame(data=zip(anomaly_scores[dim]['t'],anomaly_scores[dim]['anomaly_score']),columns=['time_idx',f'{dim}_anomaly_score']).set_index('time_idx')
        df_anomaly_scores = df_anomaly_scores.join(df_anomaly_scores_dim, how='outer')

    # create dataframe of anomaly bits
    df_anomaly_bits = pd.DataFrame()
    for dim in anomaly_bits:
        df_anomaly_bits_dim = pd.DataFrame(data=zip(anomaly_bits[dim]['t'],anomaly_bits[dim]['anomaly_bit']),columns=['time_idx',f'{dim}_anomaly_bit']).set_index('time_idx')
        df_anomaly_bits = df_anomaly_bits.join(df_anomaly_bits_dim, how='outer')

    return df_anomaly_scores, df_anomaly_bits

def train_aiad(ai_nsa, ai_kmeans, data_filename, lower_bound_value, upper_bound_value):
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
    ai_kmeans_num_samples_to_lag = config.getint('DEFAULT', 'ai_kmeans_num_samples_to_lag')
    ai_kmeans_num_samples_to_diff = config.getint('DEFAULT', 'ai_kmeans_num_samples_to_diff')
    ai_kmeans_num_samples_to_smooth = config.getint('DEFAULT', 'ai_kmeans_num_samples_to_smooth')
    ai_kmeans_max_iterations = config.getint('DEFAULT', 'ai_kmeans_max_iterations')
    lower_quantile = config.getfloat('DEFAULT', 'lower_quantile')
    upper_quantile = config.getfloat('DEFAULT', 'upper_quantile')

    if not os.path.exists(data_filename):
        raise FileNotFoundError(f"The file '{data_filename}' does not exist.")
    if os.path.getsize(data_filename) == 0:
        raise ValueError(f"The file '{data_filename}' is empty.")

    # Load and sanitize data
    train_data = pd.read_csv(data_filename)
    logging.info(f'Data {data_filename} loaded.')
    logging.info(train_data.head())

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
        return None, None, None, None
        
    # Remove columns with no variability
    columns_with_variability = train_data.loc[:, train_data.nunique() > 1].columns
    columns_removed = train_data.columns.difference(columns_with_variability)
    
    if not columns_with_variability.any():
        logging.error("All columns have been discarded due to lack of variability.")
        return None, None, None, None

    if not columns_removed.empty:
        logging.warning(f"Columns without variability have been eliminated: {list(columns_removed)}")

    train_data = train_data[columns_with_variability]

    # Scale the data
    scaler = MinMaxScaler(feature_range=(0, 1))
    train_data_scaled = scaler.fit_transform(train_data)
    train_data_scaled = pd.DataFrame(train_data_scaled, index=train_data.index, columns=train_data.columns)
    
    logging.info("Scaled training data sample.")
    logging.info(train_data_scaled.head())

    myNSA = None
    if ai_nsa:
        myNSA = nsa_train(train_data_scaled, ai_nsa_num_samples_to_lag, ai_nsa_num_samples_to_diff, ai_nsa_num_samples_to_smooth, 
                        ai_nsa_max_t_cells, ai_nsa_max_attempts, ai_nsa_percentage, ai_nsa_algorithm, ai_nsa_n_neighbors)   
    
    myKmeans = None
    if ai_kmeans:
        myKmeans = kmeans_train(train_data_scaled, ai_kmeans_num_samples_to_lag, ai_kmeans_num_samples_to_diff, ai_kmeans_num_samples_to_smooth,
                        ai_kmeans_max_iterations)
    
    return columns_with_variability, scaler, myNSA, myKmeans

def inference_aiad(ai_nsa, ai_kmeans, columns_with_variability, scaler, myNSA, myKmeans, data_filename, application_name):
    # Load configuration properties
    # Determine the directory of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Construct the full path to the ini file
    config_path = os.path.join(script_dir, 'aiad.ini')
    config = configparser.ConfigParser()
    config.read(config_path)

    ai_graph = config.getboolean('DEFAULT', 'ai_graph')
    anomaly_bit_max = config.getint('DEFAULT', 'anomaly_bit_max')                              # Assigned value to anomaly_bit   
    ai_kmeans_dim_anomaly_score = config.getfloat('DEFAULT', 'ai_kmeans_dim_anomaly_score')

    if not os.path.exists(data_filename):
        raise FileNotFoundError(f"The file '{data_filename}' does not exist.")
    if os.path.getsize(data_filename) == 0:
        raise ValueError(f"The file '{data_filename}' is empty.")

    # Load and sanitize data
    test_data = pd.read_csv(data_filename)
    logging.info(f'Data {data_filename} loaded.')
    logging.info(test_data.head())    

    # Drop the Timestamp column to avoid it being used in predictions
    test_data.drop(columns=['Timestamp'], inplace=True)

    test_data.set_index('ems_time', inplace=True)
    test_data.index = pd.to_datetime(test_data.index, unit='s')
    test_data = test_data.resample('1s').mean().interpolate()  # 1-second intervals
    test_data.index = test_data.index.astype('int64') // 10**9  # Convertir a segundos
    test_data = test_data[columns_with_variability]
    logging.info("Data after resampling and interpolation: ***************. Testing data sample.")
    logging.info(test_data.head())
    
    test_data_scaled = scaler.transform(test_data)
    test_data_scaled = pd.DataFrame(test_data_scaled, index=test_data.index, columns=test_data.columns)

    logging.info("Scaled testing data sample.")
    logging.info(test_data_scaled.head())

    results = {}
    
    # NSA
    if ai_nsa:
        df_distance_scores, df_anomaly_bits  = nsa_inference(application_name, myNSA, test_data_scaled, anomaly_bit_max)
        
        # join distance scores to test_data
        df_final = test_data.join(df_distance_scores, how='outer')

        #join anomaly bits to raw test_data
        df_final = df_final.join(df_anomaly_bits, how='outer')
        
        df_final = df_final.dropna()

        logging.info(f'NSA df_final')
        logging.info(df_final)

        if ai_graph:        
            figsize = (16,6)
            
            df_final_aux = test_data_scaled.join(df_distance_scores, how='outer')

            #join anomaly bits to raw df
            df_final_aux = df_final_aux.join(df_anomaly_bits, how='outer')
            
            df_final_aux = df_final_aux.dropna()
          

            # PCF el eje y escala mal
            title = f'03.{application_name} NSA Normalized Raw Data to Predict'
            ax = plt.gca()
            dims = test_data.columns
            data = df_final_aux[dims].set_index(pd.to_datetime(df_final_aux.index, unit='s'))
            data.plot(title=title, figsize=figsize)
            plt.savefig('images/' + title.replace('|', '.') + '.png', dpi=1200)
            plt.close()
                
            title = f'03.{application_name} NSA Distance Score'
            ax = plt.gca()
            data = df_final_aux[['distance_score']].set_index(pd.to_datetime(df_final_aux.index, unit='s'))
            data.plot(title=title, figsize=figsize)
            plt.savefig('images/' + title.replace('|', '.') + '.png', dpi=1200)
            plt.close()
                
            title = f'03.{application_name} NSA Anomaly Bit'
            ax = plt.gca()
            data = df_final_aux[['anomaly_bit']].set_index(pd.to_datetime(df_final_aux.index, unit='s'))
            data.plot(title=title, figsize=figsize)
            plt.savefig('images/' + title.replace('|', '.') + '.png', dpi=1200)
            plt.close()

            title = f'03.{application_name} NSA (Distance Score, Anomaly Bit)'
            ax = plt.gca()
            data = df_final_aux[['distance_score', 'anomaly_bit']].set_index(pd.to_datetime(df_final_aux.index, unit='s'))
            data.plot(title=title, figsize=figsize)
            plt.savefig('images/' + title.replace('|', '.') + '.png', dpi=1200)
            plt.close()

            title = f'03.{application_name} NSA Combined (Normalized Raw, Score, Bit)'
            ax = plt.gca()
            data = df_final_aux.set_index(pd.to_datetime(df_final_aux.index, unit='s'))
            data.plot(title=title, figsize=figsize)
            plt.savefig('images/' + title.replace('|', '.') + '.png', dpi=1200)
            plt.close()
        
        # Window_anomaly_rate to give an alarm
        window_anomaly_rate = df_final['anomaly_bit'].sum() * 100 / (df_final.shape[0] * anomaly_bit_max)

        logging.info(f'nsa. The "window anomaly rate" within the last period ({df_final.index.min()}, {df_final.index.max()}) was {window_anomaly_rate}%')
        logging.info(f'nsa. Another way to think of this is that {window_anomaly_rate}% of the observations during the last window were considered anomalous based on the latest trained model.')
        
        logging.info('myNSA has finished.\n')

        results["nsa_data"] = df_final
        results["nsa_window_anomaly_rate"] = window_anomaly_rate
    # end NSA
    
    # kmeans   
    if ai_kmeans:
        df_anomaly_scores, df_anomaly_bits = kmeans_inference(application_name, myKmeans, test_data_scaled, anomaly_bit_max, ai_kmeans_dim_anomaly_score)
        
        # join anomaly scores to raw test_data
        df_final2 = test_data.join(df_anomaly_scores, how='outer')

        # join anomaly bits to raw test_data
        df_final2 = df_final2.join(df_anomaly_bits, how='outer')

        df_final2 = df_final2.dropna()

        logging.info(f'k-means df_final2 = {df_final2}')

        if ai_graph:
            figsize = (16,6)
            
            df_final_aux = test_data_scaled.join(df_anomaly_scores, how='outer')

            df_final_aux = df_final_aux.join(df_anomaly_bits, how='outer')

            df_final_aux = df_final_aux.dropna()   

            for dim in myKmeans:

                # create a dim with the raw data, anomaly score and anomaly bit for the dim
                df_final_dim = df_final_aux[[dim,f'{dim}_anomaly_score',f'{dim}_anomaly_bit']]
                
                title = f'04.{application_name} k-means Normalized Raw Data to Predict - {dim}'
                ax = plt.gca()
                data = df_final_dim[[dim]].set_index(pd.to_datetime(df_final_dim.index, unit='s'))
                data.plot(title=title, figsize=figsize)
                plt.savefig('images/' + title.replace('|', '.') + '.png', dpi=1200)
                plt.close()
                
                title = f'04.{application_name} k-means Anomaly Score - {dim}'
                ax = plt.gca()
                data = df_final_dim[[f'{dim}_anomaly_score']].set_index(pd.to_datetime(df_final_dim.index, unit='s'))
                data.plot(title=title, figsize=figsize)
                plt.savefig('images/' + title.replace('|', '.') + '.png', dpi=1200)
                plt.close()

                title = f'04.{application_name} k-means Anomaly Bit - {dim}'
                ax = plt.gca()
                data = df_final_dim[[f'{dim}_anomaly_bit']].set_index(pd.to_datetime(df_final_dim.index, unit='s'))
                data.plot(title=title, figsize=figsize)
                plt.savefig('images/' + title.replace('|', '.') + '.png', dpi=1200)
                plt.close()

                title = f'04.{application_name} k-means Combined (Normalized Raw, Score, Bit) - {dim}'
                ax = plt.gca()
                #df_final_dim_normalized = (df_final_dim-df_final_dim.min())/(df_final_dim.max()-df_final_dim.min())
                #data = df_final_dim_normalized.set_index(pd.to_datetime(df_final_dim_normalized.index, unit='s'))
                data = df_final_dim.set_index(pd.to_datetime(df_final_dim.index, unit='s'))
                data.plot(title=title, figsize=figsize)
                plt.savefig('images/' + title.replace('|', '.') + '.png', dpi=1200)
                plt.close()

        window_anomaly_rate2 = {}
        for dim in myKmeans:
            
            # create a dim with the raw data, anomaly score and anomaly bit for the dim
            df_final_dim = df_final2[[dim,f'{dim}_anomaly_score',f'{dim}_anomaly_bit']]

            window_anomaly_rate2[dim] = df_final_dim[f'{dim}_anomaly_bit'].sum() * 100 / (df_final_dim.shape[0] * anomaly_bit_max)

            logging.info(f'\n\nkmeans - Dimension {dim}. The "window anomaly rate" within the last period ({df_final_dim.index.min()}, {df_final_dim.index.max()}) was {window_anomaly_rate2}%')
            logging.info(f'kmeans - Dimension {dim}. Another way to think of this is that {window_anomaly_rate2}% of the observations during the last window were considered anomalous based on the latest trained model.')

        logging.info('myKmeans has finished.\n')
        
        results["kmeans_data"] = df_final2
        results["kmeans_window_anomaly_rate"] = window_anomaly_rate2
    # end kmeans

    return results
