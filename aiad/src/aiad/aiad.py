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

from sklearn.feature_selection import VarianceThreshold
from sklearn.ensemble import RandomForestRegressor

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

def nsa_train(df, num_samples_to_lag, num_samples_to_diff, num_samples_to_smooth, ai_nsa_percentile_anomaly_score_threshold,
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
    myNSA.compute_dynamic_threshold(df_preprocessed.values[y_train == 0], percentile=ai_nsa_percentile_anomaly_score_threshold)
    
    logging.info(f'NSA model was created!!!')
            
    return myNSA

def nsa_inference(application_name, myNSA, df, anomaly_bit_max = 1):
    
    # myNSA.anomaly_radio_threshold = compute_dynamic_threshold or np.sqrt(myNSA.dimension) * myNSA.percentage 
    logging.info(f"NSA anomaly_radio_threshold {myNSA.anomaly_radio_threshold}")

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
    buffer_size = myNSA.num_samples_to_lag * 2 + myNSA.num_samples_to_diff + myNSA.num_samples_to_smooth + 1
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

            df_recent = df.loc[:t].tail(buffer_size)

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
                anomaly_bit = anomaly_bit_max if myNSA.min_distance_self >= myNSA.anomaly_radio_threshold else 0
                # if myNSA.min_distance_self >= myNSA.anomaly_radio_threshold:
                    # logging.info('adjust anomaly bit *************************************** PCF')
                    # logging.info(f'myNSA.min_distance_self {myNSA.min_distance_self} myNSA.anomaly_radio_threshold {myNSA.anomaly_radio_threshold}')
            
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

def kmeans_train(df, num_samples_to_lag, num_samples_to_diff, num_samples_to_smooth, ai_kmeans_percentile_anomaly_score_threshold, max_iterations=100):
    #################################
    # Train / Re-Train
    #################################
    n_clusters_per_dimension = 2   
    
    # initialize an empty kmeans model for each dimension
    models = {
        dim: {
            'model': KMeans(n_clusters=n_clusters_per_dimension, max_iter=max_iterations, n_init=2),
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
        
        # get cluster centers
        cluster_centers = models[dim]['model'].cluster_centers_

        # get training scores, needed to get min and max scores for normalization at inference time
        train_raw_anomaly_scores = np.sum(
            cdist(df_dim_train_preprocessed.values, cluster_centers, metric='euclidean'),
            axis=1
        )

        # save min and max anomaly score during training
        models[dim]['train_raw_anomaly_score_min'] = float(np.min(train_raw_anomaly_scores))
        models[dim]['train_raw_anomaly_score_max'] = float(np.max(train_raw_anomaly_scores))

        # calculate adaptive threshold based on ai_kmeans_anomaly_rate
        train_range = models[dim]['train_raw_anomaly_score_max'] - models[dim]['train_raw_anomaly_score_min']
        if train_range > 0:
            normalized_scores = (train_raw_anomaly_scores - models[dim]['train_raw_anomaly_score_min']) / train_range
            models[dim]['anomaly_score_threshold'] = np.percentile(normalized_scores, ai_kmeans_percentile_anomaly_score_threshold)
        else:
            models[dim]['anomaly_score_threshold'] = 1.0

    logging.info('kmeans models were created!!!')
    return models

def kmeans_inference(application_name, models, df, anomaly_bit_max, ai_kmeans_dim_anomaly_score):
    # initialize dictionary for storing anomaly scores for each dim
    anomaly_scores = {dim: {'t': [], 'anomaly_score': []} for dim in df.columns}
    # initialize dictionary for storing anomaly bits for each dim
    anomaly_bits = {dim: {'t': [], 'anomaly_bit': []} for dim in df.columns}

    logging.info(f"kmeans_inference rows df {df.shape[0]} df_timestamp_min {df.index.min()} df_timestamp_max {df.index.max()}")
    
    # for each dimension make predictions
    for dim in df.columns:    
        row_number = 0
        buffer_size = models[dim]['num_samples_to_lag'] * 2 + models[dim]['num_samples_to_diff'] + models[dim]['num_samples_to_smooth'] + 1
        
        # loop over each row of data in dataframe
        for t, row in df.iterrows():

            if row_number >= buffer_size:

                #################################
                # Inference / Scoring
                #################################

                # get a buffer of recent data (last buffer_size samples up to t inclusive)
                df_dim_recent = df[[dim]].loc[:t].tail(buffer_size)

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

                # get anomaly score based on the sum of the euclidean distances between the feature vector and each cluster centroid
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
                train_min = models[dim]['train_raw_anomaly_score_min']
                train_max = models[dim]['train_raw_anomaly_score_max']
                train_range = train_max - train_min

                if train_range == 0:
                    logging.warning(f"[kmeans_inference] Null range in training for {dim}. All values equal. Assigned anomaly_score=0.0")
                    anomaly_score = 0.0
                else:
                    anomaly_score = (raw_anomaly_score - train_min) / train_range

                # use adaptive threshold if it exists, otherwise use the fixed threshold
                dim_threshold = models[dim].get('anomaly_score_threshold', ai_kmeans_dim_anomaly_score)
                anomaly_bit = anomaly_bit_max if anomaly_score >= dim_threshold else 0

                anomaly_scores[dim]['t'].append(t)
                anomaly_scores[dim]['anomaly_score'].append(anomaly_score)
                anomaly_bits[dim]['t'].append(t)
                anomaly_bits[dim]['anomaly_bit'].append(anomaly_bit)
            else:
                row_number += 1

    # create dataframe of anomaly scores
    df_anomaly_scores = pd.DataFrame()
    for dim in anomaly_scores:
        df_anomaly_scores_dim = pd.DataFrame(
            data=zip(anomaly_scores[dim]['t'], anomaly_scores[dim]['anomaly_score']),
            columns=['time_idx', f'{dim}_anomaly_score']
        ).set_index('time_idx')
        df_anomaly_scores = df_anomaly_scores.join(df_anomaly_scores_dim, how='outer')

    # create dataframe of anomaly bits
    df_anomaly_bits = pd.DataFrame()
    for dim in anomaly_bits:
        df_anomaly_bits_dim = pd.DataFrame(
            data=zip(anomaly_bits[dim]['t'], anomaly_bits[dim]['anomaly_bit']),
            columns=['time_idx', f'{dim}_anomaly_bit']
        ).set_index('time_idx')
        df_anomaly_bits = df_anomaly_bits.join(df_anomaly_bits_dim, how='outer')

    return df_anomaly_scores, df_anomaly_bits

def train_aiad(ai_nsa, ai_kmeans, data_filename, lower_bound_value, upper_bound_value):

    # load configuration
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, 'aiad.ini')
    config = configparser.ConfigParser()
    config.read(config_path)

    ai_nsa_num_samples_to_lag = config.getint('DEFAULT', 'ai_nsa_num_samples_to_lag')
    ai_nsa_num_samples_to_diff = config.getint('DEFAULT', 'ai_nsa_num_samples_to_diff')
    ai_nsa_num_samples_to_smooth = config.getint('DEFAULT', 'ai_nsa_num_samples_to_smooth')
    ai_nsa_max_t_cells = config.getint('DEFAULT', 'ai_nsa_max_t_cells')
    ai_nsa_max_attempts = config.getint('DEFAULT', 'ai_nsa_max_attempts')
    ai_nsa_percentage = config.getfloat('DEFAULT', 'ai_nsa_percentage')
    ai_nsa_algorithm = config.getint('DEFAULT', 'ai_nsa_algorithm')
    ai_nsa_n_neighbors = config.getint('DEFAULT', 'ai_nsa_n_neighbors')
    ai_nsa_percentile_anomaly_score_threshold = config.getint('DEFAULT', 'ai_nsa_percentile_anomaly_score_threshold')
    ai_kmeans_num_samples_to_lag = config.getint('DEFAULT', 'ai_kmeans_num_samples_to_lag')
    ai_kmeans_num_samples_to_diff = config.getint('DEFAULT', 'ai_kmeans_num_samples_to_diff')
    ai_kmeans_num_samples_to_smooth = config.getint('DEFAULT', 'ai_kmeans_num_samples_to_smooth')
    ai_kmeans_max_iterations = config.getint('DEFAULT', 'ai_kmeans_max_iterations')
    ai_kmeans_percentile_anomaly_score_threshold = config.getint('DEFAULT', 'ai_kmeans_percentile_anomaly_score_threshold')
    ai_top_n_features = config.getint('DEFAULT', 'ai_top_n_features')
    lower_quantile = config.getfloat('DEFAULT', 'lower_quantile')
    upper_quantile = config.getfloat('DEFAULT', 'upper_quantile')

    if not os.path.exists(data_filename):
        raise FileNotFoundError(f"The file '{data_filename}' does not exist.")
    if os.path.getsize(data_filename) == 0:
        raise ValueError(f"The file '{data_filename}' is empty.")

    train_data = pd.read_csv(data_filename)
    # logging.info(f'Data {data_filename} loaded.')
    # logging.info(train_data.head())

    train_data.drop(columns=['Timestamp'], inplace=True, errors='ignore')
    train_data.set_index('ems_time', inplace=True)

    # remove outliers
    # train_data = remove_outliers_iqr(train_data, lower_bound_value, upper_bound_value, lower_quantile, upper_quantile)

    # convert index to datetime if it is in seconds
    train_data.index = pd.to_datetime(train_data.index, unit='s')

    # reindex at 1-minute frequency and fill NaNs
    train_data = train_data.resample('1min').mean().interpolate(method='linear').fillna(0)

    # convert index to seconds for 'aiad' compatibility
    train_data.index = (train_data.index.astype('int64') // 10**9).astype(int)

    if train_data.empty:
        logging.error("train_data is empty after resampling and interpolation.")
        return None, None, None, None

    # ===============================================================
    # COLUMN FILTERING
    # ===============================================================

    # 1️ Remove columns with no variability
    columns_with_variability = train_data.loc[:, train_data.nunique() > 1].columns
    columns_removed = train_data.columns.difference(columns_with_variability)
    if not columns_removed.empty:
        logging.warning(f"Columns without variability removed: {list(columns_removed)}")
    train_data = train_data[columns_with_variability]

    # 2️ Remove columns with low variance
    selector = VarianceThreshold(threshold=0.001)
    train_data_filtered = pd.DataFrame(selector.fit_transform(train_data),
                                       index=train_data.index,
                                       columns=train_data.columns[selector.get_support()])
    low_var_cols = train_data.columns[~selector.get_support()]
    if len(low_var_cols) > 0:
        logging.warning(f"Low-variance columns removed: {list(low_var_cols)}")
    train_data = train_data_filtered

    # 3️ Remove highly correlated columns
    corr_matrix = train_data.corr().abs()
    upper = corr_matrix.where(np.triu(np.ones(corr_matrix.shape), k=1).astype(bool))
    high_corr_features = [column for column in upper.columns if any(upper[column] > 0.9)]
    if high_corr_features:
        logging.warning(f"Highly correlated columns removed: {high_corr_features}")
    train_data.drop(columns=high_corr_features, inplace=True, errors='ignore')

    # 4 Select the N most representative columns by importance
    top_n_features = ai_top_n_features
    if train_data.shape[1] > top_n_features:
        rf = RandomForestRegressor(n_estimators=50, random_state=42)
        rf.fit(train_data.fillna(0), np.arange(len(train_data)))
        importances = pd.Series(rf.feature_importances_, index=train_data.columns)
        selected_features = importances.sort_values(ascending=False).head(top_n_features).index
        removed_features = set(train_data.columns) - set(selected_features)
        logging.warning(f"Feature selection: kept top {top_n_features}, removed {len(removed_features)}")
        train_data = train_data[selected_features]

    # ===============================================================
    # SCALING AND TRAINING
    # ===============================================================
    scaler = MinMaxScaler(feature_range=(0, 1))
    train_data_scaled = pd.DataFrame(scaler.fit_transform(train_data),
                                     index=train_data.index,
                                     columns=train_data.columns)

    # logging.info("Scaled training data sample:")
    # logging.info(train_data_scaled.head())

    # Traning models
    myNSA = nsa_train(train_data_scaled, ai_nsa_num_samples_to_lag, ai_nsa_num_samples_to_diff, ai_nsa_num_samples_to_smooth,
                      ai_nsa_percentile_anomaly_score_threshold, ai_nsa_max_t_cells, ai_nsa_max_attempts,
                      ai_nsa_percentage, ai_nsa_algorithm, ai_nsa_n_neighbors) if ai_nsa else None

    myKmeans = kmeans_train(train_data_scaled, ai_kmeans_num_samples_to_lag, ai_kmeans_num_samples_to_diff,
                            ai_kmeans_num_samples_to_smooth, ai_kmeans_percentile_anomaly_score_threshold, ai_kmeans_max_iterations) if ai_kmeans else None

    return train_data.columns, scaler, myNSA, myKmeans

def inference_aiad(ai_nsa, ai_kmeans, columns_with_variability, scaler, myNSA, myKmeans, data_filename, application_name):

    # load configuration
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, 'aiad.ini')
    config = configparser.ConfigParser()
    config.read(config_path)

    ai_graph = config.getboolean('DEFAULT', 'ai_graph')
    anomaly_bit_max = config.getint('DEFAULT', 'anomaly_bit_max')
    ai_kmeans_dim_anomaly_score = config.getfloat('DEFAULT', 'ai_kmeans_dim_anomaly_score')

    if not os.path.exists(data_filename):
        raise FileNotFoundError(f"The file '{data_filename}' does not exist.")
    if os.path.getsize(data_filename) == 0:
        raise ValueError(f"The file '{data_filename}' is empty.")

    # Load test data
    test_data = pd.read_csv(data_filename)
    # logging.info(f'Data {data_filename} loaded.')
    # logging.info(test_data.head())

    test_data.drop(columns=['Timestamp'], inplace=True)
    test_data.set_index('ems_time', inplace=True)

    # convert index to datetime if it is in seconds
    test_data.index = pd.to_datetime(test_data.index, unit='s')

    # reindex at 1-minute frequency and fill NaNs
    test_data = test_data.resample('1min').mean().interpolate(method='linear').fillna(0)

    # convert index to seconds for 'aiad' compatibility
    test_data.index = (test_data.index.astype('int64') // 10**9).astype(int)

    # Select only columns with variability
    test_data = test_data[columns_with_variability]

    # Scaling
    test_data_scaled = pd.DataFrame(scaler.transform(test_data), index=test_data.index, columns=test_data.columns)
    # logging.info("Scaled testing data sample.")
    # logging.info(test_data_scaled.head())

    results = {}

    # NSA
    if ai_nsa:
        df_distance_scores, df_anomaly_bits = nsa_inference(application_name, myNSA, test_data_scaled, anomaly_bit_max)
        df_final = test_data.join(df_distance_scores, how='outer').join(df_anomaly_bits, how='outer')
        df_final = df_final.dropna()
        results["nsa_data"] = df_final
        results["nsa_window_anomaly_rate"] = df_final['anomaly_bit'].sum() * 100 / (df_final.shape[0] * anomaly_bit_max)

    # KMeans
    if ai_kmeans:
        df_anomaly_scores, df_anomaly_bits = kmeans_inference(application_name, myKmeans, test_data_scaled, anomaly_bit_max, ai_kmeans_dim_anomaly_score)
        df_final2 = test_data.join(df_anomaly_scores, how='outer').join(df_anomaly_bits, how='outer')
        df_final2 = df_final2.dropna()
        results["kmeans_data"] = df_final2
        results["kmeans_window_anomaly_rate"] = {dim: df_final2[f"{dim}_anomaly_bit"].sum()*100/(df_final2.shape[0]*anomaly_bit_max) for dim in myKmeans}

    return results
