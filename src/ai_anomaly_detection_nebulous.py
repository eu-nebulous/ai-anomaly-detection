# ### AI anomaly detection - NebulOuS
#
# Ideas / References...
# https://www.netdata.cloud/blog/extending-anomaly-detection-training-window/
# https://blog.netdata.cloud/understanding-linux-cpu-consumption-load-and-pressure-for-performance-optimisation/
# https://www.netdata.cloud/blog/our-approach-to-machine-learning/
# https://docs.google.com/presentation/d/18zkCvU3nKP-Bw_nQZuXTEa4PIVM6wppH3VUnAauq-RU/edit#slide=id.p
# https://github.com/netdata/netdata/discussions/12763
# https://github.com/netdata/netdata/pull/14065
# https://learn.netdata.cloud/docs/deployment-guides/

import logging

# Set logging format
LOGFORMAT = "%(asctime)s,%(name)s [%(levelname)s] %(message)s"
LOGDATEFORMAT = "%Y-%m-%dT%H:%M:%S"
LOGFILENAME = "anomalydetection.log"
logging.basicConfig(filename=LOGFILENAME, filemode='a', level=logging.INFO, format=LOGFORMAT, datefmt=LOGDATEFORMAT)
logger = logging.getLogger(__name__)

# imports functions
from datetime import datetime, timedelta, timezone
import time
import pandas as pd
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from sklearn.cluster import KMeans
from scipy.spatial.distance import cdist
#!pip install netdata-pandas
from src.netdata_pandas.data import get_data_with_parent, get_parentchildren

from src.ml import distance_between_points, tCell, NSA
from sklearn.preprocessing import MinMaxScaler
import joblib
import os
import sys
import random

# Global variables
anomaly_bit_max = 1         # Assigned value to anomaly_bit

# IMPORTANT: There is the following ERROR when you execute the program...
# lib/python3.10/site-packages/anyio/_backends/_trio.py:162: TrioDeprecationWarning: trio.MultiError is deprecated since Trio 0.22.0; use BaseExceptionGroup (on Python 3.11 and later) or exceptiongroup.BaseExceptionGroup (earlier versions) instead (https://github.com/python-trio/trio/issues/2211)
#   class ExceptionGroup(BaseExceptionGroup, trio.MultiError):
# To solve the problem for Python Versions < 3.11
# 1. pip install exceptiongroup
# 2. Locate _trio.py and edit .../anyio/_backends/_trio.py (vi .../venv-jupyter/lib/python3.10/site-packages/anyio/_backends/_trio.py)
# 3. Comment the line... class ExceptionGroup(BaseExceptionGroup, trio.MultiError):
# 4. Add the line... class ExceptionGroup(BaseExceptionGroup):
# Finally... the code will be
# class ExceptionGroup(BaseExceptionGroup):
#    pass
# anyio/_backends/_trio.py
# To solve the problem for Python Versions 3.11+
# 1. pip install exceptiongroup
# 2. Locate _trio.py and edit .../anyio/_backends/_trio.py (vi .../venv-jupyter/lib/python3.10/site-packages/anyio/_backends/_trio.py)
# 3. Add the following lines...
# class ExceptionGroup(BaseExceptionGroup):
#    pass
# END IMPORTANT

# Eliminate outliers using IQR with adjustable multiplier
def remove_outliers_iqr(df):
    outlier_fraction = 100/df.shape[0]
    for column in df.columns:
        Q1 = df[column].quantile(0.25)
        Q3 = df[column].quantile(0.75)
        IQR = Q3 - Q1
        
        # Handle zero IQR
        if IQR == 0:
            continue
            
        # Calculate the appropriate multiplier
        lower_bound = df[column].quantile(outlier_fraction / 2)
        upper_bound = df[column].quantile(1 - outlier_fraction / 2)
        IQR_multiplier = (upper_bound - lower_bound) / IQR
        logger.info(f"column {column} IQR_multiplier {IQR_multiplier}")
        
        # Apply filter with adjusted multiplier
        outlier_filter = (df[column] >= (Q1 - IQR_multiplier * IQR)) & (df[column] <= (Q3 + IQR_multiplier * IQR))
        
        df = df[outlier_filter]

    df = df.dropna()
    return df

# Get raw data (hours)
def get_raw_data(context, TOPIC_NAME, protocol, parent, host, charts, last_n_hours, dims, graph, remove_outliers):
    """
    Retrieve raw data for the last `last_n_hours` hours.

    Parameters:
    host: Host to retrieve data from.
    charts (list): List of charts to retrieve data from.
    last_n_hours (int): The number of hours to look back.
    dims (list): Dimensions for the data retrieval.
    graph (boolean): If want to generate png image files. 
    remove_outliers (bool): Flag to indicate if outliers should be removed.

    Returns:
    pd.DataFrame: A DataFrame containing the retrieved data.
    """

    logger.info(f"Starting get_raw_data for last_n_hours {last_n_hours}")
    
    hours_24 = 24   # DO NOT CHANGE!!!

    # BE CAREFUL if 'last_n_hours' is greater than 24 the sampling is NOT PER SECOND... that is, time_idx of the recovered data is NOT CONSECUTIVE
   
    # based on last_n_hours define the relevant 'before' and 'after' params for the netdata rest api on the agent
    #datetime_utcnow = datetime.utcnow()
    #datetime_utcnow = datetime.now(timezone.utc)
    datetime_utcnow = datetime.now()
    
    print(f"host {host}")
    logger.info(f"host {host}")
    df = None
    i = 0
    while (i * hours_24) < last_n_hours:
        before = int((datetime_utcnow - timedelta(hours=hours_24*(i))).timestamp())
        if ((i+1) * hours_24) <= last_n_hours:
            after = int((datetime_utcnow - timedelta(hours=hours_24*(i+1))).timestamp())
        else:
            after = int((datetime_utcnow - timedelta(hours=last_n_hours)).timestamp())
        df_new = get_data_with_parent(parent=parent, hosts=host, charts=charts, after=after, before=before, protocol=protocol, context=context, TOPIC_NAME=TOPIC_NAME)
        if df_new is None:      # Means... NO more data period (after, before)
            i = last_n_hours
        else:
            if i > 0:
                df = pd.concat([df_new, df])
            else:
                df = df_new
        i += 1

    # columns_set = set(df.columns)
    # dims_set = set(dims)

    # # Obtener la intersección
    # intersection = columns_set.intersection(dims_set)

    # # Convertir la intersección a una lista si es necesario
    # intersection_list = list(intersection)

    # print('intersection_list')
    # print(intersection_list)
    # dims = intersection_list
    
    # filter df for just the dims if set
    if len(dims):
        df = df[[dim for dim in dims]]

    # removes rows containing null values (NaN)
    df = df.dropna()
    
    if remove_outliers:

        if graph:
            # lets just plot each dimension to have a look at it
            for col in df.columns:   
                title = f'01. {col} before deleting outliers'
                data = df[[col]].set_index(pd.to_datetime(df.index, unit='s'))
                # plot and set title
                data.plot(title=title, figsize=(16, 6))
                # save the image
                plt.savefig('src/images/' + title.replace('|', '.') + '.png', dpi=1200)
                # close the figure to avoid overloading memory
                plt.close()

        df = remove_outliers_iqr(df)

        if graph:
            # lets just plot each dimension to have a look at it
            for col in df.columns:   
                title = f'01. {col} after deleting outliers'
                data = df[[col]].set_index(pd.to_datetime(df.index, unit='s'))
                # plot and set title
                data.plot(title=title, figsize=(16, 6))
                # save the image
                plt.savefig('src/images/' + title.replace('|', '.') + '.png', dpi=1200)
                # close the figure to avoid overloading memory
                plt.close()

    else:
    
        if graph:
            # lets just plot each dimension to have a look at it
            for col in df.columns:
                title = f'02. {col} {datetime_utcnow.strftime("%Y-%m-%d %H%M%S")}'
                #title = f'02. {col}'
                data = df[[col]].set_index(pd.to_datetime(df.index, unit='s'))
                # plot and set title
                data.plot(title=title, figsize=(16, 6))
                # save the image
                plt.savefig('src/images/' + title.replace('|', '.') + '.png', dpi=1200)
                # close the figure to avoid overloading memory
                plt.close()
    
    logger.info(f"Finishing get_raw_data for last_n_hours {last_n_hours}")
    return df

# Get raw data (minutes)
def get_raw_data_minutes(context, TOPIC_NAME, protocol, parent, host, charts, last_n_minutes, dims, remove_outliers):
    """
    Retrieve raw data for the last `last_n_minutes` minutes, in chunks of 1440 minutes (24 hours).

    Parameters:
    host: Host to retrieve data from.
    charts (list): List of charts to retrieve data from.
    last_n_minutes (int): The number of minutes to look back.
    dims (list): Dimensions for the data retrieval.
    remove_outliers (bool): Flag to indicate if outliers should be removed.

    Returns:
    pd.DataFrame: A DataFrame containing the retrieved data.
    """
    
    minutes_1440 = 1440   # 24 hours in minutes, DO NOT CHANGE!!!

    datetime_utcnow = datetime.now(timezone.utc)
    df = pd.DataFrame()

    before = int(datetime_utcnow.timestamp())
    after = int((datetime_utcnow - timedelta(minutes=min(minutes_1440, last_n_minutes))).timestamp())
    
    df = get_data_with_parent(parent=parent, hosts=host, charts=charts, after=after, before=before, protocol=protocol, context=context, TOPIC_NAME=TOPIC_NAME)        
    
    # filter df for just the dims if set
    if len(dims):
        df = df[[dim for dim in dims]]    

    # removes rows containing null values (NaN)
    df = df.dropna()

    return df
 
# helper functions
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
    max_t_cells = 500, max_attempts = 10, percentage = 0.05, algorithm = 2, n_neighbors = 5):
    
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
    
def nsa_inference(myNSA, df, graph):
    
    dims = df.columns

    anomaly_radio_threshold = float(myNSA.self_radio)    # or np.sqrt(myNSA.dimension) * myNSA.percentage
    print(f"anomaly_radio_threshold {anomaly_radio_threshold}")

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

    #logger.info(f"rows df {df.shape[0]} df_timestamp_min {df.index.min()} df_timestamp_max {df.index.max()}")
    
    row_number = 0
    buffer_size = myNSA.num_samples_to_lag * 2 + myNSA.num_samples_to_diff + myNSA.num_samples_to_smooth

    for t, row in df.iterrows():

        if row_number >= buffer_size:

            #################################
            # Inference / Scoring
            #################################

            # get a buffer of recent data
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
                if myNSA.min_distance_self >= anomaly_radio_threshold:
                    print('ajusto el anomaly bit *************************************** paula')
                    print(f'myNSA.min_distance_self {myNSA.min_distance_self} anomaly_radio_threshold {anomaly_radio_threshold}')
                    logger.info('ajusto el anomaly bit *************************************** paula')
                    logger.info(f'myNSA.min_distance_self {myNSA.min_distance_self} anomaly_radio_threshold {anomaly_radio_threshold}')
            
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

    # join distance scores to raw df
    df_final = df.join(df_distance_scores, how='outer')

    #join anomaly bits to raw df
    df_final = df_final.join(df_anomaly_bits, how='outer')
    
    df_final = df_final.dropna()

    #logger.info(f'NSA df_final = {df_final}')

    if graph:
        figsize = (16,6)

        # PCF el eje y escala mal
        title = '03.NSA Normalized Raw Data to Predict'
        ax = plt.gca()
        data = df_final[dims].set_index(pd.to_datetime(df_final.index, unit='s'))
        data.plot(title=title, figsize=figsize)
        plt.savefig('src/images/' + title.replace('|', '.') + '.png', dpi=1200)
        plt.close()
            
        title = '03.NSA Distance Score'
        ax = plt.gca()
        data = df_final[['distance_score']].set_index(pd.to_datetime(df_final.index, unit='s'))
        data.plot(title=title, figsize=figsize)
        plt.savefig('src/images/' + title.replace('|', '.') + '.png', dpi=1200)
        plt.close()
            
        title = '03.NSA Anomaly Bit'
        ax = plt.gca()
        data = df_final[['anomaly_bit']].set_index(pd.to_datetime(df_final.index, unit='s'))
        data.plot(title=title, figsize=figsize)
        plt.savefig('src/images/' + title.replace('|', '.') + '.png', dpi=1200)
        plt.close()

        title = '03.NSA (Distance Score, Anomaly Bit)'
        ax = plt.gca()
        data = df_final[['distance_score', 'anomaly_bit']].set_index(pd.to_datetime(df_final.index, unit='s'))
        data.plot(title=title, figsize=figsize)
        plt.savefig('src/images/' + title.replace('|', '.') + '.png', dpi=1200)
        plt.close()

        title = '03.NSA Combined (Normalized Raw, Score, Bit)'
        ax = plt.gca()
        #df_final_normalized = (df_final-df_final.min())/(df_final.max()-df_final.min())
        #data = df_final_normalized.set_index(pd.to_datetime(df_final_normalized.index, unit='s'))
        data = df_final.set_index(pd.to_datetime(df_final.index, unit='s'))
        data.plot(title=title, figsize=figsize)
        plt.savefig('src/images/' + title.replace('|', '.') + '.png', dpi=1200)
        plt.close()

    return df_final

def kmeans_train(df, num_samples_to_lag, num_samples_to_diff, num_samples_to_smooth, max_iterations = 100):
    
    #################################
    # Train / Re-Train
    #################################
 
    n_clusters_per_dimension = 2
    #max_iterations = 100    
    
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
    
    return models

def kmeans_inference(models, df, graph, ai_kmeans_dim_anomaly_score):

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
                #logger.info(f'X {X}')
                # print('cluster_centers')
                # print(cluster_centers)
                #logger.info(f'cluster_centers {cluster_centers}')
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
                #logger.info(f'dim {dim} train_raw_anomaly_score_min {train_raw_anomaly_score_min} train_raw_anomaly_score_max {train_raw_anomaly_score_max}')
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

                #logger.info(f'anomaly_score {anomaly_score}')
                #logger.info(f'anomaly_bit {anomaly_bit}')
                
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

    # join anomaly scores to raw df
    df_final = df.join(df_anomaly_scores, how='outer')

    # join anomaly bits to raw df
    df_final = df_final.join(df_anomaly_bits, how='outer')

    df_final = df_final.dropna()

    #logger.info(f'k-means df_final = {df_final}')

    if graph:

        figsize = (16,6)

        for dim in models:

            # create a dim with the raw data, anomaly score and anomaly bit for the dim
            df_final_dim = df_final[[dim,f'{dim}_anomaly_score',f'{dim}_anomaly_bit']]
            
            title = f'04.k-means Normalized Raw Data to Predict - {dim}'
            ax = plt.gca()
            data = df_final_dim[[dim]].set_index(pd.to_datetime(df_final_dim.index, unit='s'))
            data.plot(title=title, figsize=figsize)
            plt.savefig('src/images/' + title.replace('|', '.') + '.png', dpi=1200)
            plt.close()
            
            title = f'04.k-means Anomaly Score - {dim}'
            ax = plt.gca()
            data = df_final_dim[[f'{dim}_anomaly_score']].set_index(pd.to_datetime(df_final_dim.index, unit='s'))
            data.plot(title=title, figsize=figsize)
            plt.savefig('src/images/' + title.replace('|', '.') + '.png', dpi=1200)
            plt.close()

            title = f'04.k-means Anomaly Bit - {dim}'
            ax = plt.gca()
            data = df_final_dim[[f'{dim}_anomaly_bit']].set_index(pd.to_datetime(df_final_dim.index, unit='s'))
            data.plot(title=title, figsize=figsize)
            plt.savefig('src/images/' + title.replace('|', '.') + '.png', dpi=1200)
            plt.close()

            title = f'04.k-means Combined (Normalized Raw, Score, Bit) - {dim}'
            ax = plt.gca()
            #df_final_dim_normalized = (df_final_dim-df_final_dim.min())/(df_final_dim.max()-df_final_dim.min())
            #data = df_final_dim_normalized.set_index(pd.to_datetime(df_final_dim_normalized.index, unit='s'))
            data = df_final_dim.set_index(pd.to_datetime(df_final_dim.index, unit='s'))
            data.plot(title=title, figsize=figsize)
            plt.savefig('src/images/' + title.replace('|', '.') + '.png', dpi=1200)
            plt.close()

    for dim in models:
        
        # create a dim with the raw data, anomaly score and anomaly bit for the dim
        df_final_dim = df_final[[dim,f'{dim}_anomaly_score',f'{dim}_anomaly_bit']]

        window_anomaly_rate = df_final_dim[f'{dim}_anomaly_bit'].sum() * 100 / (df_final_dim.shape[0] * anomaly_bit_max)

        print(f'\n\n******** kmeans - Dimension {dim}')
        print(f'window_anomaly_rate = {window_anomaly_rate}%')
        print(f'This means the "anomaly rate" within the last period ({df_final_dim.index.min()}, {df_final_dim.index.max()}) was {window_anomaly_rate}%')
        print(f'Another way to think of this is that {window_anomaly_rate}% of the observations during the last window were considered anomalous based on the latest trained model.')
        logger.info(f'\n\n******** kmeans - Dimension {dim}')
        logger.info(f'window_anomaly_rate = {window_anomaly_rate}%')
        logger.info(f'This means the "anomaly rate" within the last period ({df_final_dim.index.min()}, {df_final_dim.index.max()}) was {window_anomaly_rate}%')
        logger.info(f'Another way to think of this is that {window_anomaly_rate}% of the observations during the last window were considered anomalous based on the latest trained model.')

    return df_final

class Args:
    def __init__(self):        
        self.last_n_hours = 145                 # Last n hours of data considered to train the models. 145 hours = 24 hours * 6 days + 1 hour
        self.last_n_hours_test = False
        self.last_n_minutes_test = 10           # Last n minutes of data considered to test the models
        self.train = True
        self.kmeans = True
        self.nsa = True
        self.graph = False
        self.interval = 60                      # Seconds to wait between Netdata data requests
        self.iterations = 50
        self.ai_nsa_anomaly_rate = 0.10
        self.ai_kmeans_anomaly_rate = 0.10
        self.ai_nsa_kmeans_anomaly_rate = 0.10

def Main_Loop(context, TOPIC_NAME,
        ai_last_n_hours, ai_last_n_hours_test, ai_last_n_minutes_test, ai_train, ai_kmeans, ai_nsa, 
        ai_graph, ai_protocol, ai_ip, ai_port, ai_parent_node, ai_charts, ai_dims, ai_interval, ai_iterations,
        ai_nsa_num_samples_to_lag, ai_nsa_num_samples_to_diff, ai_nsa_num_samples_to_smooth, 
        ai_nsa_max_t_cells, ai_nsa_max_attempts, ai_nsa_percentage, ai_nsa_algorithm, ai_nsa_n_neighbors,
        ai_kmeans_num_samples_to_lag, ai_kmeans_num_samples_to_diff, ai_kmeans_num_samples_to_smooth,
        ai_nsa_anomaly_rate, ai_kmeans_anomaly_rate, ai_nsa_kmeans_anomaly_rate, ai_kmeans_max_iterations,
        ai_kmeans_dim_anomaly_score):
    
    print('Starting main loop...')
    
    # ### Inputs & Parameters

    args = Args()
    args.last_n_hours = ai_last_n_hours
    args.last_n_hours_test = ai_last_n_hours_test
    args.last_n_minutes_test = ai_last_n_minutes_test
    args.train = ai_train
    args.kmeans = ai_kmeans
    args.nsa = ai_nsa
    args.graph = ai_graph

    # data params
    parent = f'{ai_ip}:{ai_port}'
    parentchildren = get_parentchildren(parent, ai_protocol)  
    hosts = parentchildren
    if (not ai_parent_node) and (len(parentchildren) > 1):
        hosts.pop(0)
        
    #hosts = ['toronto.netdata.rocks']
    
    # # Choose 3 children
    # if len(hosts) >= 3:
        # hosts = random.sample(hosts, 3)
        
    print(f'parent {parent}')
    logger.info(f'parent {parent}')
    print(f'hosts {hosts}')
    logger.info(f'hosts {hosts}')
   
    charts = ai_charts    
    dims = ai_dims
    args.interval = ai_interval
    args.iterations = ai_iterations
    
    # anomaly rates  
    args.nsa_anomaly_rate = ai_nsa_anomaly_rate
    args.kmeans_anomaly_rate = ai_kmeans_anomaly_rate
    args.nsa_kmeans_anomaly_rate = ai_nsa_kmeans_anomaly_rate

    print(f"args.__dict__ {args.__dict__}")
    logger.info(f"args.__dict__ {args.__dict__}")

    info_hosts = {
        host: {
            'scaler' : None,
            'myNSA': None,
            'myKmeans': None
        } for host in hosts
    }

    if args.graph:
        if not os.path.exists('src/images'):
            os.makedirs('src/images')
        # Set the chunksize parameter to a higher value
        mpl.rcParams['agg.path.chunksize'] = 10000
  
    everything_ok = True

    while everything_ok:
        
        problem_counter = 0

        myNSA = None
        myKmeans = None
       
        print(f'\n\nTraining... every {args.iterations} iterations.')
        logger.info(f'\n\nTraining... every {args.iterations} iterations.')
        print(f'train: {args.train}')
        logger.info(f'train: {args.train}')
        if args.train:
        
            for host in hosts:
                print(f"Creating the models... host {host}")
                logger.info(f"Creating the models... host {host}")
                df = get_raw_data(context, TOPIC_NAME, ai_protocol, parent, host, charts, args.last_n_hours, dims, args.graph, remove_outliers=True)    # Default 145 = 6 * hours_24 + 1

                scaler_min = 0
                scaler_max = 1   
                scaler = MinMaxScaler(feature_range=(scaler_min, scaler_max))
                scaler.fit(df)
                # saving the scaler model
                joblib.dump(scaler, f'scaler_model_{host.replace(":", "_")}.pkl')
                df_scaled = scaler.transform(df)        
                df = pd.DataFrame(df_scaled, index=df.index, columns=df.columns)
                info_hosts[host]['scaler'] = scaler

                if args.nsa:
                
                    print(f"Creating the model NSA... host {host}")
                    logger.info(f"Creating the model NSA... host {host}")
                    myNSA = nsa_train(df, ai_nsa_num_samples_to_lag, ai_nsa_num_samples_to_diff, ai_nsa_num_samples_to_smooth, 
                        ai_nsa_max_t_cells, ai_nsa_max_attempts, ai_nsa_percentage, ai_nsa_algorithm, ai_nsa_n_neighbors)
                    joblib.dump(myNSA, f'nsa_model_{host.replace(":", "_")}.pkl')
                    info_hosts[host]['myNSA'] = myNSA
                    
                if args.kmeans:
                
                    print(f"Creating the model kmeans... host {host}")
                    logger.info(f"Creating the model kmeans... host {host}")
                    myKmeans = kmeans_train(df, ai_kmeans_num_samples_to_lag, ai_kmeans_num_samples_to_diff, ai_kmeans_num_samples_to_smooth,
                        ai_kmeans_max_iterations)
                    joblib.dump(myKmeans, f'kmeans_models_{host.replace(":", "_")}.pkl')
                    info_hosts[host]['myKmeans'] = myKmeans
            
        else:
            
            for host in hosts:
                if os.path.exists(f'scaler_model_{host.replace(":", "_")}.pkl'):
                    print(f"Loading scaler model... host {host}")
                    logger.info(f"Loading scaler model... host {host}")
                    scaler = joblib.load(f'scaler_model_{host.replace(":", "_")}.pkl')
                    info_hosts[host]['scaler'] = scaler

                    if args.nsa:
                        if os.path.exists(f'nsa_model_{host.replace(":", "_")}.pkl'):
                            logger.info(f'Loading nsa model... host {host}')
                            myNSA = joblib.load(f'nsa_model_{host.replace(":", "_")}.pkl')
                            info_hosts[host]['myNSA'] = myNSA
                        else:
                            logger.info(f'Error. The file nsa_model_{host.replace(":", "_")}.pkl does not exist. Please train the nsa algorithm before performing inferences.')
                            print(f'Error. The file nsa_model_{host.replace(":", "_")}.pkl does not exist. Please train the nsa algorithm before performing inferences.')
                            everything_ok = False
                        
                    if args.kmeans:
                        if os.path.exists(f'kmeans_models_{host.replace(":", "_")}.pkl'):
                            logger.info(f'Loading kmeans models... host {host}')
                            myKmeans = joblib.load(f'kmeans_models_{host.replace(":", "_")}.pkl')
                            info_hosts[host]['myKmeans'] = myKmeans
                        else:
                            logger.info(f'Error. The file kmeans_models_{host.replace(":", "_")}.pkl does not exist. Please train the kmeans algorithm before performing inferences.')
                            print(f'Error. The file kmeans_models_{host.replace(":", "_")}.pkl does not exist. Please train the kmeans algorithm before performing inferences.')
                            everything_ok = False
                else:
                    logger.info(f'Error. The file scaler_model_{host.replace(":", "_")}.pkl does not exist. Please train an algorithm before performing inferences.')
                    print(f'Error. The file scaler_model_{host.replace(":", "_")}.pkl does not exist. Please train an algorithm before performing inferences.')
                    everything_ok = False

        #while True:
        jj = 1
        while (jj <= args.iterations and everything_ok) or not args.train:
        #while everything_ok:

            logger.info(f"\n\n ***** Iteration No. {jj} *****")
            print(f"\n\n ***** Iteration No. {jj} *****")
            
            anomaly_data1 = {}
            anomaly_data2 = {}
            anomaly_data3 = {}

            if args.last_n_hours_test or args.last_n_minutes_test:
                
                # Wait a while before getting more data (optional)
                if jj > 1 or not args.train: 
                    time.sleep(args.interval)                

                for host in hosts:
            
                    if args.last_n_hours_test and args.last_n_hours_test <= 24:
                    
                        # retrieve last hours to predict
                        df = get_raw_data(context, TOPIC_NAME, ai_protocol, parent, host, charts, args.last_n_hours_test, dims, args.graph, remove_outliers=False)
                    
                    else:
                    
                        if args.last_n_minutes_test and args.last_n_minutes_test <= 1440:    # 24 hours in minutes
                        
                            # retrieve last minutes to predict
                            df = get_raw_data_minutes(context, TOPIC_NAME, ai_protocol, parent, host, charts, args.last_n_minutes_test, dims, remove_outliers=False)

                        else:
                            logger.info(f"Error. 'last_n_hours_test' should be at most 24 or 'last_n_minutes_test' should be at most 1440. Both parameters cannot be False.")
                            print(f"Error. 'last_n_hours_test' should be at most 24 or 'last_n_minutes_test' should be at most 1440. Both parameters cannot be False.")
                            everything_ok = False

                    if everything_ok:

                        scaler = info_hosts[host]['scaler']
                        df_scaled = scaler.transform(df)        
                        df = pd.DataFrame(df_scaled, index=df.index, columns=df.columns)            

                        if args.nsa:

                            myNSA = info_hosts[host]['myNSA']
                            df_final = nsa_inference(myNSA, df, args.graph)
                            
                            # window_anomaly_rate to give an alarm
                            window_anomaly_rate = df_final['anomaly_bit'].sum() * 100 / (df_final.shape[0] * anomaly_bit_max)

                            print(f'\n\n******** NSA (anomaly_bit)')
                            print(f'window_anomaly_rate = {window_anomaly_rate}%')
                            print(f'This means the "anomaly rate" within the last period ({df_final.index.min()}, {df_final.index.max()}) was {window_anomaly_rate}%')
                            print(f'Another way to think of this is that {window_anomaly_rate}% of the observations during the last window were considered anomalous based on the latest trained model.')
                            logger.info(f'\n\n******** NSA (anomaly_bit)')
                            logger.info(f'window_anomaly_rate = {window_anomaly_rate}%')
                            logger.info(f'This means the "anomaly rate" within the last period ({df_final.index.min()}, {df_final.index.max()}) was {window_anomaly_rate}%')
                            logger.info(f'This means the "anomaly rate" within the last period was {window_anomaly_rate}%')
                            logger.info(f'Another way to think of this is that {window_anomaly_rate}% of the observations during the last window were considered anomalous based on the latest trained model.')
                            
                            if window_anomaly_rate > args.nsa_anomaly_rate:
                                print('+++++++++++++++ NSA ALERT +++++++++++++++')
                                logger.info('+++++++++++++++ NSA ALERT +++++++++++++++')
                                anomaly_data1 = {
                                    "anomaly": {
                                        "algorithm": "nsa",
                                        "window_anomaly_rate": window_anomaly_rate,
                                        "window_start": df_final.index.min(),
                                        "window_end": df_final.index.max(),
                                        "iteration": jj,
                                        "data": df_final
                                    }
                                }                

                        if args.kmeans:

                            myKmeans = info_hosts[host]['myKmeans']
                            df_final2 = kmeans_inference(myKmeans, df, args.graph, ai_kmeans_dim_anomaly_score)
                            
                            # Select columns whose name contains "_anomaly_bit" and then rows where value != 0 for all
                            anomaly_bit_columns = [col for col in df_final2.columns if "_anomaly_bit" in col]            
                            filtered_df_final = df_final2[(df_final2[anomaly_bit_columns] != 0).all(axis=1)]
                            window_anomaly_rate = filtered_df_final.shape[0] * 100 / (df_final2.shape[0] * anomaly_bit_max)
                            
                            print(f'\n\n******** k-means (_anomaly_bit)')
                            print(f'window_anomaly_rate = {window_anomaly_rate}%')
                            print(f'This means the "anomaly rate" within the last period ({df_final2.index.min()}, {df_final2.index.max()}) was {window_anomaly_rate}%')
                            print(f'Another way to think of this is that {window_anomaly_rate}% of the observations during the last window were considered anomalous based on the latest trained model.')
                            logger.info(f'\n\n******** k-means (_anomaly_bit)')
                            logger.info(f'window_anomaly_rate = {window_anomaly_rate}%')
                            logger.info(f'This means the "anomaly rate" within the last period ({df_final2.index.min()}, {df_final2.index.max()}) was {window_anomaly_rate}%')
                            logger.info(f'This means the "anomaly rate" within the last period was {window_anomaly_rate}%')
                            logger.info(f'Another way to think of this is that {window_anomaly_rate}% of the observations during the last window were considered anomalous based on the latest trained model.')

                            if window_anomaly_rate > args.kmeans_anomaly_rate:
                                print('+++++++++++++++ K-Means ALERT +++++++++++++++')
                                logger.info('+++++++++++++++ K-Means ALERT +++++++++++++++')
                                anomaly_data2 = {
                                    "anomaly": {
                                        "algorithm": "kmeans",
                                        "window_anomaly_rate": window_anomaly_rate,
                                        "window_start": df_final2.index.min(),
                                        "window_end": df_final2.index.max(),
                                        "iteration": jj,
                                        "data": df_final2
                                    }
                                }

                            if args.nsa:
                                columns = df_final.columns           
                                for column in df_final2.columns:
                                    if column not in columns:
                                        df_final = df_final.join(df_final2[column], how='outer')
                                df_final = df_final.dropna()
                            else:
                                df_final = df_final2

                        if args.nsa and args.kmeans:

                            # Select columns whose name contains "anomaly_bit" and then rows where value != 0 for all
                            anomaly_bit_columns = [col for col in df_final.columns if "anomaly_bit" in col]
                            filtered_df_final = df_final[(df_final[anomaly_bit_columns] != 0).all(axis=1)]
                            window_anomaly_rate = filtered_df_final.shape[0] * 100 / (df_final.shape[0] * anomaly_bit_max)
                            
                            print(f'\n\n******** NSA & k-means (anomaly_bit & _anomaly_bit)')
                            print(f'window_anomaly_rate = {window_anomaly_rate}%')
                            print(f'This means the "anomaly rate" within the last period ({df_final.index.min()}, {df_final.index.max()}) was {window_anomaly_rate}%')
                            print(f'Another way to think of this is that {window_anomaly_rate}% of the observations during the last window were considered anomalous based on the latest trained model.')
                            logger.info(f'\n\n******** NSA & k-means (anomaly_bit & _anomaly_bit)')
                            logger.info(f'window_anomaly_rate = {window_anomaly_rate}%')
                            logger.info(f'This means the "anomaly rate" within the last period ({df_final.index.min()}, {df_final.index.max()}) was {window_anomaly_rate}%')
                            logger.info(f'This means the "anomaly rate" within the last period was {window_anomaly_rate}%')
                            logger.info(f'Another way to think of this is that {window_anomaly_rate}% of the observations during the last window were considered anomalous based on the latest trained model.')

                            if window_anomaly_rate > args.nsa_kmeans_anomaly_rate:
                                print('+++++++++++++++ NSA & K-Means ALERT +++++++++++++++')
                                logger.info('+++++++++++++++ NSA & K-Means ALERT +++++++++++++++')
                                anomaly_data3 = {
                                    "anomaly": {
                                        "algorithm": "nsa_and_kmeans",
                                        "window_anomaly_rate": window_anomaly_rate,
                                        "window_start": df_final.index.min(),
                                        "window_end": df_final.index.max(),                                
                                        "iteration": jj,
                                        "data": df_final
                                    }
                                }

                        if anomaly_data1:
                            if context.has_publisher(TOPIC_NAME):
                                problem_counter = problem_counter + 1
                                logger.info('  nsa -> Sending anomally report #' + str(problem_counter))
                                print('  nsa -> Sending anomally report #' + str(problem_counter))
                                context.publishers[TOPIC_NAME].send(anomaly_data1)
                        if anomaly_data2:
                            if context.has_publisher(TOPIC_NAME):
                                problem_counter = problem_counter + 1
                                logger.info('  k-means -> Sending anomally report #' + str(problem_counter))
                                print('  k-means -> Sending anomally report #' + str(problem_counter))
                                context.publishers[TOPIC_NAME].send(anomaly_data2)
                        if anomaly_data3:
                            if context.has_publisher(TOPIC_NAME):
                                problem_counter = problem_counter + 1
                                logger.info('  nsa + k-means -> Sending anomally report #' + str(problem_counter))
                                print('  nsa + k-means -> Sending anomally report #' + str(problem_counter))
                                context.publishers[TOPIC_NAME].send(anomaly_data3)

            else:
                
                logger.info(f"Error. Please provide one of the following parameter... last_n_hours_test or last_n_minutes_test")
                print(f"Error. Please provide one of the following parameter... last_n_hours_test or last_n_minutes_test")
                
                everything_ok = False
                
            sys.stdout.flush()

            jj += 1
