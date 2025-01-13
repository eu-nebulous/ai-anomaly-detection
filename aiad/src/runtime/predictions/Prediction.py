# Copyright (c) 2023 Institute of Communication and Computer Systems
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.        

class Prediction():
    value = None
    lower_confidence_interval_value = None
    upper_confidence_interval_value = None
    prediction_valid=False
    #Error metrics
    mae = None
    mse = None
    mape = None
    smape = None

    def __init__(self,value,confidence_interval_tuple,prediction_valid,prediction_mae,prediction_mse,prediction_mape,prediction_smape):
        self.value = value
        self.lower_confidence_interval_value,self.upper_confidence_interval_value = map(float,confidence_interval_tuple.split(","))
        self.prediction_valid = prediction_valid
        self.mae = prediction_mae
        self.mse = prediction_mse
        self.mape = prediction_mape
        self.smape = prediction_smape

    def set_last_prediction_time_needed(self,prediction_time_needed):
        self.last_prediction_time_needed = prediction_time_needed

    def get_error_metrics_string(self):
        return self.mae+";"+self.mse+";"+self.mape+";"+self.smape