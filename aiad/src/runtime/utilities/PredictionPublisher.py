#from exn.core.publisher import Publisher
from exn import core
from runtime.operational_status.AiadPredictorState import AiadPredictorState

#class PredictionPublisher(Publisher):
class PredictionPublisher(core.publisher.Publisher):
    metric_name = ""
    def __init__(self,application_name,metric_name):
#        super().__init__('publisher_'+application_name+'-'+metric_name, 'eu.nebulouscloud.preliminary_predicted.aiad.'+metric_name, True,True)
#        super().__init__('publisher_'+application_name+'-'+metric_name, 'eu.nebulouscloud.preliminary_predicted.'+AiadPredictorState.forecaster_name+"."+metric_name, True,True)
        # topic://eu.nebulouscloud.ad.aiad.allmetrics
        super().__init__('publisher_'+application_name+'-'+metric_name, 'eu.nebulouscloud.ad.'+AiadPredictorState.forecaster_name+"."+metric_name, True,True)
        self.metric_name = metric_name

    def send(self, body={}, application=""):
        super(PredictionPublisher, self).send(body, application)
