# ### AI anomaly detection - NebulOuS
# Negative Selection Algorithm (NSA) class
import numpy as np
from sklearn.neighbors import KNeighborsClassifier
from sklearn.neighbors import KDTree
from sklearn.neighbors import BallTree
from sklearn.neighbors import KernelDensity
import logging

# Set logging format
LOGFORMAT = "%(asctime)-15s,%(name)s [%(levelname)s] %(message)s"
LOGDATEFORMAT = "%Y-%m-%dT%H:%M:%S"
LOGFILENAME = "anomalydetection.log"
logging.basicConfig(filename=LOGFILENAME, filemode='a', level=logging.INFO, format=LOGFORMAT, datefmt=LOGDATEFORMAT)
logger = logging.getLogger(__name__)

def node_bounds_without_duplicates(arrays):
    arrays_without_dup = []
    arrays_set = set()
    for array in arrays:
        # Convert the array into a tuple to make it hashable
        array_tuple = tuple(array)
        if array_tuple not in arrays_set:
            arrays_without_dup.append(array)
            arrays_set.add(array_tuple)
    return np.array(arrays_without_dup)

# Obtain the distance between two datapoints (Euclidean norm)
def distance_between_points(datapoint1, datapoint2):
    return np.linalg.norm(datapoint1 - datapoint2)

class tCell:
    def __init__(self, center, radio):
        self.tcell_center = center
        self.tcell_radio = radio
        self.count = 0

    # Obtain the distance between a datapoint and a detector's center (Euclidian norm) and then check if the distance is less than a radio value
    def match(self, datapoint):
        distance = self.obtain_distance(datapoint)
        return distance < self.tcell_radio

    # Obtain the distance between a datapoint and a detector's center (Euclidian norm)
    def obtain_distance(self, datapoint):
        distance = np.linalg.norm(datapoint - self.tcell_center)
        return distance      
  
class NSA:
    def __init__(self, max_t_cells, max_attempts, dimension, percentage, algorithm = 2, n_neighbors = 3, num_samples_to_lag = 5, num_samples_to_diff = 1, num_samples_to_smooth = 3):
        self.t_cells = []
        self.max_t_cells = max_t_cells
        self.max_attempts = max_attempts
        self.dimension = dimension
        self.percentage = percentage
        self.self_radio = np.sqrt(self.dimension) * self.percentage
        self.anomaly_radio_threshold = np.sqrt(self.dimension) * self.percentage
        self.algorithm = algorithm
        self.n_neighbors = n_neighbors
        self.algor_0 =  None
        self.algor_01 =  None
        self.labels_sample = None
        self.min_distance_self = None
        self.num_samples_to_lag = num_samples_to_lag
        self.num_samples_to_diff = num_samples_to_diff
        self.num_samples_to_smooth = num_samples_to_smooth
       
    def fit(self, data_sample, labels_sample):
        self_data = data_sample[labels_sample == 0]
        self_labels = labels_sample[labels_sample == 0]
        
        if self.algorithm == 1:
            # KNeighborsClassifier
            self.algor_0 = KNeighborsClassifier(n_neighbors=1)
            self.algor_0.fit(self_data, self_labels)
            self.algor_01 = KNeighborsClassifier(n_neighbors=int(self.n_neighbors))
            self.algor_01.fit(data_sample, labels_sample)
        elif self.algorithm == 2:
            # KDTree
            self.algor_0 = KDTree(self_data)
            self.algor_01 = KDTree(data_sample)
        elif self.algorithm == 3:
            # BallTree
            self.algor_0 = BallTree(self_data)
            self.algor_01 = BallTree(data_sample)
        self.labels_sample = labels_sample

        if self.algorithm == 1:
            centers = np.random.uniform(size = (self.max_t_cells, self.dimension), low = 0 - self.self_radio, high = 1 + self.self_radio)
            
        elif self.algorithm == 2 or self.algorithm == 3:
            # data: original data points or observations 
            # index: this array represents the indices of the data points in the original dataset
            # node_data: lower index, upper index, on which feature the partition is made and on what value it is split
            # node_bounds: this array stores the bounding boxes or regions associated with each node in the tree
            data, index, node_data, node_bounds = self.algor_0.get_arrays()

            # Centers
            centers = node_bounds_without_duplicates(node_bounds[0])
            
            # IMPORTANT: all feature is normalized to [0, 1]
            centers = np.concatenate((centers, np.random.uniform(size = (self.max_t_cells, self.dimension), low = 0 - self.self_radio, high = 1 + self.self_radio)))
        
        ii = 0
        while ii < len(centers) and len(self.t_cells) < self.max_t_cells:
            if ii % 100 == 0:
                print('fit... iteration number: ', ii)

            new_max_radio = 0
            attempts = 0
            while ii < len(centers) and len(self.t_cells) < self.max_t_cells:
                
                # Find minimal distance between new points and self data... then it is the new max radio
                #new_max_radio = min(distance_between_points(centers[ii], datapoint) - self.self_radio for datapoint in self_data)
                
                if self.algorithm == 1:
                    # KNeighborsClassifier
                    distances, _ = self.algor_0.kneighbors([centers[ii]])
                elif self.algorithm == 2:
                    # KDTree
                    distances, _ = self.algor_0.query([centers[ii]], k=1)
                elif self.algorithm == 3:
                    # BallTree
                    distances, _ = self.algor_0.query([centers[ii]], k=1)
                new_max_radio = distances.min() - self.self_radio
                
                # Verify new radio is greater than 0
                if new_max_radio > 0:
                    break  # The new point meets the conditions, we end the loop
                
                #print(f"NOT APPEND ********** fit... iteration number: {ii}")
                #logger.info(f"NOT APPEND ********** fit... iteration number: {ii}")
                
                # percentage... defines the size of the distortion
                # Generate random values for the coordinates to distort and applies the distortion to a specific center (centers[ii])
                centers[ii] += np.random.uniform(low=-self.percentage, high=self.percentage, size=self.dimension)
                
                if attempts < self.max_attempts:
                    attempts += 1
                else:
                    attempts = 0
                    ii += 1
                
            if new_max_radio > 0:
                new_t_cell = tCell(centers[ii], new_max_radio)
                self.t_cells.append(new_t_cell)

            ii += 1
            
        #print(f"*** len self.t_cells: {len(self.t_cells)}")
        logger.info(f"*** len self.t_cells: {len(self.t_cells)}")
            
        return

    def detect(self, datapoint):
        if self.algorithm == 1:
            # KNeighborsClassifier
            distances, _ = self.algor_0.kneighbors([datapoint])
        elif self.algorithm == 2:
            # KDTree
            distances, _ = self.algor_0.query([datapoint], k=1)
        elif self.algorithm == 3:
            # BallTree
            distances, _ = self.algor_0.query([datapoint], k=1)
        self.min_distance_self = distances.min()    
    
        if any(t_cell.match(datapoint) for t_cell in self.t_cells):
            # False positives - label: 0 pred: 1 - We have to reduce the number!!!
            # True positives - OK
            return True
        else:
            # Euclidean norm... np.sqrt(np.sum(np.square(datapoint))) 
            # Largest point with its radius... np.sqrt(self.dimension * np.square(1 + self.self_radio))
            # If the Euclidean norm of a new point exceeds the largest point with its radius, it will be considered anomalous
            if np.sqrt(np.sum(np.square(datapoint))) > np.sqrt(self.dimension * np.square(1 + self.self_radio)):
                return True
            else:
                # False negatives - label: 1 pred: 0 - We have to reduce the number!!!
                # True negatives - OK

                # Find the k nearest neighbors and use their class as a prediction
                if self.algorithm == 1:
                    # KNeighborsClassifier
                    distances, indices = self.algor_01.kneighbors([datapoint])
                elif self.algorithm == 2:               
                    # KDTree
                    distances, indices = self.algor_01.query([datapoint], k=int(self.n_neighbors))
                elif self.algorithm == 3:
                    # BallTree
                    distances, indices = self.algor_01.query([datapoint], k=int(self.n_neighbors))

                # Get the labels of the k nearest neighbors
                count_true = 0
                count_false = 0
                for ii, idx in enumerate(indices[0]):
                    unique_distances = set(distances[0])
                    if ii == 0 and len(unique_distances) > 1:
                        # If the closest is normal/self return False(normal) and do not analyze further
                        if (self.self_radio - distances[0][ii] >= 0) and self.labels_sample.iloc[idx] == 0:
                            return False
                    if self.labels_sample.iloc[idx] == 1:
                        count_true += 1
                    else:
                        count_false += 1
                if count_true >= count_false:
                    return True
                else:
                    return False 

    def predict(self, datapoints):
        return [self.detect(datapoint) for datapoint in datapoints]      

    def detect_tunning(self, datapoint):
        if any(t_cell.match(datapoint) for t_cell in self.t_cells):
            return True
        else:
            # Euclidean norm... np.sqrt(np.sum(np.square(datapoint))) 
            # Largest point with its radius... np.sqrt(self.dimension * np.square(1 + self.self_radio))
            # If the Euclidean norm of a new point exceeds the largest point with its radius, it will be considered anomalous
            if np.sqrt(np.sum(np.square(datapoint))) > np.sqrt(self.dimension * np.square(1 + self.self_radio)):
                return True
            else:
                return False 

    def tunning(self, data_sample, labels_sample):

        print(f"*** len self.t_cells before finetune: {len(self.t_cells)}")
        logger.info(f"*** len self.t_cells before finetune: {len(self.t_cells)}")
        
        for data, label in zip(data_sample, labels_sample):
            if (self.detect_tunning(data) != label):
                if label == 0:      # False positive label: 0 pred: 1
                    print("False positive")
                    logger.info("False positive")
                    # Delete point from t_cells (detectors)
                    new_t_cells = self.t_cells
                    for t_cell in self.t_cells:
                        new_max_radio = t_cell.obtain_distance(data) - self.self_radio
                        if new_max_radio < 0:
                            new_t_cells.remove(t_cell)
                            print("Revoming a tCell...")
                            logger.info("Revoming a tCell...")
                    self.t_cells = new_t_cells             
                    
                else:               # False negative label: 1 pred: 0 
                    if self.algorithm == 1:
                        # KNeighborsClassifier
                        distances, _ = self.algor_01.kneighbors([data])
                    elif self.algorithm == 2:
                        # KDTree
                        distances, _ = self.algor_01.query([data], k=1)
                    elif self.algorithm == 3:
                        # BallTree
                        distances, _ = self.algor_01.query([data], k=1)
                    new_max_radio = distances.min() - self.self_radio
                    
                    # Verify new radio is greater than 0
                    if new_max_radio > 0:
                        new_t_cell = tCell(data, new_max_radio)
                        self.t_cells.append(new_t_cell)
                
        print(f"*** len self.t_cells after finetune: {len(self.t_cells)}")
        logger.info(f"*** len self.t_cells after finetune: {len(self.t_cells)}")
                
        return

    def compute_dynamic_threshold(self, self_data, percentile=95):
        """
        Calcula un umbral de radio (anomaly_radio_threshold) dinámico
        basado en las distancias dentro del conjunto 'self'.
        """
        if self.algorithm == 1:
            distances, _ = self.algor_0.kneighbors(self_data, n_neighbors=2)
            min_dists = distances[:, 1]  # el segundo más cercano (el primero es el mismo punto)
        elif self.algorithm in [2, 3]:  # KDTree o BallTree
            distances, _ = self.algor_0.query(self_data, k=2)
            min_dists = distances[:, 1]
        else:
            raise ValueError("Algorithm not supported for threshold computation")

        # Calcula percentil
        threshold = np.percentile(min_dists, percentile)
        self.anomaly_radio_threshold = threshold
        print(f"🔧 Dynamic anomaly_radio_threshold set to {threshold:.6f} (percentile {percentile})")
        return threshold
