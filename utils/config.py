import yaml

class Config:
    def __init__(self,
                 ai_last_n_hours, ai_last_n_hours_test, ai_last_n_minutes_test, ai_train, ai_kmeans, ai_nsa, 
                 ai_graph, ai_protocol, ai_ip, ai_port, ai_parent_node, ai_charts, ai_dims, ai_interval, ai_iterations,
                 ai_nsa_num_samples_to_lag, ai_nsa_num_samples_to_diff, ai_nsa_num_samples_to_smooth, 
                 ai_nsa_max_t_cells, ai_nsa_max_attempts, ai_nsa_percentage, ai_nsa_algorithm, ai_nsa_n_neighbors,
                 ai_kmeans_num_samples_to_lag, ai_kmeans_num_samples_to_diff, ai_kmeans_num_samples_to_smooth,
                 ai_nsa_anomaly_rate, ai_kmeans_anomaly_rate, ai_nsa_kmeans_anomaly_rate, ai_kmeans_max_iterations,
                 ai_kmeans_dim_anomaly_score):
        self.AI_LAST_N_HOURS = ai_last_n_hours
        self.AI_LAST_N_HOURS_TEST = ai_last_n_hours_test
        self.AI_LAST_N_MINUTES_TEST = ai_last_n_minutes_test
        self.AI_TRAIN = ai_train
        self.AI_KMEANS = ai_kmeans
        self.AI_NSA = ai_nsa
        self.AI_GRAPH = ai_graph
        self.AI_PROTOCOL = ai_protocol
        self.AI_IP = ai_ip
        self.AI_PORT = ai_port
        self.AI_PARENT_NODE = ai_parent_node
        self.AI_CHARTS = ai_charts
        self.AI_DIMS = ai_dims
        self.AI_INTERVAL = ai_interval
        self.AI_ITERATIONS = ai_iterations
        self.AI_NSA_NUM_SAMPLES_TO_LAG = ai_nsa_num_samples_to_lag
        self.AI_NSA_NUM_SAMPLES_TO_DIFF = ai_nsa_num_samples_to_diff
        self.AI_NSA_NUM_SAMPLES_TO_SMOOTH = ai_nsa_num_samples_to_smooth
        self.AI_NSA_MAX_T_CELLS = ai_nsa_max_t_cells
        self.AI_NSA_MAX_ATTEMPTS = ai_nsa_max_attempts
        self.AI_NSA_PERCENTAGE = ai_nsa_percentage
        self.AI_NSA_ALGORITHM = ai_nsa_algorithm
        self.AI_NSA_N_NEIGHBORS = ai_nsa_n_neighbors
        self.AI_KMEANS_NUM_SAMPLES_TO_LAG = ai_kmeans_num_samples_to_lag
        self.AI_KMEANS_NUM_SAMPLES_TO_DIFF = ai_kmeans_num_samples_to_diff
        self.AI_KMEANS_NUM_SAMPLES_TO_SMOOTH = ai_kmeans_num_samples_to_smooth
        self.AI_NSA_ANOMALY_RATE = ai_nsa_anomaly_rate
        self.AI_KMEANS_ANOMALY_RATE = ai_kmeans_anomaly_rate
        self.AI_NSA_KMEANS_ANOMALY_RATE = ai_nsa_kmeans_anomaly_rate
        self.AI_KMEANS_MAX_ITERATIONS = ai_kmeans_max_iterations
        self.AI_KMEANS_DIM_ANOMALY_SCORE = ai_kmeans_dim_anomaly_score


def load_config_from_yaml(yaml_file_path):
    with open(yaml_file_path, 'r') as file:
        yaml_config = yaml.safe_load(file)

    # Create the Config object
    config = Config(**yaml_config)

    return config
