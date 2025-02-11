# Anomaly Detection Component

The **Anomaly Detection** component identifies scenarios where resource utilization in Kubernetes-based applications deviates from expected behavior. It achieves this by analyzing metrics stored in **InfluxDB**.

## How does the Anomaly Detection component work?

**1** - Subscribes to monitoring.metric_list.

**2** - Receives a message containing:

	application name
	
	metrics (name, lower bound, upper bound)

**3** - Defines a topic to publishing results.

**4** - Starts a continuous cycle of modeling and monitoring the application.

**4.1** - Retrieves historical application data (metrics, days_to_use_data_from).

**4.2** - Builds anomaly detection models:

		Creates the Negative Selection Algorithm (NSA) model for the set of metrics.

		Creates a k-means model for each individual metric.

**4.3** - Retrieves recent application data (minutes_to_infer).

**4.4** - Applies inference models to detect deviations.

**4.5** - Checks if the anomaly_rate within the window exceeds a threshold:

		Publishes an incident for further analysis and action

## Metrics
The component can monitor any specified **metric** through the **graphical user interface (GUI)** along with **lower_bound_value** and **upper_bound_value** constraints for each of the metrics.

## Topics

### Subscriber Topic

The subscription topic is `topic://eu.nebulouscloud.monitoring.metric_list`.

The information received is:

```
- Application Name
- Version
- Metric List
  - Name
  - Lower Bound
  - Upper Bound
```
  
Example:

```json
{
    "name": "Application",
    "version": 1,
    "metric_list": [
        {"name": "system_cpu_user", "lower_bound": "-Infinity", "upper_bound": "Infinity"},
        {"name": "system_ram_used", "lower_bound": "0.0", "upper_bound": "10000.0"},
        {"name": "system_cpu_system", "lower_bound": "0.0", "upper_bound": "100.0"}
    ]
}
```

### Publisher Topic

When the rate of deviations exceeds a threshold (anomaly_rate), an incident is published in `topic://eu.nebulouscloud.ad.aiad.allmetrics` for further analysis and action.

For the NSA:

```
message_body = {
	"method": "aiad nsa",
	"level": 3,
	"application": application_name,
	"timestamp": current_time,
	"window_start": min_time,
	"window_end": max_time,
	"window_anomaly_rate": window_anomaly_rate,
	"predictionTime": next_prediction_time,
	"metrics": set of metrics
}
```

For the k-means:

```
message_body = {
	"method": "aiad kmeans",
	"level": 3,
	"application": application_name,
	"timestamp": current_time,
	"window_start": min_time,
	"window_end": max_time,
	"window_anomaly_rate": window_anomaly_rate,
	"predictionTime": next_prediction_time,
	"metrics": metric
}		
```
