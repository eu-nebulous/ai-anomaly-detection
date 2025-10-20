# 🧠 Anomaly Detection Component

The **Anomaly Detection** component identifies deviations in network metrics within Kubernetes-based applications, using AI techniques to detect abnormal behavior.

## 🔍 Key Features

- **Distributed detection**: runs threads per application instance.  
- **AI-based modeling**: leverages the **Negative Selection Algorithm (NSA)** and **K-Means clustering**.  
- **Adaptive configuration**: supports `.ini` files and environment variables.

---

## ⚙️ How It Works

1. **Subscription**: Subscribes to the `monitoring.metric_list` topic.  
2. **Message Handling**: Receives the application name and version.  
3. **Result Topic Setup**: Defines the topic for publishing anomaly detection results.  
4. **Continuous Modeling & Monitoring Cycle**:
   - Retrieves application instances (master and worker IDs) from InfluxDB.
   - For each instance:
     - Fetches network metrics with the `adt_` prefix.
     - For each metric:
       - Retrieves historical data.
     - Filters out:
       - Metrics with no variability.
       - Metrics with low variance.
       - Highly correlated metrics.
     - Selects the **N most representative metrics** based on importance.
     - Builds anomaly detection models:
       - One **NSA model** for the representative metrics.
       - One **K-Means model** per representative metric.
     - Retrieves recent data for the current time window.
     - Applies inference:
       - NSA-based anomaly detection.
       - K-Means-based anomaly detection per metric.
     - Evaluates thresholds:
       - If the **NSA anomaly rate** exceeds the threshold → publishes an incident.
       - If the **K-Means anomaly rate** for any metric exceeds the threshold → publishes an incident.

---

## 📊 Monitored Metrics

All **network metrics** prefixed with `adt_`.

---

## 📨 Messaging Topics

### Subscriber Topic

```text
topic://eu.nebulouscloud.monitoring.metric_list
```

**Message format:**

```json
{
    "name": "Application",
    "version": 1
}
```

### Publisher Topic

```text
topic://eu.nebulouscloud.ad.aiad.allmetrics
```

Incidents are published when anomaly rates exceed predefined thresholds.

#### NSA Message Example

```json
{
    "method": "aiad nsa",
    "level": 3,
    "application": "application_name",
    "node": "instance_id",
    "timestamp": "current_time",
    "window_start": "min_time",
    "window_end": "max_time",
    "window_anomaly_rate": "window_anomaly_rate",
    "predictionTime": "next_prediction_time",
    "metrics": ["metric1", "metric2", "..."]
}
```

#### K-Means Message Example

```json
{
    "method": "aiad kmeans",
    "level": 3,
    "application": "application_name",
    "node": "instance_id",
    "timestamp": "current_time",
    "window_start": "min_time",
    "window_end": "max_time",
    "window_anomaly_rate": "window_anomaly_rate",
    "predictionTime": "next_prediction_time",
    "metrics": "metric_name"
}
```
