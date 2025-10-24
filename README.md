# 🧠 Anomaly Detection Component

The **Anomaly Detection** component monitors network metrics of Kubernetes-based applications (prefix `adt_`) and flags deviations from learned “normal” behaviour. It is implemented as a modular component that integrates with NebulOuS via the monitoring data bus and uses in-memory models per application instance.

## 🔍 Key Features
- **Distributed detection**: Multi-threaded per-instance metric collection and inference.
- **Dual AI anomaly detection strategies**: Negative Selection Algorithm (NSA) and K-Means clustering.
- **Adaptive configuration**: Fully configurable via `.ini` file and environment variables (supporting edge deployment and dynamic switching).
- **Persistence and messaging**: InfluxDB for storage and Artemis broker for real-time alerting.
- **Traceability**: Each anomaly is persisted in the application bucket for later analysis and visualization.

## 🧩 Architecture & Workflow

1. **Subscription**: Subscribes to the `topic://eu.nebulouscloud.monitoring.metric_list` topic.  
2. **Message Handling**: Receives a metric_list message with the application name and version. An entry is added in AiadPredictorState.received_applications
3. **Result Topic Setup**: Defines the `topic://eu.nebulouscloud.ad.aiad.allmetrics` topic for publishing anomaly detection results.  
4. **Continuous Modeling & Monitoring Cycle**:
   - Retrieves application instances (master and worker IDs) from InfluxDB.
   - For each instance:
     - Fetches network metrics with the `adt_` prefix.
     - For each metric:
       - Retrieves historical data.
     - Filters out:
       - Non-variable or low-variance metrics.
       - Highly correlated metrics.
     - Selects the **N most representative metrics** based on importance.
     - Builds anomaly detection models:
       - One **NSA model** for the representative metrics.
       - One **K-Means model** per representative metric.
     - Retrieves recent metric data for the current time window.
     - Applies inference:
       - NSA-based anomaly detection for representative metrics as a whole.
       - K-Means-based anomaly detection per metric.
     - Evaluates thresholds:
       - If the **NSA anomaly rate** exceeds the defined NSA threshold → publishes an incident and stores in InfluxDB under the same application bucket for persistence and traceability.
       - If the **K-Means anomaly rate** for any metric exceeds the defined K-Means threshold → publishes an incident and stores in InfluxDB under the same application bucket for persistence and traceability.

## 📊 Monitored Metrics

All **network.related metrics** prefixed with `adt_`.


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

- ✉️ Example NSA message:
```json
{
  "method": "aiad nsa",
  "level": 3,
  "application": "<application_name>",
  "node": "<instance_id>",
  "timestamp": <current_time>,
  "window_start": <window_start_epoch>,
  "window_end": <window_end_epoch>,
  "window_anomaly_rate": <percentage>,
  "predictionTime": <next_prediction_time>,
  "metrics": ["<metric1>", "<metric2>", ...]
}
```
- ✉️ Example K-Means message:
```json
{
  "method": "aiad kmeans",
  "level": 3,
  "application": "<application_name>",
  "node": "<instance_id>",
  "timestamp": <current_time>,
  "window_start": <window_start_epoch>,
  "window_end": <window_end_epoch>,
  "window_anomaly_rate": <percentage>,
  "predictionTime": <next_prediction_time>,
  "metrics": "<metric_name>"
}
```

---

## 💾 Anomaly Persistence in InfluxDB

Each published anomaly detection (NSA or K-Means) is also stored in InfluxDB in the same bucket used by the monitored application. 

This enables **historical tracking, visualization, and correlation** within NebulOuS dashboards.

**InfluxDB Storage Details**

- **Bucket name**: nebulous_<application_name>_bucket

- **Measurement**: aiad

- **Tags**:

	- method → "aiad nsa" or "aiad kmeans"

	- application → application name (nebulous_<...>_bucket)

	- node → instance ID

- **Fields**:

	- window_start (epoch)

	- window_end (epoch)

	- window_anomaly_rate (float)

	- prediction_time (epoch)

	- timestamp (epoch)

	- metrics → a metric for K-Means / metrics for NSA	

---

### **Example records**

- NSA

| time                 | method   | application      | node       | window_start | window_end | window_anomaly_rate | prediction_time | timestamp  | metrics                                                                                                                                                                                                                      |
| -------------------- | -------- | ---------------- | ---------- | ------------ | ---------- | ------------------- | --------------- | ---------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 2025-10-14T11:20:11Z | aiad nsa | application_name | instance_1 | 1761295860   | 1761297960 | 21.88888888888889   | 1761302700      | 1761302411 | ['adt_cpu_system_dst_3', 'adt_cpu_system_dst_5', 'adt_net_in_dst_5', 'adt_cpu_user_dst_2', 'adt_ram_used_dst_4', 'adt_net_out_dst_4', 'adt_net_out_dst_5', 'adt_net_in_dst_2', 'adt_cpu_system_dst_2', 'adt_ram_used_dst_5'] |


- K-Means

| time                 | method      | application      | node       | window_start | window_end | window_anomaly_rate  | prediction_time | timestamp  | metrics            |
| -------------------- | ----------- | ---------------- | ---------- | ------------ | ---------- | -------------------- | --------------- | ---------- | ------------------ |
| 2025-10-14T11:20:11Z | aiad kmeans | application_name | instance_2 | 1761295860   | 1761297960 | 20.555555555555555   | 1761302700      | 1761302411 | adt_ram_used_dst_2 |


Notes

* time is automatically set by InfluxDB.

* For K-Means, metrics is a single string.

* For NSA, it is a list of representative metrics.

* All numeric fields are stored as numbers for aggregation and querying.


Dual persistence (broker + InfluxDB) ensures:

- Real-time alerts via Artemis.

- Historical analysis via InfluxDB dashboards.

---

### 📊 Querying Anomalies in InfluxDB (Flux examples)

** Retrieve all anomaly detections (NSA & K-Means) for a given application**

```flux
from(bucket: "nebulous_<application_name>_bucket")
  |> range(start: -24h)
  |> filter(fn: (r) => r._measurement == "aiad")
  |> filter(fn: (r) => r.application == "<application_name>")
  |> keep(columns: ["_time", "method", "node", "metrics", "_field", "_value"])
  |> sort(columns: ["_time"], desc: true)
```

---

** Retrieve only K-Means detections exceeding a given threshold (e.g., 30%)**

```flux
from(bucket: "nebulous_<application_name>_bucket")
  |> range(start: -6h)
  |> filter(fn: (r) => r._measurement == "aiad")
  |> filter(fn: (r) => r.method == "aiad kmeans")
  |> filter(fn: (r) => r._field == "window_anomaly_rate" and r._value > 30.0)
  |> keep(columns: ["_time", "node", "metrics", "_value"])
  |> rename(columns: {_value: "anomaly_rate"})
```

---

** Retrieve recent NSA detections per instance**

```flux
from(bucket: "nebulous_<application_name>_bucket")
  |> range(start: -12h)
  |> filter(fn: (r) => r._measurement == "aiad")
  |> filter(fn: (r) => r.method == "aiad nsa")
  |> group(columns: ["node"])
  |> mean(column: "_value")
```

---


## ⚙️ Configuration

| Parameter                         | Description                                                       |
|-----------------------------------|-------------------------------------------------------------------|
| `AI_NSA`                          | Enable NSA algorithm (boolean)                                    |
| `AI_NSA_ANOMALY_RATE`             | Threshold (%) for NSA anomaly rate                                |
| `AI_KMEANS`                       | Enable K-Means algorithm (boolean)                                |
| `AI_KMEANS_ANOMALY_RATE`          | Threshold (%) for K-Means anomaly rate per metric                 |
| `NUMBER_OF_DAYS_TO_USE_DATA_FROM` | Days of historical data for model building                        |
| `NUMBER_OF_MINUTES_TO_INFER`      | Inference window duration                                         |
| `PATH_TO_DATASETS`                | Filesystem path for storing datasets                              |
| ...                               | Other parameters in `aiad.ini` and `Utilities.load_configuration` |

## 💡​ Model Building & AI Algorithms
- **Data Preparation**: Historical metrics from InfluxDB, resampled to 1-minute frequency, interpolated, and merged. Columns with zero variability, low variance, or high correlation are removed.
- **NSA**:
  - Trains "tCells" detectors modeling the "self" region.
  - Flags data points not matched or exceeding distance threshold as anomalies.
- **K-Means**:
  - Trains clustering models per metric.
  - Calculates distances to centroids and normalizes to detect anomalies.
- **Thresholding**:
  - Compares anomaly rate to configured threshold to publish alerts.
  - Persist anomalies exceeding threshold to InfluxDB alongside broker messages.

## 🗄️ Data Persistence & Logging
- Metrics and anomalies stored in InfluxDB buckets: `nebulous_<application_name>_bucket`.
- Anomaly detections stored under measurement aiad (NSA & K-Means).
- Logging with timestamps: `[YYYY-MM-DD HH:MM:SS] ...` via `Utilities.print_with_time()`.
- Global state in `AiadPredictorState` with threads, consumers/publishers, and configuration.

## ⏳​ Usage & Deployment
- Run: `python Predictor.py <configuration_file>`
- Ensure Artemis and InfluxDB are accessible with configured credentials

## 🌎​ Integration with NebulOuS Ecosystem
- Component within NebulOuS Meta-OS stack
  - **Upstream**: Monitoring data bus (`metric_list`).
  - **Downstream**: Alert bus (`eu.nebulouscloud.ad.aiad.allmetrics`).
  - **External**: InfluxDB and orchestrators consuming detections.
  - **Visualization**: Enables long-term anomaly analysis within NebulOuS dashboards via InfluxDB.
