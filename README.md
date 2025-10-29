# 🧠 Anomaly Detection Component

The **Anomaly Detection** component monitors network metrics of Kubernetes-based applications (prefix `adt_`) and flags deviations from learned “normal” behaviour. It is implemented as a modular component that integrates with NebulOuS via the monitoring data bus and uses in-memory models per application instance.

The component runs as a lightweight edge service within the NebulOuS Meta-OS, continuously monitoring active application instances.

## 🔍 Key Features
- **Distributed detection**: Multi-threaded per-instance metric collection and inference.
- **Dual AI anomaly detection strategies**: Negative Selection Algorithm (NSA) and K-Means clustering.
- **Adaptive configuration**: Fully configurable via `.ini` file and environment variables (supporting edge deployment and dynamic switching).
- **Persistence and messaging**: InfluxDB for storage, and Artemis broker for real-time alerting.
- **Traceability**: Each anomaly is persisted in the application bucket for later analysis and visualization.

## 🧩 Architecture & Workflow

1. **Subscription**: Subscribes to the `topic://eu.nebulouscloud.monitoring.metric_list` topic.  
2. **Message Handling**: Receives a metric_list message with the application name and version. An entry is added in AiadPredictorState.received_applications
3. **Result Topic Setup**: Publishes anomaly detection results to the `topic://eu.nebulouscloud.ad.aiad.allmetrics` topic.
4. **Continuous Modeling & Monitoring Cycle**:
   - Retrieves application instances (master and worker IDs) from InfluxDB.
   - Cycle N intervals:
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
   This cycle repeats periodically, ensuring continuous adaptation to workload dynamics.

## 📊 Monitored Metrics

All **network metrics** prefixed with `adt_`.


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


Each anomaly detection (NSA or K-Means) is automatically stored in **InfluxDB**, within the same bucket used by the monitored application.  
This enables **historical tracking, visualization, and correlation** in NebulOuS dashboards.

### 🧠 InfluxDB Storage Details

| Element         | Description                                                                                                                                                                                                                                                                                                                                             |
| --------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Bucket name** | `nebulous_<application_name>_bucket`                                                                                                                                                                                                                                                                                                                    |
| **Measurement** | `_predicted_<method>_<instance>[_<metric>]`                                                                                                                                                                                                                                                                                                             |
| **Tags**        | - `application` → application name (e.g., `<application_name>`)<br> - `node` → instance ID (e.g., `<instance>`)<br> - `method` → `"aiad nsa"` or `"aiad kmeans"`<br> - `metrics` → a single metric (K-Means) or list of metrics (NSA)<br> - `level` → the EMS level at which this metric value has been published (1,2 or 3)                            |
| **Fields**      | - `window_start` → epoch timestamp (start of window)<br> - `window_end` → epoch timestamp (end of window)<br> - `metricValue` → anomaly rate as float<br> - `predictionTime` → epoch timestamp when prediction was made<br> - `anomaly` → `true` if metricValue exceeds the threshold, `false` otherwise<br> - `timestamp` → epoch timestamp of message |

---

### 📊 Example Records

#### NSA (Multivariate Detection)

| time                 | measurement                    | method   | application      | node       | window_start | window_end | metricValue         | anomaly | predictionTime  | timestamp  | metrics                                                                                                                                                                                       |
| -------------------- | ------------------------------ | -------- | ---------------- | ---------- | ------------ | ---------- | ------------------- | ------- | --------------- | ---------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 2025-10-14T11:20:11Z | _predicted_aiad_nsa_instance_1 | aiad nsa | application_name | instance_1 | 1761295860   | 1761297960 | 21.88888888888889   | true    | 1761302700      | 1761302411 | adt_cpu_system_dst_3,adt_cpu_system_dst_5,adt_net_in_dst_5,adt_cpu_user_dst_2,adt_ram_used_dst_4,adt_net_out_dst_4,adt_net_out_dst_5,adt_net_in_dst_2,adt_cpu_system_dst_2,adt_ram_used_dst_5 |

#### K-Means (Univariate Detection)

| time                 | measurement                                          | method      | application      | node       | window_start | window_end | metricValue         | anomaly | predictionTime  | timestamp  | metrics            |
| -------------------- | ---------------------------------------------------- | ----------- | ---------------- | ---------- | ------------ | ---------- | ------------------- | ------- | --------------- | ---------- | ------------------ |
| 2025-10-14T11:20:11Z | _predicted_aiad_kmeans_instance_2_adt_ram_used_dst_2 | aiad kmeans | application_name | instance_2 | 1761295860   | 1761297960 | 20.555555555555555  | true    | 1761302700      | 1761302411 | adt_ram_used_dst_2 |

---

### 📝 Notes

* `time` is automatically set by **InfluxDB** when the record is written.
* `metricValue` corresponds to the window anomaly rate.
* `anomaly` is true if the value exceeded the configured detection threshold.
* For **K-Means**, the `metrics` field contains a **single metric name**.
* For **NSA**, it contains a **list of representative metrics** (comma-separated).
* All numeric fields are stored as numbers, supporting aggregation and filtering in Flux queries.
* This design ensures that **AIAD anomalies** can be visualized directly in NebulOuS dashboards alongside operational metrics — no schema changes required.

Dual persistence (broker + InfluxDB) ensures:

- Real-time alerts via Artemis.

- Historical analysis via InfluxDB dashboards.

---

### 📊 Querying Anomalies in InfluxDB (Flux examples)

** Retrieve all anomaly detections (NSA & K-Means) for a given application**

```flux
from(bucket: "nebulous_<application_name>_bucket")
  |> range(start: -24h)
  |> filter(fn: (r) => r._measurement =~ /^_predicted_aiad_.*/)
  |> filter(fn: (r) => r.application_name == "<application_name>")
  |> keep(columns: ["_time", "method", "node", "metrics", "_field", "_value"])
  |> sort(columns: ["_time"], desc: true)  
  
```

---

** Retrieve only K-Means detections exceeding a given threshold (e.g., 30%)**

```flux
from(bucket: "nebulous_<application_name>_bucket")
  |> range(start: -6h)
  |> filter(fn: (r) => r._measurement =~ /^_predicted_aiad_kmeans.*/)
  |> filter(fn: (r) => r._field == "metricValue" and r._value > 30.0)
  |> keep(columns: ["_time", "node", "metrics", "_value"])
  |> rename(columns: {_value: "anomaly_rate"})
```

---

** Retrieve recent NSA detections per instance**

```flux
from(bucket: "nebulous_<application_name>_bucket")
  |> range(start: -12h)
  |> filter(fn: (r) => r._measurement =~ /^_predicted_aiad_nsa.*/)
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
  - All detections are persisted to InfluxDB, but only those exceeding the threshold are also published to the Artemis broker.

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
