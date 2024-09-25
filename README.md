# AI Anomaly Detection

# PR22_0297-NebulOus

## Running in local

Python 3.x

# Console #1

```
kubectl -n nebulous-cd get pods
```

Get nebulous-activemq-xxxxx pod name (for example nebulous-activemq-6b7f4cd65-p58xs)

Replace in the following line with the privious info

```
kubectl -n nebulous-cd port-forward nebulous-activemq-xxxxx 32754:32754
```

For example
```
kubectl -n nebulous-cd port-forward nebulous-activemq-6b7f4cd65-p58xs 32754:32754
```

# Console #2. Choose 2.1 or 2.2

2.1 Example to run in local with local configuration

```
python __main__.py configs/config_local.yml
```

2.2 or example to run in local with general configuration

```
python __main__.py configs/config.yml
```

## Building a docker container & running

```
docker-compose build 
```

```
docker-compose up
```

