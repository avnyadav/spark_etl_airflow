

https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html#setting-the-right-airflow-user

Execute below command to configure airflow in your system.

```
docker-compose up airflow-init
```



```
docker-compose up
```


Flower url:
Flower is a web based tool for monitoring and administrating Celery clusters. This topic describes how to configure Airflow to secure your flower instance.
```
http://localhost:5555/
```

Airflow url 
```
http://localhost:8080
```
### Airflow user name
```
airflow
```
### Airflow password: 
```
airflow
```

Upgrade package
```
 pip install --upgrade pip setuptools wheel
```
To create whl package for custom module
```
python setup.py bdist_wheel 
```