
## Inicializa os servidores Master e Worker do Spark
/opt/spark/sbin/start-master.sh && /opt/spark/sbin/start-worker.sh spark://spark:7077


## Abra a interface web do Spark
firefox http://127.0.0.1:8080/ &

## Executa o script no Spark
spark-submit --master spark://spark:7077 capgemini-aceleracao-pyspark/scripts/<script>.py 2> /dev/null
