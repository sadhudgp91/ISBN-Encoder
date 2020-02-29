# Spark Tuning

The existing Spark job works very well for small data (<1TB) but fails for big data with exceptions like: "MapOutputTracker: Missing an output location for shuffle". 

Please improve the code and tune the Spark configuration to make it ready for big data.

#### Describe your improvements

__TODO:__ Please explain your improvements briefly and highlight the advantages and disadvantages of it.

### Configuration

The current Spark job settings:

```
$SPARK_HOME/bin/spark-submit \
        --properties-file /etc/spark/conf/spark-defaults.conf \
        --master yarn \
        --deploy-mode client \
        --num-executors 60 \
        --executor-memory 5g \
        --driver-memory 2g \
        --class $CLASS \
        $JAR \
        $INPUT \
        $OUTPUT
```

