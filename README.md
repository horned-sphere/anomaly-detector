# README #

## Requires

Java 8
Scala 2.11
Flink 1.3.2
Elasticsearch 5.6.4
Maven 3

## Build With

```mvn package```

A runnable, shaded jar will be generated in the target directory.

## Excecute With

```bin/flink run AnomalyDetector-1.0-SNAPSHOT.jar --jobName Anomalies --configFile /path/to/job.properties --data file:///path/to/TestFile.csv```

## Configuration Properties

Flink state checkpoint interval.

checkpoint.interval (e.g. 30 minutes)

Minimum time between checkoints.

checkpoint.min_between (e.g. 5 minutes)

Maximum time that the interpolate will extrpolate for (15 minutes to match the spec).

interpolation.max_gap (e.g. 15 minutes)

Number of the most recent records that the interpolator keeps between windows or the purpose of extrapolation.

interpolation.history_len (e.g. 20)

Amount by which the interpolation window slides (windows are 3X this size).

interpolation.window_slide (e.g. 5 minutes)

Amount by which the outlier detection window slides (windows are 3X this size).

outliers.window_slide (e.g. 3 minutes)

Threshold for rejecting points (number of standard deviation under the assumption of Gaussianity).

outliers.threshold_multiple (e.g. 4.0)

Elastic search host IP.

es.host_ip=127.0.0.1

Elastic search port.

es.port

Elastic search cluster name.

es.cluster_name

Index name for raw data.

es.index_name.raw

Type name for raw data.

es.type_name.raw

Index name for anomaly scores.

es.index_name.score

Type name for anomaly scores.

es.type_name.score

Index name for corrected data.

es.index_name.corrected

Type for corrected data.

es.type_name.corrected