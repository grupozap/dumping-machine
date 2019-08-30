# Dumping Machine

> Dumping Machine is an application which dumps Kafka Avro topics to S3 or HDFS as Parquet.

---

## Table of Contents (Optional)

- [Installation](#installation)
- [Compatibility](#compatibility)
- [Partition](#partition)
- [Hive Metastore](#Hive-Metastore)
- [Team](#team)

---

## Installation

- Clone this repo to your local machine using `https://github.com/grupozap/dumping-machine`

### Build requirements

- JDK 8

### Setup

Make sure you've made changes to `config/application.yml`

```shell
$ ./gradlew clean run
```

---

## Compatibility

- [Kafka 1.1.1](https://mvnrepository.com/artifact/org.apache.kafka/kafka_2.12/1.1.1) 
- [Hive Metastore 2.3.5](https://mvnrepository.com/artifact/org.apache.hive/hive-metastore/2.3.5)
- [Hadoop 2.9.2](https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-common/2.9.2)

---

## Partition

Partitioning is by date and hour

```
{TOPIC_NAME}/{DATE}/{HOUR}/{PARQUET_FILE}
```

Example:

```
prod-dataplatform-events/dt=2019-08-30/hr=22/1_78465.parquet
prod-dataplatform-events/dt=2019-08-30/hr=23/3_78977.parquet
prod-dataplatform-events/dt=2019-08-31/hr=00/8_77567.parquet
```

---

## Hive Metastore

**Dumping Machine** supports Hive Metastore for the following operations:
- Create database
- Create table
- Update table
- Add partition

---

## Team
Made with :heart: by the [Grupo ZAP](https://github.com/grupozap) engineering team