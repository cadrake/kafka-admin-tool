# Kafka Admin Tool - Simple utilitiy for rebalancing and reassigning topic and partitions

The Kafka Admin Tool is designed to simplify the task of rebalancing and reassigning topic partitions

## Building

To build the tool run

```
make
```

Then copy `./bin/kafka-admin-tool` to a location of your choice

## The `reassign` Command

Takes a broker list and optional topic regex and reassigns partitions, note that the `-f` and `-t`
options take the broker id (eg id set in the kafka configuration). Unless otherwise noted, use
`--execute` to actually apply the changes.

### Examples

<details><summary>Move all partitions for matching topics away from a given broker:</summary>

```
kafka-admin-tool reassign --broker-list localhost:9092 --topic-filter .*reassign.* -f 5
```
</details>

<details><summary>Move all partitions for matching topics from one broker to a new broker:</summary>

```
kafka-admin-tool reassign --broker-list localhost:9092 --topic-filter .*reassign.* -f 5 -t 1
```
</details>

<details><summary>Move all partitions for matching topics from one broker to a new broker and save the reassignments as a json that can be fed to kafka-reassign-partitions:</summary>

```
kafka-admin-tool reassign --broker-list localhost:9092 --topic-filter .*reassign.* -f 5 -t 1 -o assignments.json
```
</details>

## The `alter` Command

Takes a broker list and optional topic regex and either set a new replication factor or adjust the
existing replication factor. Unless otherwise noted, use `--execute` to actually apply the changes.

### Examples

<details><summary>Increase the number of replicas for matching topics by 2:</summary>

```
kafka-admin-tool alter --broker-list localhost:9092 --topic-filter .*topic.v1 --delta-rf 2
```
</details>

<details><summary>Decrease the number of replicas for matching topics by 2:</summary>

```
kafka-admin-tool alter --broker-list localhost:9092 --topic-filter .*topic.v1 --delta-rf -2
```
</details>

<details><summary>Set the number of replicas for matching topics to 10:</summary>

```
kafka-admin-tool alter --broker-list localhost:9092 --topic-filter .*topic.v1 --new-rf 10
```
</details>

## The `verify` Command

Takes a reassignments json file and tracks the live clusters progress moving the partitions

### Examples

<details><summary>Track a reassignment in progress</summary>

```

kafka-admin-tool verify --input-json assignments.json
```
</details>
