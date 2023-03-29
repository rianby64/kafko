# Kafkame

The purpose of this repo is to show a simple example of how to connect to kafka and

- Publish
- Listen

The core is located at `kafkame` directory. And, the examples of how to use `kafkame` are at `cmd` directory.

Feel free to run `podman-compose up` and then run both listening and sending commands.


```bash
podman exec -it kafkame_kafka1_1 bash
kafka-topics.sh --create --topic orders --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1

cd cmd/listening
go run main.go

cd cmd/sending
go run main.go
```
