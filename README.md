## Solution

* sample cdc events: `stream.jsonl`
* `org.konnect.KafkaWriter` ingests sample cdc events into `cdc-events` Kafka topic
* `org.konnect.ESWriter` persists the data from Kafka topic into Opensearch index `cdc`

![img.png](solution.png)

## Execution

- start cluster(s)
  ```shell
  docker --version     
  # Docker version 27.5.1, build 9f9e405
  
  docker compose up -d  # somehow docker-compose is not available in my env
  ```
- execute processes:
  - compile project: `mvn clean compile`
  - execute Kafka publisher: `mvn exec:java -Dexec.mainClass="org.konnect.KafkaWriter"`

    Verify using `kcat -b localhost:9092 -C -t cdc-events`, or
    from [Kafka UI](http://localhost:8080/ui/clusters/local/all-topics?perPage=25)
  - write to Opensearch: `mvn exec:java -Dexec.mainClass="org.konnect.ESWriter"`

    Verify using `curl localhost:9200/cdc/_search | less` or from [Opensearch dashboard](http://localhost:5601)

- teardown using `docker compose down`
