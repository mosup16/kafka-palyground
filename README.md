## Kafka Playground (CPU Usage Monotiring Case Study 👀️)

#### The project is a proof of concept that uses kafka alongside a java based clients.
The project consists of three threads. The first one acts as a kafka producer that produces a key value pairs of the cpu usage reads as the values and the time of these reads as the keys. The second thread is a processor that consumes the messages produced by the previous thread and transforms it to the percentile format ,then publishes it to the sink topic. The last thread is a consumer that recives the transformed cpu usages and just prints it to the console.

#### To run the project first you need to have kafka installed and running on :

```
localhost:9092
```

#### To install kafka with docker you can use this line of code :

```
docker run -p 9092:9092 -d --name kafka-kraft bashj79/kafka-kraft
```

#### **Note** that this kafka image uses kafka kraft mode and installs a single instance cluster that runs without any zookeeper which is ***Awesome*** 🚀️🎉️🎉️

After installing kafka , running the project with maven and intalling the dependncies you should see somthing like that in the console

```
produce to cpu-usage topic on partition number 0 with offset 595
transform from {1664121527734:0.23404255319148937} to {1664121527734:23.404255319148938} and send to cpu-usage-percentage topic on partition number 0 with offset 584
cpu usage : 23.40% , usage read timestamp : 1664121527734ms, time taken to consume the message : 7ms
produce to cpu-usage topic on partition number 0 with offset 596
transform from {1664121528734:0.33695652173913043} to {1664121528734:33.69565217391305} and send to cpu-usage-percentage topic on partition number 0 with offset 585
cpu usage : 33.70% , usage read timestamp : 1664121528734ms, time taken to consume the message : 9ms
produce to cpu-usage topic on partition number 0 with offset 597
transform from {1664121529735:0.22340425531914893} to {1664121529735:22.340425531914892} and send to cpu-usage-percentage topic on partition number 0 with offset 586
cpu usage : 22.34% , usage read timestamp : 1664121529735ms, time taken to consume the message : 8ms
```

