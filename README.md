## Problem

I decided to create a simple pipeline, where I collect [metrics](flink/src/main/scala/flink/model/Metrics.scala) (a case class)
for each machine (a simple string), and calculate the average resource consumption over a period of time.
[This pipeline](flink/src/main/scala/flink/pipeline/FlinkIncorrectPipeline.scala) works just fine when dealing with orderly data. 
However, it behaves strangely when out-of-order events come.

When Flink encounters late events, it triggers [late firing](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/operators/windows/#late-elements-considerations).
However, [the pipeline](flink/src/main/scala/flink/pipeline/FlinkIncorrectPipeline.scala) doesn't always capture intermediate results caused by late events.
Moreover, it doesn't consider the allowed lateness at all (it doesn't drop out-of-order events when set to 0).
I have tried to capture this behavior in [unit-tests](flink/src/test/scala/FlinkConsumerSpec.scala).

## Unit-tests

I have created 4 unit-tests, which share the same testing data. 

```scala
val events: Seq[(EventTime, Machine, Metrics)] = Seq(
    (1L, "machine", Metrics(1, 1)),
    (2L, "machine", Metrics(3, 3)),
    (5L, "machine", Metrics(5, 5)),
    (6L, "machine", Metrics(6, 6)),
    (3L, "machine", Metrics(4, 4)),
    (4L, "machine", Metrics(9, 9)),
    (7L, "machine", Metrics(8, 8)))
```

Considering the fact that the window size is 3 millisecond, event 3 and 4 (look at the event time) are late.
If the allowed lateness is set to 0, they should be dropped. 

```scala
List[(EventTime, Machine, Metrics)](
  (2L, "machine", Metrics(2, 2)),
  (5L, "machine", Metrics(5, 5)),
  (8L, "machine", Metrics(7, 7)))
```

But if the allowed lateness is equal to 3, they should cause two late firings for [3, 6).

```scala
List[(EventTime, Machine, Metrics)](
  (2L, "machine", Metrics(2, 2)),
  (5L, "machine", Metrics(5, 5)),
  (5L, "machine", Metrics(4.5, 4.5)), // late firing
  (5L, "machine", Metrics(6, 6)),     // late firing
  (8L, "machine", Metrics(7, 7)))
```

Unfortunately, the incorrect pipeline doesn't work for both cases. 

### Possible cause 

Notably, [the incorrect pipeline](flink/src/main/scala/flink/pipeline/FlinkIncorrectPipeline.scala) starts working if you remove `mapWith` at the very beginning of it. 
However, you will have to provide modified data already containing 1-s (so that you don't need `mapWith` anymore).

### Unit-test utils

I have developed [a mock source](flink/src/test/scala/util/MockKafkaSource.scala), which emits events and ascending watermarks.
Also, I have created [a mock sink](flink/src/test/scala/util/MockKafkaSink.scala), which creates an accumulator for string test outcomes.
