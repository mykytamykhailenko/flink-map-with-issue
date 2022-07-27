import flink.model.Metrics
import flink.pipeline.{FlinkCorrectPipeline, FlinkIncorrectPipeline}
import flink.util.Util.{EventTime, Machine}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.specs2.matcher.Matchers
import org.specs2.mutable.Specification
import util.{MockKafkaSink, MockKafkaSource}

class FlinkConsumerSpec extends Specification with Matchers {

  val flinkCluster = new MiniClusterWithClientResource(
    new MiniClusterResourceConfiguration.Builder()
      .setNumberSlotsPerTaskManager(2)
      .setNumberTaskManagers(1)
      .build)

  val windowSizeAndStep = 3

  val events: Seq[(EventTime, Machine, Metrics)] = Seq(
    (1L, "machine", Metrics(1, 1)),
    (2L, "machine", Metrics(3, 3)),
    (5L, "machine", Metrics(5, 5)),
    (6L, "machine", Metrics(6, 6)),
    (3L, "machine", Metrics(4, 4)),
    (4L, "machine", Metrics(9, 9)),
    (7L, "machine", Metrics(8, 8)))

  "correct flink consumer" should {

    "account for out of order events when allowed lateness is 3" in {

      val env = StreamExecutionEnvironment.getExecutionEnvironment

      // 3 and 4 are out-of-order events.
      val eventSource = MockKafkaSource(events)

      val mockSource = env.addSource(eventSource)
      val mockSink = MockKafkaSink(accumulatorName = "accountForOutOfOrder")

      FlinkCorrectPipeline(allowedLateness = 3, windowSizeAndStep).build(mockSource, _.addSink(mockSink))

      val job = env.execute()

      mockSink.getResults(job) === List[(EventTime, Machine, Metrics)](
        (2L, "machine", Metrics(2, 2)),
        (5L, "machine", Metrics(5, 5)),
        (5L, "machine", Metrics(4.5, 4.5)), // late firing
        (5L, "machine", Metrics(6, 6)), // late firing
        (8L, "machine", Metrics(7, 7))
      )
    }

    "drop out of order events when allowed lateness is 0" in {

      val env = StreamExecutionEnvironment.getExecutionEnvironment

      // 3 and 4 are out-of-order events.
      val eventSource = MockKafkaSource(events)

      val mockSource = env.addSource(eventSource)
      val mockSink = MockKafkaSink(accumulatorName = "discardOutOfOrder")

      FlinkCorrectPipeline(allowedLateness = 0, windowSizeAndStep).build(mockSource, _.addSink(mockSink))

      val job = env.execute()

      mockSink.getResults(job) === List[(EventTime, Machine, Metrics)](
        (2L, "machine", Metrics(2, 2)),
        (5L, "machine", Metrics(5, 5)),
        (8L, "machine", Metrics(7, 7))
      )
    }

  }

  "incorrect flink consumer" should {

    // This test always fail
    "account for out of order events when allowed lateness is 3" in {

      val env = StreamExecutionEnvironment.getExecutionEnvironment

      // 3 and 4 are out-of-order events.
      val eventSource = MockKafkaSource(events)

      val mockSource = env.addSource(eventSource)
      val mockSink = MockKafkaSink(accumulatorName = "accountForOutOfOrder2")

      FlinkIncorrectPipeline(allowedLateness = 3, windowSizeAndStep).build(mockSource, _.addSink(mockSink))

      val job = env.execute()

      mockSink.getResults(job) === List[(EventTime, Machine, Metrics)](
        (2L, "machine", Metrics(2, 2)),
        (5L, "machine", Metrics(5, 5)),
        (5L, "machine", Metrics(4.5, 4.5)), // late firing
        (5L, "machine", Metrics(6, 6)), // late firing
        (8L, "machine", Metrics(7, 7))
      )
    }

    // This test may or may not fail
    "drop out of order events when allowed lateness is 0" in {

      val env = StreamExecutionEnvironment.getExecutionEnvironment

      // 3 and 4 are out-of-order events.
      val eventSource = MockKafkaSource(events)

      val mockSource = env.addSource(eventSource)
      val mockSink = MockKafkaSink(accumulatorName = "discardOutOfOrder2")

      FlinkIncorrectPipeline(allowedLateness = 0, windowSizeAndStep).build(mockSource, _.addSink(mockSink))

      val job = env.execute()

      mockSink.getResults(job) === List[(EventTime, Machine, Metrics)](
        (2L, "machine", Metrics(2, 2)),
        (5L, "machine", Metrics(5, 5)),
        (8L, "machine", Metrics(7, 7))
      )
    }

  }
}
