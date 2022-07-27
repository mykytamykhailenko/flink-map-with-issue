package flink.pipeline

import flink.model.Metrics
import flink.util.Util.Machine
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, createTypeInformation}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

case class FlinkCorrectPipeline(allowedLateness: Long, windowSize: Long)  {

  def build(source: DataStream[(Machine, Metrics)],
            sink: DataStream[(Machine, Metrics)] => DataStreamSink[(Machine, Metrics)]): DataStreamSink[(Machine, Metrics)] = {

    val averageMetrics = new ProcessWindowFunction[(Machine, Metrics), (Machine, Metrics), String, TimeWindow]  {

      override def process(key: String,
                           context: Context,
                           elements: Iterable[(Machine, Metrics)],
                           out: Collector[(Machine, Metrics)]): Unit = {

        val noMetrics = (key, Metrics(0, 0), 0)

        // Sums metrics up and counts the number of samples.
        val (machine, totalMetrics, totalSamples) = elements.foldRight(noMetrics) {
          case ((_, metrics), (machine, accMetrics, accSamples)) =>
            (machine, accMetrics + metrics, accSamples + 1)
        }

        out.collect((machine, totalMetrics / totalSamples))
      }
    }

    val pipeline =
      source
        .keyBy(_._1)
        .window(TumblingEventTimeWindows.of(Time.milliseconds(windowSize)))
        .allowedLateness(Time.milliseconds(allowedLateness))
        .process(averageMetrics)

    sink(pipeline)
  }

}
