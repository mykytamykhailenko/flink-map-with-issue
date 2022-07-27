package flink.pipeline

import flink.model.Metrics
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala.extensions.acceptPartialFunctions
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

case class FlinkIncorrectPipeline(allowedLateness: Long, windowSize: Long) {

  def build(source: DataStream[(String, Metrics)],
            sink: DataStream[(String, Metrics)] => DataStreamSink[(String, Metrics)]): DataStreamSink[(String, Metrics)] = {

    val pipeline =
      source
        .mapWith { case (machine, metrics) => (machine, metrics, 1) }
        .keyBy(_._1)
        .window(TumblingEventTimeWindows.of(Time.milliseconds(windowSize)))
        .allowedLateness(Time.milliseconds(allowedLateness))
        .reduceWith { case ((machine, left, lc), (_, right, rc)) =>
          (machine, left + right, lc + rc)
        }
        .mapWith { case (machine, metrics, count) => machine -> (metrics / count) }


    sink(pipeline)
  }

}