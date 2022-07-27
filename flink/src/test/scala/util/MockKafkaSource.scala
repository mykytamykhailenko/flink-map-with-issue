package util

import flink.util.Util.{EventTime, Machine}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.watermark.Watermark
import flink.model.Metrics

import scala.annotation.tailrec

// Sends events downstream while propagating ascending watermarks.
// Behaves like punctuated watermark strategy.
case class MockKafkaSource(records: Seq[(EventTime, Machine, Metrics)]) extends SourceFunction[(Machine, Metrics)] {

  def run(context: SourceFunction.SourceContext[(Machine, Metrics)]): Unit = {

    @tailrec
    def emit(events: Seq[(EventTime, Machine, Metrics)], latestEvent: Long): Unit =
      events match {
        case (eventTime, machine, metrics) +: otherEvents =>
          val mark = Math.max(eventTime, latestEvent)

          context.collectWithTimestamp(machine -> metrics, eventTime)
          context.emitWatermark(new Watermark(mark))

          emit(otherEvents, mark)
        case Nil =>
      }

    emit(records, Long.MinValue)
  }

  def cancel(): Unit = ()

}
