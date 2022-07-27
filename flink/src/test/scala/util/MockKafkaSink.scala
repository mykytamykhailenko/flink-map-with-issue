package util

import flink.model.Metrics
import flink.util.Util.{EventTime, Machine}
import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.accumulators.ListAccumulator
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

import java.util
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

// Creates an accumulator per job.
case class MockKafkaSink(accumulatorName: String) extends RichSinkFunction[(Machine, Metrics)] {

  override def open(parameters: Configuration): Unit = {
    getRuntimeContext.addAccumulator(accumulatorName, new ListAccumulator[(EventTime, Machine, Metrics)]())
  }

  override def invoke(value: (Machine, Metrics), context: SinkFunction.Context): Unit = {
    val (machine, metrics) = value
    getRuntimeContext.getAccumulator(accumulatorName).add((context.timestamp(), machine, metrics))
  }

  def getResults(jobResult: JobExecutionResult): List[(EventTime, Machine, Metrics)] = {
    jobResult.getAccumulatorResult(accumulatorName).asInstanceOf[util.ArrayList[(EventTime, Machine, Metrics)]].toList
  }

}
