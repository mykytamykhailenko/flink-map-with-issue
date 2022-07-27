package flink.serde

import flink.model.Metrics
import flink.util.Util.Machine
import io.github.azhur.kafkaserdeplayjson.PlayJsonSupport.toSerializer
import org.apache.kafka.common.serialization.Serializer

object FlinkSerializer {

  // When using lambda code does not get serialized correctly.
  val keySer: Serializer[(Machine, Metrics)] = (topic: String, data: (Machine, Metrics)) => {
    val (machine, _) = data
    implicitly[Serializer[String]].serialize(topic, machine)
  }
  val valSer: Serializer[(Machine, Metrics)] = (topic: String, data: (Machine, Metrics)) => {
    val (_, metrics) = data
    implicitly[Serializer[Metrics]].serialize(topic, metrics)
  }

}
