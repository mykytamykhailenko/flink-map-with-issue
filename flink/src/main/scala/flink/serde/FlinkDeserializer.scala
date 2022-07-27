package flink.serde

import flink.model.Metrics
import flink.util.Util.{EventTime, Machine}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerRecord
import play.api.libs.json.{Json, Reads}

object FlinkDeserializer extends KafkaRecordDeserializationSchema[(EventTime, Machine, Metrics)] {

  private[this] def deserializeInputByteArray[T](input: Array[Byte])(implicit reads: Reads[T]): T = reads.reads(Json.parse(input)).get

  def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]], out: Collector[(EventTime, Machine, Metrics)]): Unit = {

    val key = deserializeInputByteArray[String](record.key())
    val value = deserializeInputByteArray[Metrics](record.value())

    out.collect((record.timestamp(), key, value))
  }

  def getProducedType: TypeInformation[(EventTime, Machine, Metrics)] = TypeInformation.of(classOf[(EventTime, Machine, Metrics)])

}
