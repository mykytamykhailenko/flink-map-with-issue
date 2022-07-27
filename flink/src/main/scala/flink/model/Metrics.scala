package flink.model

import play.api.libs.json.{Format, Json}

case class Metrics(cpu: BigDecimal,
                   ram: BigDecimal) {

  def +(other: Metrics): Metrics = Metrics(other.cpu + cpu, other.ram + ram)

  def /(count: Int): Metrics = Metrics(cpu / count, ram / count)

}

object Metrics {

  implicit val metricsFormat: Format[Metrics] = Json.format[Metrics]

}
