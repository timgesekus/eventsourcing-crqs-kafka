package de.dfs.kafkabackend

import java.util.Collections
import java.util.logging.Logger

import akka.actor.{ Actor, ActorLogging, ActorRef, ActorSystem, Props }
import cakesolutions.kafka.KafkaConsumer
import org.apache.kafka.clients.admin
import cakesolutions.kafka.akka._
import KafkaConsumerActor.{ Confirm, Subscribe }
import com.typesafe.config.{ Config, ConfigFactory }
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer

import scala.concurrent.duration._

object StateReloader {

  /*
   * Starts an ActorSystem and instantiates the below Actor that subscribes and
   * consumes from the configured KafkaConsumerActor.
   */
  def apply(config: Config, system: ActorSystem) = {
    val consumerConf = KafkaConsumer.Conf(
      new StringDeserializer,
      new StringDeserializer,
      groupId = "consumer-state",
      enableAutoCommit = false,
      autoOffsetReset = OffsetResetStrategy.LATEST,
      bootstrapServers = "localhost:9092")
      .withConf(config)

    val consumer = KafkaConsumer(consumerConf)
    val topic = "consumer-state"

    val topics = consumer.listTopics()
    val csTopic = topics.get("consumer-state").get(0)
    val tp = new TopicPartition(csTopic.topic, csTopic.partition)
    consumer.assign(Collections.singletonList(tp))
    consumer.seekToEnd(Collections.singletonList(tp))
    val position = consumer.position(tp)
    consumer.seek(tp, position - 1)
    val record = consumer.poll(10)
    record.forEach(r => {
      println(r.value())

    })
    println("topics : {}", topics.get("consumer-state"))
  }
}

class StateReloader(
  kafkaConfig: KafkaConsumer.Conf[String, String],
  actorConfig: KafkaConsumerActor.Conf) extends Actor with ActorLogging {

  private val recordsExt = ConsumerRecords.extractor[String, String]

  private val consumer = context.actorOf(
    KafkaConsumerActor.props(kafkaConfig, actorConfig, self))

  consumer ! Subscribe.AutoPartition(List("AcCommands"), assignedListener, revokedListener)

  override def receive: Receive = {

    // Records from Kafka
    case recordsExt(records) =>
      processRecords(records.pairs)
      sender() ! Confirm(records.offsets)

  }

  private def processRecords(records: Seq[(Option[String], String)]) =
    records.foreach {
      case (key, value) =>
        log.info(s"Received [$key,$value]")
    }

  private def assignedListener(tps: List[TopicPartition]): Offsets = {
    log.info("Partitions have been assigned" + tps.toString())

    // Should load the offsets from a persistent store and any related state
    val offsetMap = tps.map { tp =>
      tp -> 0l
    }.toMap

    // Return the required offsets for the assigned partitions
    Offsets(offsetMap)
  }

  private def revokedListener(tps: List[TopicPartition]): Unit = {
    log.info("Partitions have been revoked" + tps.toString())
    // Opportunity to clear any state for the revoked partitions
    ()
  }
}