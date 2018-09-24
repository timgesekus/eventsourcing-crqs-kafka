package de.dfs.kafkabackend

import java.util.Collections

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import cakesolutions.kafka.{KafkaConsumer, KafkaProducer, KafkaProducerRecord}
import cakesolutions.kafka.akka.{Offsets, _}
import KafkaConsumerActor.{Confirm, Subscribe}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.consumer.{ConsumerRecord, OffsetAndMetadata, OffsetResetStrategy}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import spray.json._
import DefaultJsonProtocol._
import cakesolutions.kafka.KafkaProducer.Conf
import de.dfs.acprotocol._
import de.dfs.kafkafbackend._
import de.dfs.acprotocol.AcProtocol._
import org.apache.kafka.clients.producer.ProducerRecord

import scala.concurrent.duration._

object AcService {

  val CONSUMER_GROUP_ID = "ac-service-gi"
  val AC_COMMANDS_TOPIC = "AcCommands"
  val AC_EVENTS_TOPIC = "ac-events"
  val CONSUMER_STATE_TOPIC = "consumer-state"
  /*
   * Starts an ActorSystem and instantiates the below Actor that subscribes and
   * consumes from the configured KafkaConsumerActor.
   */
  def apply(config: Config, system: ActorSystem): ActorRef = {
    val consumerConf = KafkaConsumer.Conf(
      new StringDeserializer,
      new StringDeserializer,
      groupId = CONSUMER_GROUP_ID,
      enableAutoCommit = false,

      bootstrapServers = "localhost:9092")
      .withConf(config)

    val actorConf = KafkaConsumerActor.Conf(1.seconds, 3.seconds)
    system.actorOf(Props(new AcService(consumerConf, actorConf)))
  }
}

class AcService(
  kafkaConfig: KafkaConsumer.Conf[String, String],
  actorConfig: KafkaConsumerActor.Conf) extends Actor with ActorLogging {

  case class SerializedState(ac: List[AC])
  var acs = Map[Long, AC]()

  implicit val serialzedStateFormat = jsonFormat1(SerializedState)

  private val recordsExt = ConsumerRecords.extractor[String, String]

  private val producer = KafkaProducer(
    Conf(new StringSerializer, new StringSerializer(),
      transactionalId = Some("ac-service-id"),
      enableIdempotence = true,
      bootstrapServers = "localhost:9092"))
  producer.initTransactions()
  private val consumer = context.actorOf(
    KafkaConsumerActor.props(kafkaConfig, actorConfig, self))

  consumer ! Subscribe.AutoPartition(List(AcService.AC_COMMANDS_TOPIC), assignedListener, revokedListener)

  override def receive: Receive = {
    // Records from Kafka
    case recordsExt(records) =>
      log.info("Records received")
      processRecords(records.recordsList)
      sender() ! Confirm(records.offsets)
    case _ => log.error("Unknown message received")
  }

  private def processRecords(records: List[ConsumerRecord[String, String]]) = {

    producer.beginTransaction()
    val offsets = records.map {
      record =>
        {
          log.info(s"Received [$record.key,$record.value,$record.offset]")
          val lala = record.offset
          val event = parseCommand(record.value())
          event match {
            case a: AddAc => addFlight(a.ac)
            case a: UpdateAc => updateFlight(a.ac)
            case a: DeleteAc => deleteFlight(a.id)
            case a: AcCommand => log.info("no found")
          }
          val tp = new TopicPartition(record.topic, record.partition)
          val om = new OffsetAndMetadata(record.offset + 1)
          (tp -> om)
        }
    }
    commitReads(records)
    sendState()
    producer.commitTransaction
  }

  private def addFlight(ac: AC) = {
    val record = if (!acs.contains(ac.id)) {
      acs = acs + (ac.id -> ac)
      val message = buildAcAdded(ac).toJson.compactPrint
      log.info("Sending add message")
      Some(KafkaProducerRecord(AcService.AC_EVENTS_TOPIC, ac.id.toString, message))
    } else {
      log.error("Flight existed")
      None
    }
    record.foreach(producer.send(_))
  }

  private def updateFlight(ac: AC) = {
    val record = if (acs.contains(ac.id)) {
      acs = acs.updated(ac.id, ac)
      val message = buildAcUpdated(ac).toJson.compactPrint
      log.info("Sending update message")
      Some(KafkaProducerRecord(AcService.AC_EVENTS_TOPIC, ac.id.toString, message))
    } else {
      log.error("Flight does not exits")
      None
    }
    record.foreach(producer.send(_))
  }

  private def deleteFlight(id: Long) = {
    val record = if (acs.contains(id)) {
      acs = acs - id
      val message = buildAcDeleted(id).toJson.compactPrint
      log.info("Sending remove message")
      Some(KafkaProducerRecord(AcService.AC_EVENTS_TOPIC, id.toString, message))
    } else {
      log.error("Flight does not exits")
      None
    }
    record.foreach(producer.send(_))
  }

  private def sendState(): Unit = {
    val serializedState = SerializedState(acs.values.toList)
    val stateRecord = KafkaProducerRecord(AcService.CONSUMER_STATE_TOPIC, "0", serializedState.toJson.compactPrint)
    producer.send(stateRecord)
  }

  private def commitReads(records: List[ConsumerRecord[String, String]]): Unit = {
    val offsets = records.map {
      record =>
        {
          val tp = new TopicPartition(record.topic, record.partition)
          val om = new OffsetAndMetadata(record.offset + 1)
          (tp -> om)
        }
    }
    val map = offsets.foldLeft(Map[TopicPartition, OffsetAndMetadata]()) { (m, offset) => m + offset }
    producer.sendOffsetsToTransaction(map, AcService.CONSUMER_GROUP_ID)

  }

  private def assignedListener(tps: List[TopicPartition]): Unit = {
    log.info("Partitions have been assigned" + tps.toString())

    try {
      val state = reloadState
      setState(state)
    } catch {
      case e: Throwable => log.error(e, "Reloading state failed")
    }
  }

  private def reloadState: SerializedState = {
    val consumerConf = KafkaConsumer.Conf(
      new StringDeserializer,
      new StringDeserializer,
      groupId = AcService.CONSUMER_STATE_TOPIC,
      enableAutoCommit = false,
      autoOffsetReset = OffsetResetStrategy.EARLIEST,
      bootstrapServers = "localhost:9092")

    val consumer = KafkaConsumer(consumerConf)
    val topics = consumer.listTopics().get(AcService.CONSUMER_STATE_TOPIC)
    val csTopic = topics.get(0)
    val tp = new TopicPartition(csTopic.topic, csTopic.partition)
    consumer.assign(Collections.singletonList(tp))
    consumer.seekToEnd(Collections.singletonList(tp))
    val position = consumer.position(tp)
    consumer.seek(tp, position - 2)
    val records = consumer.poll(200)
    val count = records.count()
    val record = records.iterator().next()

    val state = record.value().parseJson.convertTo[SerializedState]
    // Should load the offsets from a persistent store and any related state
    state
  }

  def setState(s: SerializedState) = {
    acs = s.ac.groupBy(_.id).map(e =>
      e._1 -> e._2.head)
  }

  private def revokedListener(tps: List[TopicPartition]): Unit = {
    log.info("Partitions have been revoked" + tps.toString())
    // Opportunity to clear any state for the revoked partitions
    ()
  }
}