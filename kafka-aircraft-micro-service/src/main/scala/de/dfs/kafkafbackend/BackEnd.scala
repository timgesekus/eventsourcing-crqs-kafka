package de.dfs.kafkabackend

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import akka.Done
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import spray.json._
import DefaultJsonProtocol._

import scala.io.StdIn
import scala.concurrent.Future
import cakesolutions.kafka._
import cakesolutions.kafka.KafkaProducer.Conf
import com.typesafe.config.ConfigFactory
import de.dfs.kafkafbackend.AC
import de.dfs.kafkafbackend.AcProtocol._
import org.apache.kafka.common.serialization.StringSerializer

//#main-class
object BackEnd {
  def main(args: Array[String]) {

    implicit val system = ActorSystem("my-system")
    implicit val materializer = ActorMaterializer()
    // needed for the future flatMap/onComplete in the end
    implicit val executionContext = system.dispatcher

    val producer = KafkaProducer(
      Conf(new StringSerializer, new StringSerializer(), bootstrapServers = "localhost:9092"))

    AcService(ConfigFactory.load, system)

    val route: Route =
      post {
        path("ac") {
          entity(as[AC]) { message =>
            val fut = producer.send(KafkaProducerRecord("AcCommands", None, buildAddAc(message)))
            onComplete(fut)(_ => complete("Ac Added"))
          }
        }
      } ~ put {
        path("ac") {
          entity(as[AC]) { message =>
            val fut = producer.send(KafkaProducerRecord("AcCommands", None, buildUpdateAc(message)))
            onComplete(fut)(_ => complete("AC updated"))
          }
        }
      } ~ delete {
        path("ac" / LongNumber) { id =>
          val fut = producer.send(KafkaProducerRecord("AcCommands", None, buildDeleteAc(id)))
          onComplete(fut)(_ => complete("AC droped"))
        }
      }
    val bindingFuture = Http().bindAndHandle(route, "0.0.0.0", 8081)

    println(s"Server online at http://localhost:8080/\nPress RETURN to sto...")
    //StdIn.readLine() // let it run until user presses return
    //bindingFuture
    // .flatMap(_.unbind()) // trigger unbinding from the port
    // .onComplete(_ => system.terminate()) // and shutdown when done
  }
}