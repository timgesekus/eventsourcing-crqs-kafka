package de.dfs.acprotocol

import spray.json._
import DefaultJsonProtocol._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._

case class AC(id: Long, x: Long, y: Long)

sealed class AcCommand
case class AddAc(ac: AC) extends AcCommand
case class UpdateAc(ac: AC) extends AcCommand
case class DeleteAc(id: Long) extends AcCommand

sealed class AcEvent
case class AcAdded(ac: AC) extends AcEvent
case class AcUpdated(ac: AC) extends AcEvent
case class AcDeleted(id: Long) extends AcEvent

case class Command(commandType: String, payload: JsValue)

case class Event(eventType: String, payload: JsValue)

object AcProtocol {

  implicit val acFormat = jsonFormat3(AC)
  implicit val commandFormat = jsonFormat2(Command)
  implicit val eventFormat = jsonFormat2(Event)

  implicit val addAcFormat = jsonFormat1(AddAc)
  implicit val updpateAcFormat = jsonFormat1(UpdateAc)
  implicit val deleteAcFormat = jsonFormat1(DeleteAc)

  implicit val acAddedFormat = jsonFormat1(AcAdded)
  implicit val acUpdatedFormat = jsonFormat1(AcUpdated)
  implicit val acDeletedFormat = jsonFormat1(AcDeleted)

  def parseCommand(command: String): AcCommand = {
    val acCommand = command.parseJson.convertTo[Command]
    acCommand.commandType match {
      case "add" => acCommand.payload.convertTo[AddAc]
      case "update" => acCommand.payload.convertTo[UpdateAc]
      case "delete" => acCommand.payload.convertTo[DeleteAc]
    }
  }

  def parseEvent(event: String): AcEvent = {
    val acEvent = event.parseJson.convertTo[Event]
    acEvent.eventType match {
      case "added" => acEvent.payload.convertTo[AcAdded]
      case "updated" => acEvent.payload.convertTo[AcUpdated]
      case "deleted" => acEvent.payload.convertTo[AcDeleted]
    }
  }

  def buildAddAc(ac: AC): String = {
    val addAc = AcAdded(ac).toJson
    Command("add", addAc).toJson.compactPrint
  }

  def buildUpdateAc(ac: AC): String = {
    val addAc = AcUpdated(ac).toJson
    Command("update", addAc).toJson.compactPrint
  }

  def buildDeleteAc(id: Long): String = {
    val addAc = AcDeleted(id).toJson
    Command("delete", addAc).toJson.compactPrint
  }

  def buildAcAdded(ac: AC): String = {
    val acAdded = AcAdded(ac).toJson
    Event("add", acAdded).toJson.compactPrint
  }

  def buildAcUpdated(ac: AC): String = {
    val acUpdated = AcUpdated(ac).toJson
    Event("update", acUpdated).toJson.compactPrint
  }

  def buildAcDeleted(id: Long): String = {
    val acDeleted = AcDeleted(id).toJson
    Event("delete", acDeleted).toJson.compactPrint
  }
}
