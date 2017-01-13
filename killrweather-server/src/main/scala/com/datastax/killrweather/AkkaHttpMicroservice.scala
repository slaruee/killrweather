package com.datastax.killrweather;

import java.time.OffsetDateTime
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.event.{Logging, LoggingAdapter}
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.server.Directives._
import akka.pattern.ask
import akka.stream.{ActorMaterializer, Materializer}
import akka.util.Timeout
import com.datastax.killrweather.Weather.Measure
import com.typesafe.config.{Config, ConfigFactory}
import spray.json.{DefaultJsonProtocol, DeserializationException, JsArray, JsString, JsValue, RootJsonFormat}

import scala.collection.mutable
import scala.concurrent.{Await, ExecutionContextExecutor, Future}

case class MeasureRequest(gardenApiKey: String, sensorSlugs: Array[String], startDate: String, endDate: String)
case class AlignMeasureRequest(gardenApiKey: String, sensorSlugs: Array[String], startDate: String, endDate: String)

trait Protocols extends DefaultJsonProtocol {
  implicit object measureRequestFormat extends RootJsonFormat[MeasureRequest] {
    def write(r: MeasureRequest) =
      throw new NotImplementedError()

    def read(value: JsValue) = {
      value.asJsObject.getFields("gardenApiKey", "sensorSlugs", "startDate", "endDate") match {
        case Seq(JsString(gardenApiKey), JsArray(sensorSlugs), JsString(startDate), JsString(endDate)) =>
          new MeasureRequest(gardenApiKey, sensorSlugs.map(value => String.valueOf(value).replaceAll("\"", "")).toArray[String], startDate, endDate)
        case _ => throw new DeserializationException("MeasureRequest expected")
      }
    }
  }

  implicit val measureFormat = jsonFormat6(Measure.apply)

  implicit object alignMeasureRequestFormat extends RootJsonFormat[AlignMeasureRequest] {
    def write(r: AlignMeasureRequest) =
      throw new NotImplementedError()

    def read(value: JsValue) = {
      value.asJsObject.getFields("gardenApiKey", "sensorSlugs", "startDate", "endDate") match {
        case Seq(JsString(gardenApiKey), JsArray(sensorSlugs), JsString(startDate), JsString(endDate)) =>
          new AlignMeasureRequest(gardenApiKey, sensorSlugs.map(value => String.valueOf(value).replaceAll("\"", "")).toArray[String], startDate, endDate)
        case _ => throw new DeserializationException("MeasureRequest expected")
      }
    }
  }
}

trait Service extends Protocols {
  implicit val system: ActorSystem
  implicit def executor: ExecutionContextExecutor
  implicit val materializer: Materializer
  implicit val killrWeather: KillrWeather

  def config: Config
  val logger: LoggingAdapter

  def getMeasurePerRange(gardenApiKey: String, sensorSlugs: Array[String], startDate: String, endDate: String): Future[Either[String, Array[Measure]]] = {
    implicit val timeout = Timeout(15, TimeUnit.SECONDS)
    val future = ask(killrWeather.guardian, WeatherEvent.GetMeasurePerRange(
      gardenApiKey,
      sensorSlugs,
      OffsetDateTime.parse(startDate),
      OffsetDateTime.parse(endDate)))
    val result = Await.result(future, timeout.duration).asInstanceOf[mutable.WrappedArray[Measure]].toArray[Measure]

    Future[Either[String, Array[Measure]]] (
      Right(result)
    )
  }

  def alignMeasurePerRange(gardenApiKey: String, sensorSlugs: Array[String], startDate: String, endDate: String): Future[String] = {
    implicit val timeout = Timeout(2, TimeUnit.MINUTES)
    val future = ask(killrWeather.guardian, WeatherEvent.AlignMeasurePerRange(
      gardenApiKey,
      sensorSlugs,
      OffsetDateTime.parse(startDate),
      OffsetDateTime.parse(endDate)))
    // TODO: replace with a timeout value
    val result = Await.result(future, timeout.duration)

    Future[String] (
      "time series aligned!"
    )
  }

  def aggregateDailyDistancePerRange(gardenApiKey: String, sensorSlugs: Array[String], startDate: String, endDate: String): Future[String] = {
    implicit val timeout = Timeout(2, TimeUnit.MINUTES)
    val future = ask(killrWeather.guardian, WeatherEvent.AggregateDailyDistancePerRange(
      gardenApiKey,
      sensorSlugs,
      OffsetDateTime.parse(startDate),
      OffsetDateTime.parse(endDate)))
    // TODO: replace with a timeout value
    val result = Await.result(future, timeout.duration)

    Future[String] (
      "daily distance aggregated!"
    )
  }

  val routes = {
    logRequestResult("akka-http-microservice") {
      pathPrefix("get-measure-per-range") {
        (post & entity(as[MeasureRequest])) { measureRequest =>
          complete {
            getMeasurePerRange(measureRequest.gardenApiKey, measureRequest.sensorSlugs, measureRequest.startDate, measureRequest.endDate)
          }
        }
      } ~ pathPrefix("align-measure-per-range") {
        (post & entity(as[AlignMeasureRequest])) { measureRequest =>
          complete {
            alignMeasurePerRange(measureRequest.gardenApiKey, measureRequest.sensorSlugs, measureRequest.startDate, measureRequest.endDate)
          }
        }
      } ~ pathPrefix("aggregate-daily-distance-per-range") {
        (post & entity(as[AlignMeasureRequest])) { measureRequest =>
          complete {
            aggregateDailyDistancePerRange(measureRequest.gardenApiKey, measureRequest.sensorSlugs, measureRequest.startDate, measureRequest.endDate)
          }
        }
      }
    }
  }
}

object AkkaHttpMicroservice extends App with Service {
  override implicit val system = ActorSystem()
  override implicit val executor = system.dispatcher
  override implicit val materializer = ActorMaterializer()

  override val config = ConfigFactory.load()
  override val logger = Logging(system, getClass)

  val settings = new WeatherSettings

  /** Creates the ActorSystem. */
  val killrWeather = KillrWeather(system)

  Http().bindAndHandle(routes, config.getString("http.interface"), config.getInt("http.port"))
}
