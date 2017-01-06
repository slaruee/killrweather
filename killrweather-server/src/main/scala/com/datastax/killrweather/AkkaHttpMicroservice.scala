package com.datastax.killrweather;

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
import org.joda.time.DateTime
import spray.json.DefaultJsonProtocol

import scala.collection.mutable
import scala.concurrent.{Await, ExecutionContextExecutor, Future}

/** Greenhouse specific */
case class MeasureRequest(gardenApiKey: String, sensorSlug: String, startDate: String, endDate: String)

trait Protocols extends DefaultJsonProtocol {
  /** Greenhouse specific */
  implicit val measureRequestFormat = jsonFormat4(MeasureRequest.apply)
  implicit val measureFormat = jsonFormat6(Measure.apply)
}

trait Service extends Protocols {
  implicit val system: ActorSystem
  implicit def executor: ExecutionContextExecutor
  implicit val materializer: Materializer
  implicit val killrWeather: KillrWeather

  def config: Config
  val logger: LoggingAdapter

  def getMeasurePerRange(gardenApiKey: String, sensorSlug: String, startDate: String, endDate: String): Future[Either[String, Array[Measure]]] = {
    implicit val timeout = Timeout(15, TimeUnit.SECONDS)
    val future = ask(killrWeather.guardian, WeatherEvent.GetMeasurePerRange(
      gardenApiKey,
      sensorSlug,
      DateTime.parse(startDate),
      DateTime.parse(endDate)))
    val result = Await.result(future, timeout.duration).asInstanceOf[mutable.WrappedArray[Measure]].toArray[Measure]

    Future[Either[String, Array[Measure]]] (
      Right(result)
    )
  }

  def alignMeasurePerRange(gardenApiKey: String, sensorSlug: String, startDate: String, endDate: String): Future[String] = {
    implicit val timeout = Timeout(15, TimeUnit.SECONDS)
    val future = ask(killrWeather.guardian, WeatherEvent.AlignMeasurePerRange(
      gardenApiKey,
      sensorSlug,
      startDate,
      endDate))
    val result = Await.result(future, timeout.duration)

    Future[String] (
      "aligned!"
    )
  }

  val routes = {
    logRequestResult("akka-http-microservice") {
      pathPrefix("get-measure-per-range") {
        (post & entity(as[MeasureRequest])) { measureRequest =>
          complete {
            getMeasurePerRange(measureRequest.gardenApiKey, measureRequest.sensorSlug, measureRequest.startDate, measureRequest.endDate)
          }
        }
      } ~ pathPrefix("align-measure-per-range") {
        (post & entity(as[MeasureRequest])) { measureRequest =>
          complete {
            alignMeasurePerRange(measureRequest.gardenApiKey, measureRequest.sensorSlug, measureRequest.startDate, measureRequest.endDate)
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
