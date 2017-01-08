/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.killrweather

import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneId, ZonedDateTime}
import java.util.{Calendar, Date}

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.pattern.pipe
import com.cloudera.sparkts.{DateTimeIndex, MinuteFrequency, Resample, TimeSeriesRDD}
import com.datastax.spark.connector._
import com.datastax.spark.connector.types.TimestampParser
import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.joda.time.DateTime

import scala.concurrent.Future

/** The TemperatureActor reads the daily temperature rollup data from Cassandra,
  * and for a given weather station, computes temperature statistics by month for a given year.
  */
class MeasureActor(sc: SparkContext, settings: WeatherSettings)
  extends AggregationActor with ActorLogging with Serializable {

  import Weather._
  import WeatherEvent._
  import settings.{CassandraGreenHouseKeySpace => greenhouseKeyspace, CassandraGreenhouseTableRaw => greenhouseRawtable, CassandraGreenhouseTableSync => greenhouseSynctable}

  def receive: Actor.Receive = {
    /** Greenhouse specific */
    case e: GetMeasurePerRange         => range(e.gardenApiKey, e.sensorSlug, e.startDate, e.endDate, sender)
    case e: AlignMeasurePerRange       => align(e.gardenApiKey, e.sensorSlug, e.startDate, e.endDate, sender)
    /*case e: GetDailyTemperature        => daily(e.day, sender)
    case e: DailyTemperature           => store(e)
    case e: GetMonthlyHiLowTemperature => highLow(e, sender)*/
  }

  /** Retrieves measures given a garden api key, a sensor and an interval
    *
    * @param gardenApiKey the gardenApiKey
    * @param sensorSlug the sensorSlug
    * @param startDate the startDate
    * @param endDate the endDate
    * @param requester the requester to be notified
    */
  def range(gardenApiKey: String, sensorSlug: String, startDate: DateTime, endDate: DateTime, requester: ActorRef): Unit = {
    var years = "("
    for (i <- startDate.getYear() to endDate.getYear()) {
      years += ("'" + i.toString() + "', ")
    }
    years = years.dropRight(2)
    years += ")"

    sc.cassandraTable[Measure](greenhouseKeyspace, greenhouseRawtable)
      .select(
        "year",
        "event_time",
        "value",
        "garden_api_key",
        "sensor_slug",
        "user_id")
      .where("year in " + years + " AND garden_api_key = ? AND sensor_slug = ? AND event_time >= ? AND event_time <= ?",
        gardenApiKey,
        sensorSlug,
        startDate.toString(),
        endDate.toString())
      .collectAsync() pipeTo requester
  }

  /** Retrieves, aligns and store measures given a garden api key, a sensor and an interval so that time series are enabled
    * for comparisons.
    *
    * @param gardenApiKey the gardenApiKey
    * @param sensorSlug the sensorSlug
    * @param startDate the startDate
    * @param endDate the endDate
    * @param requester the requester to be notified
    */
  def align(gardenApiKey: String, sensorSlug: String, startDate: String, endDate: String, requester: ActorRef): Unit = {
    val localStartDate = LocalDateTime.parse(startDate)
    val localEndDate = LocalDateTime.parse(endDate)

    var years = "("
    for (i <- localStartDate.getYear() to localEndDate.getYear()) {
      years += ("'" + i.toString() + "', ")
    }
    years = years.dropRight(2)
    years += ")"

    val rowRDD = sc.cassandraTable[Measure](greenhouseKeyspace, greenhouseRawtable)
      .select(
        "year",
        "event_time",
        "value",
        "garden_api_key",
        "sensor_slug",
        "user_id")
      .where("year in " + years + " AND garden_api_key = ? AND sensor_slug = ? AND event_time >= ? AND event_time <= ?",
        gardenApiKey,
        sensorSlug,
        startDate.toString(),
        endDate.toString())
      .map(measure => Row(new Timestamp(TimestampParser.parse(measure.eventTime).getTime), measure.sensorSlug, measure.value))

    val sqlContext = new SQLContext(sc)

    // 1. creating time series for resampling with real timestamps
    // WARNING: we were not able to use TimeSeriesRDD.resample at this point
    //          indeed a spark Serialization exception is thrown

    val dataframe = sqlContext.createDataFrame(rowRDD, StructType(Seq(
      StructField("event_time", TimestampType, true),
      StructField("sensor_slug", StringType, true),
      StructField("value", DoubleType, true)
    )))
    val zone = ZoneId.systemDefault()
    val originIndex = DateTimeIndex.irregular(rowRDD.map(row => {
      row.getTimestamp(0).getTime * 1000000
    }).collect())
    val timeSeriesRdd = TimeSeriesRDD.timeSeriesRDDFromObservations(
      originIndex,
      dataframe,
      "event_time", "sensor_slug", "value")

    // 2. resampling time series with closest quarters

    def aggr(arr: Array[Double], start: Int, end: Int) = {
      arr.slice(start, end).sum
    }

    val firstVector = timeSeriesRdd.values.first() // WARNING: no other vector must exist at this point, only one sensor is provided
    val resampledVector = Resample.resample(firstVector, originIndex, DateTimeIndex.uniformFromInterval(
      ZonedDateTime.of(localStartDate, zone),
      ZonedDateTime.of(localEndDate, zone),
      new MinuteFrequency(15)), aggr, false, false)

    var measures: Seq[Measure] = Seq()
    val leftDate = roundDateToClosestQuarter(new Date(rowRDD.first().getTimestamp(0).getTime))
    val calendar = Calendar.getInstance()
    for(i <- 0 to resampledVector.size - 1) {
      leftDate.setTime(leftDate.getTime + 60000 * 15)
      calendar.setTime(leftDate)
      measures = measures :+ Measure(calendar.get(Calendar.YEAR).toString, leftDate.toInstant.toString, resampledVector(i), gardenApiKey, sensorSlug, 0)
    }

    // 3. Storing aligned data
    store(measures)

    Future() pipeTo requester
  }

  /** Stores the daily temperature aggregates asynchronously which are triggered
    * by on-demand requests during the `forDay` function's `self ! data`
    * to the daily temperature aggregation table.
    */
  private def store(e: Seq[Measure]): Unit =
    sc.parallelize(e).saveToCassandra(greenhouseKeyspace, greenhouseSynctable)

  private def roundDateToClosestQuarter(whateverDateYouWant: Date): Date = {
    val calendar = Calendar.getInstance()
    calendar.setTime(whateverDateYouWant)

    val unroundedMinutes = calendar.get(Calendar.MINUTE)
    val mod: Int = unroundedMinutes % 15
    calendar.add(Calendar.MINUTE, if(mod < 8) -mod else (15 - mod))
    calendar.set(Calendar.SECOND, 0)
    calendar.set(Calendar.MILLISECOND, 0)

    calendar.getTime
  }
}
