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
import java.time._
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.Calendar

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.pattern.pipe
import com.cloudera.sparkts.{DateTimeIndex, MinuteFrequency, TimeSeriesRDD}
import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd.ClusteringOrder
import com.datastax.spark.connector.types.TimestampParser
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

import scala.concurrent.Future

/** The MeasureActor reads the measures rollup data from Cassandra,
  * and for a given garden aligns measures data for a given interval.
  */
class MeasureActor(sc: SparkContext, settings: WeatherSettings)
  extends AggregationActor with ActorLogging with Serializable {

  import Weather._
  import WeatherEvent._
  import settings.{CassandraGreenHouseKeySpace => greenhouseKeyspace, CassandraGreenhouseTableRaw => greenhouseRawtable, CassandraGreenhouseTableSync => greenhouseSynctable}

  def receive: Actor.Receive = {
    case e: GetMeasurePerRange         => range(e.gardenApiKey, e.sensorSlugs, e.startDate, e.endDate, sender)
    case e: AlignMeasurePerRange       => align(e.gardenApiKey, e.sensorSlugs, e.startDate, e.endDate, sender)
  }

  /** Retrieves measures given a garden and an interval
    *
    * @param gardenApiKey the gardenApiKey
    * @param startDate the startDate
    * @param endDate the endDate
    * @param requester the requester to be notified
    */
  def range(gardenApiKey: String, sensorSlugs: Array[String], startDate: OffsetDateTime, endDate: OffsetDateTime, requester: ActorRef): Unit = {
    val months = concatenateMonths(startDate.toLocalDateTime, endDate.toLocalDateTime)
    val slugs = concatenateSlugs(sensorSlugs)

    sc.cassandraTable[Measure](greenhouseKeyspace, greenhouseRawtable)
      .select(
        "month",
        "event_time",
        "value",
        "garden_api_key",
        "sensor_slug",
        "user_id")
      .where("month in " + months + " AND sensor_slug in " + slugs + " AND garden_api_key = ? AND event_time >= ? AND event_time <= ?",
        gardenApiKey,
        startDate.toString(),
        endDate.toString())
      .clusteringOrder(ClusteringOrder.Descending)
      .collectAsync() pipeTo requester
  }

  /** Retrieves, aligns and store measures given a garden api key, a collection of sensors' slugs and an interval so that
    * time series are enabled for comparisons.
    *
    * @param gardenApiKey the gardenApiKey
    * @param sensorSlugs the collection of sensors' slugs
    * @param startDate the startDate
    * @param endDate the endDate
    * @param requester the requester to be notified
    */
  def align(gardenApiKey: String, sensorSlugs: Array[String], startDate: OffsetDateTime, endDate: OffsetDateTime, requester: ActorRef): Unit = {
    val localStartDate = startDate.toLocalDateTime
    val localEndDate = endDate.toLocalDateTime

    val months = concatenateMonths(localStartDate, localEndDate)
    val slugs = concatenateSlugs(sensorSlugs)

    val rowRDD = sc.cassandraTable[Measure](greenhouseKeyspace, greenhouseRawtable)
      .select(
        "month",
        "event_time",
        "value",
        "garden_api_key",
        "sensor_slug",
        "user_id")
      .where("month in " + months + " AND sensor_slug in " + slugs + " AND garden_api_key = ? AND event_time >= ? AND event_time <= ?",
        gardenApiKey,
        startDate.toString(),
        endDate.toString())
      .map(measure => Row(new Timestamp(TimestampParser.parse(measure.eventTime).getTime), measure.sensorSlug, measure.value))
    // WARNING: assert ordering of values below

    val sqlContext = new SQLContext(sc)

    // 1. creating time series for resampling with real timestamps
    // WARNING: we were not able to use TimeSeriesRDD.resample at this point
    //          indeed a spark Serialization exception is thrown

    val dataframe = sqlContext.createDataFrame(rowRDD, StructType(Seq(
      StructField("event_time", TimestampType, true),
      StructField("sensor_slug", StringType, true),
      StructField("value", DoubleType, true)
    )))
    val originIndex = DateTimeIndex.irregular(rowRDD.map(row => {
      row.getTimestamp(0).getTime * 1000000
    }).collect())
    val timeSeriesRdd = TimeSeriesRDD.timeSeriesRDDFromObservations(
      originIndex,
      dataframe,
      "event_time", "sensor_slug", "value")

    // 2. resampling time series with closest quarters

    /**
      * Unsampling callback
      * We keep last event - and do not aggregate - since we are unsampling (adding more values than existing)
      */
    def last(arr: Array[Double], start: Int, end: Int) = {
      arr(if(start < arr.length) start else start - 1)
    }

    // WARNING: zone is very important below since server zone can be different from client one
    // TODO: assert that startDate and endDate have the same zone id?
    val zone = ZoneId.of(startDate.getOffset.getId)
    var measures: Seq[Measure] = Seq()

    val tsSensorSlugs = timeSeriesRdd.keys
    for (i <- 0 until timeSeriesRdd.values.count().toInt) {
      val currSensorSlug = tsSensorSlugs(i)
      // WARNING: removeInstantsWithNaNs seems to lead to java.lang.ArrayIndexOutOfBoundsException with several sensors
      val currSensorVector = timeSeriesRdd.filter(series => {
        series._1.equals(currSensorSlug)
      }).removeInstantsWithNaNs().values.first()
      // WARNING: no other vector must exist at this point, only one sensor is provided
      val currResampledVector = Resample.resample(currSensorVector,
        DateTimeIndex.irregular(rowRDD
          .filter(row => row.getString(1).equals(currSensorSlug))
          .map(row => {
            row.getTimestamp(0).getTime * 1000000
          }).collect()),
        DateTimeIndex.uniformFromInterval(
          roundDateToClosestQuarter(originIndex.first),
          roundDateToClosestQuarter(originIndex.last),
          new MinuteFrequency(15)),
        last, false, false)

      val leftDate = roundDateToClosestQuarter(originIndex.first)
      for (j <- 0 until currResampledVector.size) {
        val nextDate = leftDate.plus(j * 15, ChronoUnit.MINUTES).toInstant.atZone(zone)
        measures = measures :+ Measure(
          nextDate.getYear.toString + "/" + f"${nextDate.getMonthValue}%02d",
          nextDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZ")) /* ISO format */,
          currResampledVector(j),
          gardenApiKey,
          currSensorSlug,
          0)
      }
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

  /** Builds a months string for cassandra query
    *
    * @param startDate a start date
    * @param endDate an end date
    * @tparam D a LocalDateTime
    * @return a list of months formatted for cassandra
    */
  private def concatenateMonths[D <: LocalDateTime](startDate: D, endDate: D): String = {
    var months = "("
    var amount = startDate.until(endDate, ChronoUnit.MONTHS);
    if (startDate.getMonth != endDate.getMonth) {
      amount += 1
    }
    for (i <- 0 to amount.toInt) {
      // TODO: optimize addition below which could be called once
      val nextDate = startDate.plus(i, ChronoUnit.MONTHS)
      months += ("'" + nextDate.getYear.toString + "/" + f"${nextDate.getMonthValue}%02d" + "', ")
    }
    months = months.dropRight(2)
    months += ")"

    return months
  }

  /** Builds a sensors' slugs string for cassandra query
    *
    * @param sensorSlugs a collection of sensors' slugs
    * @return a collection of sensors' slugs formatted for cassandra
    */
  private def concatenateSlugs(sensorSlugs: Array[String]): String = {
    var slugs = "("
    for (i <- 0 until sensorSlugs.length) {
      slugs += ("'" + sensorSlugs(i) + "', ")
    }
    slugs = slugs.dropRight(2)
    slugs += ")"

    return slugs
  }

  /**
    * Rounds date to closest quarter.
    *
    * @param date the date
    * @return the rounded date
    */
  private def roundDateToClosestQuarter(date: ZonedDateTime): ZonedDateTime = {
    val calendar = Calendar.getInstance()
    calendar.setTimeInMillis(date.toInstant.toEpochMilli)

    val unroundedMinutes = calendar.get(Calendar.MINUTE)
    val mod: Int = unroundedMinutes % 15
    calendar.add(Calendar.MINUTE, if(mod < 8) -mod else (15 - mod))
    calendar.set(Calendar.SECOND, 0)
    calendar.set(Calendar.MILLISECOND, 0)

    TimeSeriesUtils.longToZonedDateTime(calendar.getTimeInMillis * 1000000, ZoneId.of(calendar.getTimeZone.getID))
  }
}

// copy - pasted from spark-timeseries/src/main/scala/com/cloudera/sparkts/TimeSeriesUtils.scala
/**
  * Internal utilities for dealing with 1-D time series.
  */
object TimeSeriesUtils {
  def longToZonedDateTime(dt: Long, zone: ZoneId = ZoneId.systemDefault()): ZonedDateTime = {
    ZonedDateTime.ofInstant(Instant.ofEpochSecond(dt / 1000000000L, dt % 1000000000L), zone)
  }

  def zonedDateTimeToLong(dt: ZonedDateTime): Long = {
    val secondsInNano = dt.toInstant().getEpochSecond() * 1000000000L
    secondsInNano + dt.getNano()
  }
}

// copy - pasted from spark-timeseries/src/main/scala/com/cloudera/sparkts/Resample.scala
object Resample {
  /**
    * Converts a time series to a new date-time index, with flexible semantics for aggregating
    * observations when downsampling.
    *
    * Based on the closedRight and stampRight parameters, resampling partitions time into non-
    * overlapping intervals, each corresponding to a date-time in the target index. Each resulting
    * value in the output series is determined by applying an aggregation function over all the
    * values that fall within the corresponding window in the input series. If no values in the
    * input series fall within the window, a NaN is used.
    *
    * Compare with the equivalent functionality in Pandas:
    * http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.resample.html
    *
    * @param ts The values of the input series.
    * @param sourceIndex The date-time index of the input series.
    * @param targetIndex The date-time index of the resulting series.
    * @param aggr Function for aggregating multiple points that fall within a window.
    * @param closedRight If true, the windows are open on the left and closed on the right. Otherwise
    *                    the windows are closed on the left and open on the right.
    * @param stampRight If true, each date-time in the resulting series marks the end of a window.
    *                   This means that all observations after the end of the last window will be
    *                   ignored. Otherwise, each date-time in the resulting series marks the start of
    *                   a window. This means that all observations after the end of the last window
    *                   will be ignored.
    * @return The values of the resampled series.
    */
  def resample(
                ts: Vector,
                sourceIndex: DateTimeIndex,
                targetIndex: DateTimeIndex,
                aggr: (Array[Double], Int, Int) => Double,
                closedRight: Boolean,
                stampRight: Boolean): Vector = {
    val tsarr = ts.toArray
    val result = new Array[Double](targetIndex.size)
    val sourceIter = sourceIndex.nanosIterator().buffered
    val targetIter = targetIndex.nanosIterator().buffered

    // Values within interval corresponding to stamp "c" (with next stamp at "n")
    //
    // !closedRight && stampRight:
    // 1 2 3 4
    //         c
    //
    // !closedRight && !stampRight:
    // 1 2 3 4
    // c       n
    //
    // closedRight && stampRight:
    // 1 2 3 4
    //       c
    //
    // closedRight && !stampRight
    //   1 2 3 4
    // c       n

    // End predicate should return true iff dt falls after the window labeled by cur DT (at i)
    val endPredicate: (Long, Long, Long) => Boolean = if (!closedRight && stampRight) {
      (cur, next, dt) => dt >= cur
    } else if (!closedRight && !stampRight) {
      (cur, next, dt) => dt >= next
    } else if (closedRight && stampRight) {
      (cur, next, dt) => dt > cur
    } else {
      (cur, next, dt) => dt > next
    }

    var i = 0 // index in result array
    var j = 0 // index in source array

    // Skip observations that don't belong with any stamp
    if (!stampRight) {
      val firstStamp = targetIter.head
      while (sourceIter.head < firstStamp || (closedRight && sourceIter.head == firstStamp)) {
        sourceIter.next()
        j += 1
      }
    }

    // Invariant is that nothing lower than j should be needed to populate result(i)
    while (i < result.length) {
      val cur = targetIter.next()
      val next = if (targetIter.hasNext) targetIter.head else Long.MaxValue
      val sourceStartIndex = j

      while (sourceIter.hasNext && !endPredicate(cur, next, sourceIter.head)) {
        sourceIter.next()
        j += 1
      }
      val sourceEndIndex = j

      // Urban Potager specific code below
      //if (sourceStartIndex == sourceEndIndex) {
      //  result(i) = Double.NaN
      //} else {
        result(i) = aggr(tsarr, sourceStartIndex, sourceEndIndex)
      //}

      i += 1
    }
    Vectors.dense(result)
  }
}
