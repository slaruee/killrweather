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

import akka.actor.Actor
import org.apache.spark.streaming.StreamingContext
import kafka.server.KafkaConfig
import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.embedded.EmbeddedKafka
import com.datastax.killrweather.actor.{KafkaProducer, WeatherActor}

/** 2. The RawDataPublisher transforms annual weather .gz files to line data and publishes to a Kafka topic.
  *
  * This just batches one data file for the demo. But you could do something like this
  * to set up a monitor on an S3 bucket, so that when new files arrive, Spark streams
  * them in. New files are read as text files with 'textFileStream' (using key as LongWritable,
  * value as Text and input format as TextInputFormat)
  * {{{
  *   streamingContext.textFileStream(dir)
       .reduceByWindow(_ + _, Seconds(30), Seconds(1))
  * }}}
  */
class RawDataPublisher(val config: KafkaConfig, ssc: StreamingContext, settings: WeatherSettings) extends KafkaProducer {
  import settings._
  import WeatherEvent._

  def receive : Actor.Receive = {
    case PublishFeed =>
      ssc.textFileStream(DataLoadPath).flatMap(_.split("\\n"))
        .map(send(KafkaTopicRaw, KafkaGroupId, _))
  }
}

/** 3. The KafkaStreamActor elegantly creates a streaming pipeline from Kafka to Cassandra via Spark.
  * It creates the Kafka stream which streams the raw data, transforms it, to
  * a column entry for a specific weather station[[com.datastax.killrweather.Weather.RawWeatherData]],
  * and saves the new data to the cassandra table as it arrives.
  */
class KafkaStreamActor(kafka: EmbeddedKafka, ssc: StreamingContext, settings: WeatherSettings) extends WeatherActor {
  import settings._
  import Weather._

  /* Creates the Kafka stream and defines the work to be done. */
  val stream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
    ssc, kafka.kafkaParams, Map(KafkaTopicRaw -> 1), StorageLevel.MEMORY_ONLY)

  stream.map { case (_,d) => d.split(",")}
    .map (RawWeatherData(_))
    .saveToCassandra(CassandraKeyspace, CassandraTableRaw)

  ssc.start()

  def receive : Actor.Receive = {
    case _ =>
  }
}