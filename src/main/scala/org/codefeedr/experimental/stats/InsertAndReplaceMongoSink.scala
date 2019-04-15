package org.codefeedr.experimental.stats

import com.mongodb.client.model.UpdateOptions
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{
  RichSinkFunction,
  SinkFunction
}
import org.codefeedr.experimental.stats.StatsObjects.Stats
import org.codefeedr.plugins.mongodb.BaseMongoSink
import org.json4s.jackson.Serialization
import org.mongodb.scala.bson.collection.mutable.Document
import org.mongodb.scala.result
import org.mongodb.scala.model.Filters._

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class InsertAndReplaceMongoSink(val userConfigg: Map[String, String])
    extends BaseMongoSink[Stats](userConfigg) {

  override def invoke(value: Stats, context: SinkFunction.Context[_]): Unit = {
    val collection = getCollection

    val json = Serialization.write(value)(formats)
    val doc = Document(json)
    doc += "_id" -> value.date

    val updateOptions = new UpdateOptions()
    updateOptions.upsert(true)

    val result =
      collection.updateOne(equal("_id", value.date), doc, updateOptions)

    Await.result(result.toFuture, Duration.Inf)
  }
}
