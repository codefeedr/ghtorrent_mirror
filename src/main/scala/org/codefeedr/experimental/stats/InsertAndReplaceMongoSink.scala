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
import org.mongodb.scala.model.ReplaceOptions

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/** Inserts stats into MongoDB. */
class InsertAndReplaceMongoSink(val userConfigg: Map[String, String])
    extends BaseMongoSink[Stats](userConfigg) {

  /** Called for every stats object.
    * The _id is set equal to date, so that it will be overriden in case of an update.
    *
    * @param value the stats object.
    * @param context the sink context.
    */
  override def invoke(value: Stats, context: SinkFunction.Context[_]): Unit = {
    val collection = getCollection

    // Set _id == date.
    val json = Serialization.write(value)(formats)
    val doc = Document(json)
    doc += "_id" -> value.date

    // Make sure it replaces or inserts.
    val replaceOptions = new ReplaceOptions()
    replaceOptions.upsert(true)

    // Let's go.
    val result =
      collection.replaceOne(equal("_id", value.date), doc, replaceOptions)

    // And wait.
    Await.result(result.toFuture, Duration.Inf)
  }
}
