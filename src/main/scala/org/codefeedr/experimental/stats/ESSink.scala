package org.codefeedr.experimental.stats

import java.net.URI
import java.nio.charset.StandardCharsets

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.connectors.elasticsearch.{
  ActionRequestFailureHandler,
  ElasticsearchSinkFunction,
  RequestIndexer
}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.util.ExceptionUtils
import org.apache.http.HttpHost
import org.apache.logging.log4j.scala.Logging
import org.codefeedr.plugins.elasticsearch.stages.ElasticSearchSink
import org.codefeedr.stages.OutputStage
import org.elasticsearch.ElasticsearchParseException
import org.elasticsearch.action.ActionRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException
import org.elasticsearch.common.xcontent.XContentType
import org.json4s.FieldSerializer.{renameFrom, renameTo}
import org.json4s.ext.JavaTimeSerializers
import org.json4s.jackson.Serialization
import org.json4s.{FieldSerializer, NoTypeHints}

import scala.reflect.{ClassTag, Manifest}

class ESSink[T <: Serializable with AnyRef: ClassTag: Manifest](
    index: String,
    stageId: String = "es_output",
    servers: Set[String] = Set(),
    config: Map[String, String] = Map())
    extends OutputStage[T](Some(stageId))
    with Logging {

  //TODO Add configuration support
  override def main(source: DataStream[T]): Unit = {
    val config = createConfig()
    val transportAddresses = createTransportAddresses()

    val eSinkBuilder = new ElasticsearchSink.Builder[T](
      transportAddresses,
      new ElasticSearchSink(index))

    eSinkBuilder.setBulkFlushMaxActions(1)
    eSinkBuilder.setFailureHandler(new ActionRequestFailureHandler {
      override def onFailure(action: ActionRequest,
                             failure: Throwable,
                             restStatusCode: Int,
                             indexer: RequestIndexer): Unit = {
        if (ExceptionUtils
              .findThrowable(failure, classOf[EsRejectedExecutionException])
              .isPresent) {
          indexer.add(action)
        } else if (ExceptionUtils
                     .findThrowable(failure,
                                    classOf[ElasticsearchParseException])
                     .isPresent) {} else {
          println("FAILURE: ")
          println(failure)
          println(restStatusCode)
          //throw failure
        }
      }
    })
    source.addSink(eSinkBuilder.build())
  }

  def createConfig(): java.util.HashMap[String, String] = {
    val config = new java.util.HashMap[String, String]
    config
  }

  /**
    * Create elastic search host address list
    *
    * @return List
    */
  def createTransportAddresses(): java.util.ArrayList[HttpHost] = {
    val transportAddresses = new java.util.ArrayList[HttpHost]

    if (servers.isEmpty) {
      logger.info(
        "Transport address set is empty. Using localhost with default port 9300.")
      transportAddresses.add(new HttpHost("localhost", 9200, "http"))
    }

    for (server <- servers) {
      val uri = new URI(server)

      if (uri.getScheme == "es") {
        logger.info(s"Adding transport address $server")
        transportAddresses.add(new HttpHost(uri.getHost, uri.getPort, "http"))
      }
    }

    transportAddresses
  }

}

/**
  * An elastic search sink
  *
  * @param index Index to be used in ElasticSearch
  * @tparam T Type of input
  */
private class ElasticSearchSink[
    T <: Serializable with AnyRef: ClassTag: Manifest](index: String)
    extends ElasticsearchSinkFunction[T] {

  // ES records are not allowed to have _id fields, so we replace it with idd.
  val esSerializer = FieldSerializer[T](
    renameTo("_id", "idd"),
    renameFrom("idd", "_id")
  )

  implicit lazy val formats = Serialization.formats(NoTypeHints) ++ JavaTimeSerializers.all + esSerializer

  def createIndexRequest(element: T): IndexRequest = {
    val bytes = serialize(element)

    Requests
      .indexRequest()
      .index(index)
      .`type`("json")
      .source(bytes, XContentType.JSON)
  }

  override def process(element: T,
                       ctx: RuntimeContext,
                       indexer: RequestIndexer): Unit = {
    indexer.add(createIndexRequest(element))
  }

  /**
    * Serialize an element into JSON
    *
    * @param element Element
    * @return JSON bytes
    */
  def serialize(element: T): Array[Byte] = {
    val bytes = Serialization.write[T](element)(formats)

    bytes.getBytes(StandardCharsets.UTF_8)
  }

}
