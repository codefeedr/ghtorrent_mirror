package org.codefeedr.experimental.stats

import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.windowing.ReduceApplyWindowFunction
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.codefeedr.experimental.stats.StatsObjects.Stats

class EmitHighestTimestamp
    extends ProcessWindowFunction[(Long, Stats), Stats, String, TimeWindow] {

  override def process(key: String,
                       context: Context,
                       elements: Iterable[(Long, Stats)],
                       out: Collector[Stats]): Unit = {
    val toEmit =
      elements.toList.reduceLeft((a, b) => if (a._1 > b._1) a else b)._2

    out.collect(toEmit)
  }
}
