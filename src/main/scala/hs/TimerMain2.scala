package hs
import org.apache.flink.streaming.api._
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import util.Random
import org.apache.flink.streaming.api.scala._
import collection.mutable._

import collection.mutable
import org.apache.flink.streaming.api.functions.source._


/**
 * 每隔1秒发送一个tuple2类型的数据，第一个字段值为随机的一个姓氏，第二个字段为自增的数字
 **/
class MySourceTuple2 extends SourceFunction[(String, Long)] {

  var isRunning: Boolean = true
  val names: List[String] = List("张", "王", "李", "赵")
  private val random = new Random()
  var number: Long = 1

  override def run(ctx: SourceFunction.SourceContext[(String, Long)]): Unit = {
    while (true) {
      val index: Int = random.nextInt(4)
      ctx.collect((names(index), number))
      number += 1
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }
}
object TimerMain2 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.addSource(new MySourceTuple2)
    .keyBy(_._1)
    .process(new KeyedProcessFunction[String, (String, Long), String] {

          //缓存流数据
          private val cache: mutable.Map[String, ListBuffer[Long]] = mutable.Map[String, ListBuffer[Long]]()
          private var first: Boolean = true

          /**
           * 定时器触发时回调该函数
           *
           * @param timestamp 定时器触发时间
           */
          override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, (String, Long), String]#OnTimerContext, out: Collector[String]): Unit = {
            println("定时器触发：" + timestamp)
            //将缓存中的数据组织成需要的格式
            val builder = new StringBuilder()
            println("cache: " + cache)
            for (entry: (String, ListBuffer[Long]) <- cache) {
              builder.append(entry._1).append(":")
              for (ele <- entry._2) {
                builder.append(ele).append(",")
                //  张；赵:102,103,104；王:101；李:105；
              }
              builder.delete(builder.size - 1, builder.size).append("；")
              cache(entry._1).clear()
            }
            println("定时器注册：" + timestamp)
            //该定时器执行完任务之后，重新注册一个定时器
            ctx.timerService().registerProcessingTimeTimer(timestamp + 10000)
            out.collect(builder.toString())
          }

          /**
           * 处理每一个流数据
           */
          override def processElement(value: (String, Long), ctx: KeyedProcessFunction[String, (String, Long), String]#Context, out: Collector[String]): Unit = {
            println("processElement:"+value)
            //仅在该算子接收到第一个数据时，注册一个定时器
            if (first) {
              first = false
              val time: Long = System.currentTimeMillis()
              println("定时器第一次注册：" + time)
              ctx.timerService().registerProcessingTimeTimer(time + 10000)
            }
            //将流数据更新到缓存中
            if (cache.contains(value._1)) {
              cache(value._1).append(value._2)
            } else {
              cache.put(value._1, ListBuffer[Long](value._2))
            }
          }
        }
        )
        .print("处理结果：")
    env.execute()
  }

}

// output
// 处理结果：> 赵:16,19,20；张；王:17；李:18；
// processElement:(赵,21)
// processElement:(张,22)
// processElement:(赵,23)
// processElement:(张,24)
// processElement:(赵,25)
// 定时器触发：1602318706403
// cache: Map(赵 -> ListBuffer(21, 23, 25), 张 -> ListBuffer(22, 24), 王 -> ListBuffer(), 李 -> ListBuffer())
// 定时器注册：1602318706403
// 处理结果：> 赵:21,23,25；张:22,24；王；李；
