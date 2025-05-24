package com.yw.flink.example.scalacases.case09_state

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import scala.collection.mutable.ListBuffer

/**
  * Flink OperatorState 算子状态测试
  * 案例：读取socket基站日志数据，每隔3条打印到控制台
  * 001,186,187,busy,1000,10
  * 002,187,186,fail,2000,20
  * 003,186,188,busy,3000,30
  * 004,187,186,busy,4000,40
  * 005,186,187,busy,5000,50
  */
object OperatorStateTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //开启checkpoint
    env.enableCheckpointing(5000)
    env.setParallelism(1)

    //导入隐式转换
    import org.apache.flink.streaming.api.scala._
    //读取socket数据
    val ds: DataStream[String] = env.socketTextStream("node5", 9999)
    ds.map(new MyRichMapAndCheckpointFunction()).print()
    env.execute()
  }

}

class MyRichMapAndCheckpointFunction extends RichMapFunction[String, String] with CheckpointedFunction {
  //每个并行实例中的状态
  private var listState: ListState[String] = _

  //本地存储数据的集合
  private val stationLogList: ListBuffer[String] = new ListBuffer[String]()

  //来一条数据处理一条
  override def map(line: String): String = {
    if (stationLogList.size == 3) {
      var info: String = ""
      //拼接写出
      for (elem <- stationLogList) {
        info += elem + " | "
      }
      stationLogList.clear()
      info
    } else {
      stationLogList.append(line)
      "当前集合长度：" + stationLogList.size + ",不满足写出条件！"
    }

  }

  //该方法在checkpoint触发时调用，需要在该方法中准备状态，由checkpoint写出到外部系统
  override def snapshotState(functionSnapshotContext: FunctionSnapshotContext): Unit = {
    println("checkpoint将要把状态写出去，需要准备状态了")
    listState.clear()
    //将本地集合数据加入到状态中
    import scala.collection.JavaConverters._
    listState.addAll(stationLogList.asJava)
  }

  //该方法在自定义处理数据业务逻辑执行前执行，用作初始化状态+恢复状态后如何进行业务逻辑处理
  override def initializeState(context: FunctionInitializationContext): Unit = {
    println("initializeState 调用了，初始化状态和恢复状态")
    //定义状态描述器
    val listStateDescriptor = new ListStateDescriptor[String]("listState", classOf[String])
    //获取状态
    listState = context.getOperatorStateStore.getListState(listStateDescriptor)

    //恢复状态处理的业务逻辑
    if (context.isRestored) {
      //状态恢复后，如何处理业务逻辑
      import scala.collection.JavaConverters._
      for (elem <- listState.get().asScala) {
        stationLogList.append(elem)
      }
    }
  }
}
