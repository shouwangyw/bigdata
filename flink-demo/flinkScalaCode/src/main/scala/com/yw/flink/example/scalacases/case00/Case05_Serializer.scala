package com.yw.flink.example.scalacases.case00

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * Scala 序列化
  */
object Case05_Serializer {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //自定义注册kryo序列化
    env.getConfig.registerTypeWithKryoSerializer(classOf[Student], classOf[StudentSerializer])

    //导入隐式转换
    import org.apache.flink.streaming.api.scala._

    val ds: DataStream[String] = env.socketTextStream("nc_server", 9999)

    ds.map(one => {
      val arr: Array[String] = one.split(",")
      new Student(arr(0).toInt, arr(1), arr(2).toInt)
    }).print()

    env.execute()
  }

}
class Student {
  private var id: Int = _
  private var name: String = _
  private var age: Int = _

  def this(id: Int, name: String, age: Int) {
    this()
    this.id = id;
    this.name = name;
    this.age = age;
  }

  def getId: Int = id
  def setId(id: Int): Unit = {
    this.id = id
  }

  def getName: String = name
  def setName(name: String): Unit = {
    this.name = name
  }

  def getAge: Int = age
  def setAge(age: Int): Unit = {
    this.age = age
  }

  override def toString = s"Student($id, $name, $age)"
}

class StudentSerializer extends Serializer {
  override def write(kryo: Kryo, output: Output, t: Nothing): Unit = {
    val student = t.asInstanceOf[Student]

    output.writeInt(student.getId)
    output.writeString(student.getName)
    output.writeInt(student.getAge)
  }

  override def read(kryo: Kryo, input: Input, aClass: Class[Nothing]): Nothing = {
    new Student(input.readInt(), input.readString(), input.readInt()).asInstanceOf
  }
}
