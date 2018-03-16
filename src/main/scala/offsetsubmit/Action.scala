package offsetsubmit

import org.apache.spark.streaming.kafka.KafkaControl

object Action {
  def main(args: Array[String]): Unit = {
    //1.brokerlist 2.groupId  3.topic 4.smallest or largest 5.option(zookeeper:0 or kafka:1)
    if(args(0).equals("--help")){
      println("1.brokerlist 2.groupId  3.topic(可以多个逗号隔开) 4.smallest or largest 5.option(zookeeper:0 or kafka:1)")
    }else {
      val brokerlist = args(0)
      val topic = args(2).split(",").toSet
      val pattern = args(3)
      val option = args(4).toShort
      val groupId = args(1)
      new KafkaControl().changeOffset(brokerlist, topic, groupId, pattern, option)
    }
  }
}
