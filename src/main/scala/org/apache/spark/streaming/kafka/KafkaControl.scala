package org.apache.spark.streaming.kafka

class KafkaControl {

  /**
    * 修改某个groupId消费topic的offset位置信息
    * @param brokerList kafka地址(若是集群模式,需要全部写全)
    * @param topics     kafka主题
    * @param groupId
    * @param pattern    smallest或者是largest
    * @param option     更新到zookeeper还是kafka(0是zk,1是kafka)
    */
  def changeOffset(brokerList:String,topics:Set[String],groupId:String,pattern:String,option:Short){
    val kafkaParam = Map[String,String](
      "metadata.broker.list" -> brokerList,
      "group.id" -> groupId,
      "auto.offset.reset" -> pattern)
    var kc = new KafkaCluster(kafkaParam)
    val originOffsetsE = kc.getPartitions(Set("__consumer_offsets"))
    var originPartitionNum = 0
    if(originOffsetsE.isLeft)
      println("获取__consumer_offsets分区失败！！！")
    else
      originPartitionNum = originOffsetsE.right.get.size
    topics.foreach(topic=>{
      val partitionsE = kc.getPartitions(Set(topic))
      if(partitionsE.isLeft)throw new Exception(s"get kafka partition faild: ${partitionsE.left.get}")
      val partitions = partitionsE.right.get
      if(originPartitionNum!=0)
        println(s"该主题所在的分区为:${Math.abs(groupId.hashCode()) % originPartitionNum}")
      val reset = kafkaParam.get("auto.offset.reset").map(_.toLowerCase)
      if(reset==Some("smallest")){
        val smallOffsetsE = kc.getEarliestLeaderOffsets(partitions)
        if(smallOffsetsE.isLeft)throw new Exception(s"get earliest leader offsets faild: ${smallOffsetsE.left.get}")
        val smallOffset = smallOffsetsE.right.get
        val offsets = smallOffset.map{
          case(tp,offset)=>(tp,offset.offset)
        }
        kc.setConsumerOffsets(groupId,offsets,option)
      }else{
        val largeOffsetsE = kc.getLatestLeaderOffsets(partitions)
        if(largeOffsetsE.isLeft) throw new Exception(s"get largest leader offsets faild: ${largeOffsetsE.left.get}")
        val largeOffset = largeOffsetsE.right.get
        val offsets = largeOffset.map{
          case(tp,offset)=>(tp,offset.offset)
        }
        kc.setConsumerOffsets(groupId,offsets,option)
      }

    })
  }
}
