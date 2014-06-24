package com.phosphene.kafkastorm.storm

import com.phosphene.avro.Stashy
import backtype.storm.topology.base.BaseBasicBolt
import backtype.storm.topology.{BasicOutputCollector, OutputFieldsDeclarer}
import backtype.storm.tuple.{Fields, Tuple, Values}
import com.google.common.base.Throwables
import com.twitter.bijection.avro.SpecificAvroCodecs
import com.twitter.bijection.Injection
import org.apache.avro.specific.SpecificRecordBase
import scala.util.{Try, Failure, Success}
import org.slf4j.{Logger, LoggerFactory}


class StashyTypeFilterBolt extends BaseBasicBolt {

  // is transient => Logger not serializable
  @transient lazy private val log: Logger = LoggerFactory.getLogger(classOf[StashyTypeFilterBolt])
  // Must be transient because Injection is not serializable.  Must be implicit because that's how Injection works.
  @transient lazy implicit private val specificAvroBinaryInjection: Injection[Stashy, Array[Byte]] =
    SpecificAvroCodecs.toBinary[Stashy]


  override def execute(input: Tuple, collector: BasicOutputCollector) {

    val readTry = Try(input.getValueByField("pojo"))
    readTry match {
      case Success(pojo) if pojo != null => filterOnStashyType(pojo, collector)
      case Success(_) => log.error("Reading from input tuple returned null")
      case Failure(e) => log.error("Could not read from input tuple: " + Throwables.getStackTraceAsString(e))
    }

  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer) {
    declarer.declareStream("StreamOne", new Fields("pojo"))
    declarer.declareStream("StreamTwo", new Fields("pojo"))
    declarer.declareStream("StreamThree", new Fields("pojo"))
  }


  private def filterOnStashyType(pojo: Object, collector: BasicOutputCollector) {

    val stashy:Stashy = pojo match {
      case x:Stashy => x
      case _ => throw new ClassCastException
    }


    collector.emit(new Values(stashy))

  }

}
