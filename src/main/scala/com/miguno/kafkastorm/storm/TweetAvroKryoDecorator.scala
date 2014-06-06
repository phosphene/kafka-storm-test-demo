package com.phosphene.kafkastorm.storm

import backtype.storm.serialization.IKryoDecorator
import com.esotericsoftware.kryo.Kryo
import com.phosphene.avro.Tweet
import com.twitter.chill.KryoSerializer
import com.twitter.chill.avro.AvroSerializer

class TweetAvroKryoDecorator extends IKryoDecorator {
  override def decorate(k: Kryo) {
    k.register(classOf[Tweet], AvroSerializer.SpecificRecordSerializer[Tweet])
    KryoSerializer.registerAll(k)
  }
}