package com.phosphene.kafkastorm.storm

import backtype.storm.serialization.IKryoDecorator
import com.esotericsoftware.kryo.Kryo
import com.phosphene.avro.Stashy
import com.twitter.chill.KryoSerializer
import com.twitter.chill.avro.AvroSerializer

class StashyAvroKryoDecorator extends IKryoDecorator {
  override def decorate(k: Kryo) {
    k.register(classOf[Stashy], AvroSerializer.SpecificRecordSerializer[Stashy])
    KryoSerializer.registerAll(k)
  }
}
