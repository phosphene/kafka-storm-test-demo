package com.phosphene.kafkastorm.storm

import backtype.storm.topology.{BasicOutputCollector, OutputFieldsDeclarer}
import backtype.storm.tuple.{Fields, Tuple, Values}
import com.phosphene.avro.Stashy
import com.twitter.bijection.avro.SpecificAvroCodecs
import com.twitter.bijection.Injection
import org.apache.avro.specific.SpecificRecordBase

import org.mockito.Matchers._
import org.mockito.Mockito.{when => mwhen, _}
import org.scalatest.{FunSpec, GivenWhenThen, Matchers}
import org.scalatest.mock.MockitoSugar
import scala.concurrent.duration._

class StashyTypeFilterBoltSpec extends FunSpec with Matchers with GivenWhenThen with MockitoSugar {

  private val AnyStashy = new Stashy("ANY_message_1", "ANY_version_1",1234.seconds.toSeconds, "ANYstring", "ANYstring")

  describe("A StashyTypeFilterBolt") {

    it("should read incoming tuples") {
      Given("no bolt")

      When("I create a StashyTypeFilterBolt bolt ")
      val bolt = new StashyTypeFilterBolt
      And("the bolt receives a tuple")
      val tuple = mock[Tuple]
      mwhen(tuple.getValueByField(anyString)).thenReturn(AnyStashy, Nil: _*)
      val collector = mock[BasicOutputCollector]
      bolt.execute(tuple, collector)

      Then("the bolt should read the field 'pojo' from the tuple")
      verify(tuple, times(1)).getValueByField("pojo")
    }

    it("should receive pojos and send new pojos to downstream bolts") {
      Given("a bolt of type StashyTypeFilterBolt")
      val bolt = new StashyTypeFilterBolt
      And("a Stashy pojo")
      val tuple = mock[Tuple]

      mwhen(tuple.getValueByField(anyString)).thenReturn(AnyStashy, Nil: _*)

      When("the bolt receives the Stashy pojo")
      val collector = mock[BasicOutputCollector]
      bolt.execute(tuple, collector)

      Then("the bolt should send the a Stashy to downstream bolts")
      verify(collector, times(1)).emit(new Values(AnyStashy))
    }

    it("should skip over tuples for which reading fails") {
      Given("a bolt")
      val bolt = new StashyTypeFilterBolt

      And("a tuple from which one cannot read")
      val tuple = mock[Tuple]
      mwhen(tuple.getValueByField(anyString)).thenReturn(null, Nil: _*)

      When("the bolt receives the tuple")
      val collector = mock[BasicOutputCollector]
      bolt.execute(tuple, collector)

      Then("the bolt should not send any data to downstream bolts")
      verifyZeroInteractions(collector)
    }

  }

}
