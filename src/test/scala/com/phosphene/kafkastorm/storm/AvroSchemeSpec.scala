package com.phosphene.kafkastorm.storm

import com.phosphene.avro.Stashy
import com.twitter.bijection.Injection
import com.twitter.bijection.avro.SpecificAvroCodecs
import org.scalatest.{FunSpec, GivenWhenThen, Matchers}
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.language.reflectiveCalls

class AvroSchemeSpec extends FunSpec with Matchers with GivenWhenThen {

  implicit val specificAvroBinaryInjectionForStashy = SpecificAvroCodecs.toBinary[Stashy]

  val fixture = {
    val BeginningOfEpoch = 0.seconds
    val AnyTimestamp = 1234.seconds
    val now = System.currentTimeMillis().millis

    new {
      val t1 = new Stashy("ANY_message_1", "ANY_version_1",now.toSeconds, "ANYstring", "ANYstring")
      val t2 = new Stashy("ANY_message_2", "ANY_version_2",BeginningOfEpoch.toSeconds, "ANYstring", "ANYstring")
      val t3 = new Stashy("ANY_message_3", "ANY_version_3",AnyTimestamp.toSeconds, "ANYstring", "ANYstring")
      val messages = Seq(t1, t2, t3)
    }
  }

  describe("An AvroScheme") {

    it("should have a single output field named 'pojo'") {
      Given("a scheme")
      val scheme = new AvroScheme

      When("I get its output fields")
      val outputFields = scheme.getOutputFields()

      Then("there should only be a single field")
      outputFields.size() should be(1)

      And("this field should be named 'pojo'")
      outputFields.contains("pojo") should be(true)
    }


    it("should deserialize binary records of the configured type into pojos") {
      Given("a scheme for type Stashy ")
      val scheme = new AvroScheme[Stashy]
      And("some binary-encoded Stashy records")
      val f = fixture
      val encodedStashys = f.messages.map(Injection[Stashy, Array[Byte]])

      When("I deserialize the records into pojos")
      val actualStashys = for {
        l <- encodedStashys.map(scheme.deserialize)
        tweet <- l.asScala
      } yield tweet

      Then("the pojos should be equal to the original pojos")
      actualStashys should be(f.messages)
    }

    it("should throw a runtime exception when serialization fails") {
      Given("a scheme for type Stashy ")
      val scheme = new AvroScheme[Stashy]
      And("an invalid binary record")
      val invalidBytes = Array[Byte](1, 2, 3, 4)

      When("I deserialize the record into a pojo")

      Then("the scheme should throw a runtime exception")
      val exception = intercept[RuntimeException] {
        scheme.deserialize(invalidBytes)
      }
      And("the exception should provide a meaningful explanation")
      exception.getMessage should be("Could not decode input bytes")
    }

  }

  describe("An AvroScheme companion object") {

    it("should create an AvroScheme for the correct type") {
      Given("a companion object")

      When("I ask it to create a scheme for type Stashy")
      val scheme = AvroScheme.ofType(classOf[Stashy])

      Then("the scheme should be an AvroScheme")
      scheme shouldBe an[AvroScheme[_]]
      And("the scheme should be parameterized with the type Stashy")
      scheme.tpe.shouldEqual(manifest[Stashy])
    }

  }

}
