package edu

import org.scalatest.{FlatSpec, Matchers}

class MatchContainerRequestSpec extends FlatSpec with Matchers {
  "handleAllocatedContainers" should "split input set to 4 sets" in {
    case object Hello
    case object LeaveMeAlone

    val input = Seq(Hello, 1, 2, LeaveMeAlone, 3, false, true, Hello, Hello)
    val bucket1 = Seq(3)
    val f1 = { inp: Seq[Any] => Split(bucket1, inp.diff(bucket1)) }
    val bucket2 = Seq(true, false)
    val f2 = { inp: Seq[Any] => Split(bucket2, inp.diff(bucket2)) }
    val bucket3 = Seq(Hello, Hello, LeaveMeAlone)
    val f3 = { inp: Seq[Any] => Split(bucket3, inp.diff(bucket3)) }
    val remainingAfterOffRackMatches = Seq(1, 2, Hello)

    val actual = MatchContainerRequest.handleAllocatedContainers(input)(Seq(f1, f2, f3))
    val expected = Seq(bucket1, bucket2, bucket3, remainingAfterOffRackMatches)

    actual shouldBe expected
  }
}