package edu

case class Split(split: Seq[Any], remaining: Seq[Any])
case class SplitAcc(remaining: Seq[Any], result: Seq[Seq[Any]])

object MatchContainerRequest {

  def handleAllocatedContainers(allocatedContainers: Seq[Any])
                               (fs: Seq[Seq[Any] => Split]): Seq[Seq[Any]] = {
    val SplitAcc(remaining, result) = fs.foldLeft(SplitAcc(allocatedContainers, Seq[Seq[Any]]())) {
      case (SplitAcc(remaining, result), f) =>
        val tmp = f(remaining)
        SplitAcc(tmp.remaining, result :+ tmp.split)
    }
    result :+ remaining
  }
}