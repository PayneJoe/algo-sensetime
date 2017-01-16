package com.sensetime.ad.algo.scalatest

/**
  * Created by yuanpingzhou on 1/13/17.
  */
object OrientedObject {
  case class Person(var name: String,var age: Int)

  object Person {
    def apply() = new Person("<no name>", 0)
    def apply(name: String) = new Person(name, 0)
  }

  class Brain private {
    override def toString = "this is brain"
    //def toWhat = "what"
  }

  object Brain{
    def getInstance = new Brain
  }

  def main(args: Array[String]) = {
    // customer-defined constructor for case class
    val p0 = Person()
    println(s"${p0.name} ${p0.age}")

    val ret = Brain.getInstance
    println(ret)
  }
}
