package com.sensetime.ad.algo.scalatest

/**
  * chapter 2 : Polymorphism Function , from Functional Programming with Scala
  *
  * Created by yuanpingzhou on 1/6/17.
  */
object PolymorphismFunction{

  def findFirst[A](as: Array[A],p: A => Boolean): Int = {
    def loop(n: Int): Int = {
      if(n >= as.length) {
        -1
      }
      else if(p(as(n))){
        n
      }
      else{
        loop(n + 1)
      }
    }
    loop(0)
  }

  def isSorted[A](as: Array[A],p: (A,A) => Boolean): Boolean = {
    def loop(n: Int): Boolean = {
      if(n >= as.length - 1){
        true
      }
      else if(p(as(n),as(n + 1))) {
        loop(n + 1)
      }
      else{
        false
      }
    }
    loop(0)
  }

  def intFind(v: Int): Boolean = {
    v == 10
  }

  def intSort(a: Int,b: Int): Boolean = {
    a < b
  }

  def main(args: Array[String]) ={
    val arr = Array(20,30,40,50,70,80)
    //val ret = findFirst(arr,intFind)
    val ret = isSorted(arr,intSort)
    println(s"result : ${ret}")
  }
}
