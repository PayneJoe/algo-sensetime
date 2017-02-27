package com.sensetime.ad.dm.scalatest

/**
  * chapter 3 : Functional Structure , from Functional Programming with Scala
  *
  * Created by yuanpingzhou on 1/6/17.
  */

object FunctionalStructure {

  sealed trait List[+A]

  // `List` data type, parameterized on a type, `A`
  case object Nil extends List[Nothing]

  // A `List` data constructor representing the empty list
  /* Another data constructor, representing nonempty lists. Note that `tail` is another `List[A]`,
which may be `Nil` or another `Cons`.
 */
  case class Cons[+A](head: A, tail: List[A]) extends List[A]

  object List {
    def intSum(nums: List[Int]): Int = {
      nums match {
        case Cons(x, xs) => x + intSum(xs)
        case Nil => 0
      }
    }

    def doubleSum(nums: List[Double]): Double = {
      nums match {
        case Cons(x, xs) => x + doubleSum(xs)
        case Nil => 0.0
      }
    }

    // List constructor
    def apply[A](as: A*): List[A] = {
      if (as.isEmpty) {
        Nil
      }
      else {
        Cons(as.head, apply(as.tail: _*))
      }
    }

    def removeLast[A](as: List[A]): List[A] = {
      as match {
        case Cons(v,Nil) => Nil
        case Cons(h,t) => Cons(h,removeLast(t))
      }
    }

    def length[A](as: List[A],f: (Int,Int) => Int): Int ={
      as match {
        case Nil => 0
        case Cons(h,t) => f(1,length(t,f))
      }
    }

    def foldLeft[A,B](as: List[A],z: B,f: (B,A) => B): B = {
      as match {
        case Cons(h,Nil) => f(z,h)
        case Cons(h,t) => foldLeft(t,f(z,h),f)
      }
    }

    def revert[A](as: List[A],r: List[A] = Nil): List[A] = {
      as match {
        case Cons(h,Nil) => Cons(h,r)
        case Cons(h,t) => revert(t,Cons(h,r))
      }
    }

    def foldRight[A,B](as: List[A],z: B,f: (A,B) => B): B ={
      as match {
        case Cons(h,t) => f(h,foldRight(t,z,f))
        case Nil => z
      }
    }

    def join[A](as: List[List[A]],z: List[A] = Nil)(f: (List[A],List[A]) => List[A]): List[A] = {
      as match {
        case Cons(h,t) => join(t,f(z,h))(f)
        case Nil => z
      }
    }

    def append[A](a: List[A] ,b: List[A]): List[A] = {
      a match {
        case Cons(h,t) => Cons(h,append(t,b))
        case Nil => b
      }
    }

    def valueConvert[A](as: List[A],f: A => A): List[A] = {
      as match {
        case Cons(h,t) => Cons(f(h),valueConvert(t,f))
        case Nil => Nil
      }
    }

    def typeConvert[A](as: List[A],f: A => String): List[String] = {
      as match {
        case Cons(h,t) => Cons(f(h),typeConvert(t,f))
        case Nil => Nil
      }
    }

    def filter[A](as: List[A],f: A => Boolean): List[A] = {
      as match {
        case Cons(h,t) => {
          if (f(h) == true){
            filter(t,f)
          }
          else{
            Cons(h,filter(t,f))
          }
        }
        case Nil => Nil
      }
    }

    def flatMap[A,B](as: List[A],f: A => List[B]): List[B] = {
      as match {
        case Cons(h,t) => append(f(h),flatMap(t,f))
        case Nil => Nil
      }
    }

    def hasSubsequence[A](sup: List[A],sub: List[A],f: (List[A],List[A]) => Boolean): Boolean ={
      sup match {
        case Nil => false
        case Cons(h,t) => {
          if(f(sup,sub) == true){
            true
          }
          else {
            hasSubsequence(t, sub, f)
          }
        }
      }
    }

    def isPrefix[A](sup: List[A],sub: List[A]): Boolean = {
      (sup,sub) match {
        case (_,Nil) => true
        case (Cons(hp,tp),Cons(hb,tb)) => {
          if(hp == hb){
            isPrefix(tp,tb)
          }
          else{
            false
          }
        }
        case _ => false
      }
    }

    def equal[A](a: A,b: A): Boolean = {
      a == b
    }

    def add[A](a: List[A],b: List[A],f: (A,A) => A): List[A] ={
      (a,b) match {
        case (Cons(ha,ta),Cons(hb,tb)) => Cons(f(ha,hb),add(ta,tb,f))
        case _ => Nil
      }
    }

    def tripled(v: Int): List[Double] = {
      Cons(v.toDouble,Cons(v.toDouble,Cons(v.toDouble,Nil)))
    }

    def isOdd(v: Int): Boolean = {
      ((v % 2) == 1)
    }

    def toString[A](v: A): String = {
      v.toString
    }

    def addOne[A](a: Int): Int = {
      a + 1
    }

    def intAdd(a: Int,b: Int): Int = {
      a + b
    }

    def intProduct(a: Int,b: Int): Int = {
      a * b
    }
  }

  def main(args: Array[String]) ={
    val arr = List(1, 2, 3, 4, 5)
    // match order
    val ret0 = arr match {
      case Cons(x, Cons(2, Cons(4, _))) => x
      case Nil => 42
      case _ => 101
      case Cons(h, t) => h + List.intSum(t)
      case Cons(x, Cons(y, Cons(3, Cons(4, _)))) => x + y
    }
    println(s"${ret0}")

    //val nestedList = List(List(1,2,3),List(4,5,6),List(7,8,9))
    //println(List.join(nestedList)(List.append))

    //val ret = List.typeConvert(arr,List.toString)
    //println(ret)

    //val ret = List.filter(arr,List.isOdd)
    //println(ret)

    //val ret = List.flatMap(arr,List.tripled)
    //println(ret)

    //val la = List(1,2,3)
    //val lb = List(4,5,6)
    //val ret = List.add(la,lb,List.intAdd)
    //println(ret)

    val lp = List(1,2,3,4,5)
    val lb = List(5,6)
    val ret = List.hasSubsequence(lp,lb,List.isPrefix)
    println(ret)
  }
}
