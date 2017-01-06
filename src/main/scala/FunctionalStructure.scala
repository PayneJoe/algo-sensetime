package com.sensetime.ad.algo.scalatest

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
    // `List` companion object. Contains functions for creating and working with lists.

    def sum(ints: List[Int]): Int = ints match {
      // A function that uses pattern matching to add up a list of integers
      case Nil => 0 // The sum of the empty list is 0.
      case Cons(x, xs) => x + sum(xs) // The sum of a list starting with `x` is `x` plus the sum of the rest of the list.
    }

    def apply[A](as: A*): List[A] = // Variadic function syntax
      if (as.isEmpty) Nil
      else Cons(as.head, apply(as.tail: _*))

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
        case Nil => z
        case Cons(h,t) => f(h,foldRight(t,z,f))
      }
    }

    def join[A](as: List[List[A]],z: List[A] = Nil)(f: (List[A],List[A]) => List[A]): List[A] = {
      as match {
        case Cons(h,t) => join(t,f(z,h))(f)
        //case Cons(h,Nil) => f(z,h) //no need here
        case Nil => z
      }
    }

    def append[A](a: List[A] ,b: List[A]): List[A] = {
      a match {
        case Cons(h,t) => Cons(h,append(t,b))
        //case Cons(h,Nil) => Cons(h,b) // no need here
        case Nil => b
      }
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
      case Cons(h, t) => h + List.sum(t)
      case Cons(x, Cons(y, Cons(3, Cons(4, _)))) => x + y
    }
    println(s"${ret0}")

    //val ret = List.foldLeft(arr,0,List.intAdd)
    val ret = List.foldLeft(arr,1,List.intProduct)
    println(s"${ret}")

    val nestedList = List(List(1,2,3),List(4,5,6),List(7,8,9))
    println(List.join(nestedList)(List.append))

  }
}
