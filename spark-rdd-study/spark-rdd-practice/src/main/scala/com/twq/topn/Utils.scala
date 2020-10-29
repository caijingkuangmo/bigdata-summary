package com.twq.topn

import com.google.common.collect.{Ordering => GuavaOrdering}

import scala.collection.JavaConverters._

object Utils {
  /**
    * Returns the first K elements from the input as defined by the specified implicit Ordering[T]
    * and maintains the ordering.
    */
  def takeOrdered[T](input: Iterator[T], num: Int)(implicit ord: Ordering[T]): java.util.List[T] = {
    val ordering = new GuavaOrdering[T] {
      override def compare(l: T, r: T): Int = ord.compare(l, r)
    }
    ordering.leastOf(input.asJava, num)
  }
}
