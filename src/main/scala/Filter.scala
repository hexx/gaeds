package com.github.hexx.gaeds

import scala.collection.JavaConverters._
import com.google.appengine.api.datastore.Query.{ FilterOperator, CompositeFilterOperator }
import com.google.appengine.api.datastore.Query.{ Filter => LLFilter, FilterPredicate => LLFilterPredicate }

sealed trait Filter {
  def and(f: Filter) = FilterAnd(this, f)
  def or(f: Filter) = FilterOr(this, f)
  def toFilter: LLFilter
}

case class FilterAnd(f1: Filter, f2: Filter) extends Filter {
  def toFilter = CompositeFilterOperator.and(f1.toFilter, f2.toFilter)
}

case class FilterOr(f1: Filter, f2: Filter) extends Filter {
  def toFilter = CompositeFilterOperator.or(f1.toFilter, f2.toFilter)
}

case class FilterPredicate[T](property: BaseProperty[T], operator: FilterOperator, value: T*) extends Filter {
  def toFilter =
    if (operator == FilterOperator.IN) {
      new LLFilterPredicate(property.__nameOfProperty, operator, value.asJava)
    } else {
      new LLFilterPredicate(property.__nameOfProperty, operator, value(0))
    }
}
