package com.github.hexx.gaeds

import scala.collection.JavaConverters._
import com.google.appengine.api.datastore.{ Cursor, FetchOptions, Index, Transaction }
import com.google.appengine.api.datastore.{ Query => LLQuery }
import com.google.appengine.api.datastore.Query.{ FilterOperator, SortDirection }

class Query[T <: Mapper[T]: ClassManifest, U <: Mapper[U]](
    txn: Option[Transaction],
    mapper: T,
    ancestorKey: Option[Key[U]],
    fetchOptions: FetchOptions = FetchOptions.Builder.withDefaults,
    _reverse: Boolean = false,
    _filter: Option[Filter] = None,
    sortPredicate: List[SortPredicate] = List()) {
  def filter(f: T => Filter) = {
    val newFilter = Some(_filter map (_ and f(mapper)) getOrElse f(mapper))
    new Query(txn, mapper, ancestorKey, fetchOptions, _reverse, newFilter, sortPredicate)
  }

  def addSort(f: T => SortPredicate) =
    new Query(txn, mapper, ancestorKey, fetchOptions, _reverse, _filter, sortPredicate :+ f(mapper))
  def sort(f: T => SortPredicate) = addSort(f)

  def asEntityIterator(keysOnly: Boolean) = prepare(keysOnly).asIterator(fetchOptions).asScala
  def asQueryResultIterator(keysOnly: Boolean) = prepare(keysOnly).asQueryResultIterator(fetchOptions)

  def asIterator(): Iterator[T] = asEntityIterator(false).map(mapper.fromEntity(_))
  def asKeyIterator(): Iterator[Key[T]] = asEntityIterator(true).map(e => Key(e.getKey))

  def asIteratorWithCursorAndIndex(): Iterator[(T, () => Cursor, () => Seq[Index])] = {
    val iterator = asQueryResultIterator(false)
    iterator.asScala.map(entity => (mapper.fromEntity(entity), iterator.getCursor _, () => iterator.getIndexList.asScala.toSeq))
  }
  def asKeyIteratorWithCursorAndIndex(): Iterator[(Key[T], () => Cursor, () => Seq[Index])] = {
    val iterator = asQueryResultIterator(true)
    iterator.asScala.map(entity => (Key(entity.getKey), iterator.getCursor _, () => iterator.getIndexList.asScala.toSeq))
  }

  def asSingleEntity(keysOnly: Boolean) = prepare(keysOnly).asSingleEntity
  def asSingle(): T = mapper.fromEntity(asSingleEntity(false))
  def asSingleKey(): Key[T] = Key(asSingleEntity(true).getKey)

  def count() = prepare(false).countEntities(fetchOptions)

  def prepare(keysOnly: Boolean) = txn match {
    case Some(t) => Datastore.service.prepare(t, toQuery(keysOnly))
    case None => Datastore.service.prepare(toQuery(keysOnly))
  }

  def reverse() = new Query(txn, mapper, ancestorKey, fetchOptions, !_reverse, _filter, sortPredicate)

  def toQuery(keysOnly: Boolean) = {
    val query = ancestorKey match {
      case Some(k) => new LLQuery(mapper.kind, k.key)
      case None => new LLQuery(mapper.kind)
    }
    _filter foreach (query setFilter _.toFilter)
    for (p <- sortPredicate) {
      query.addSort(p.property.__nameOfProperty, p.direction)
    }
    if (_reverse) {
      query.reverse()
    }
    if (keysOnly) {
      query.setKeysOnly()
    }
    query
  }

  // wrapping FetchOptions
  def startCursor(cursor: Cursor) =
    new Query(txn, mapper, ancestorKey, fetchOptions.startCursor(cursor), _reverse, _filter, sortPredicate)
  def endCursor(cursor: Cursor) =
    new Query(txn, mapper, ancestorKey, fetchOptions.endCursor(cursor), _reverse, _filter, sortPredicate)
  def chunkSize(size: Int) =
    new Query(txn, mapper, ancestorKey, fetchOptions.chunkSize(size), _reverse, _filter, sortPredicate)
  def limit(limit: Int) =
    new Query(txn, mapper, ancestorKey, fetchOptions.limit(limit), _reverse, _filter, sortPredicate)
  def offset(offset: Int) =
    new Query(txn, mapper, ancestorKey, fetchOptions.offset(offset), _reverse, _filter, sortPredicate)
  def prefetchSize(size: Int) =
    new Query(txn, mapper, ancestorKey, fetchOptions.prefetchSize(size), _reverse, _filter, sortPredicate)
}
