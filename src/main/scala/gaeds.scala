package com.github.hexx.gaeds

import java.lang.reflect.Method
import java.util.concurrent.{ Future, TimeUnit }
import scala.collection.JavaConverters._
import com.google.appengine.api.datastore._
import com.google.appengine.api.datastore.Query.FilterOperator
import com.google.appengine.api.datastore.Query.SortDirection

object Datastore {
  val service = DatastoreServiceFactory.getDatastoreService
  val asyncService = DatastoreServiceFactory.getAsyncDatastoreService

  def allocateIdRange(range: KeyRange) = service.allocateIdRange(range)
  def allocateIds(parent: Key, kind: String, num: Long) = service.allocateIds(parent, kind, num)
  def allocateIds(kind: String, num: Long) = service.allocateIds(kind, num)

  def beginTransaction() = service.beginTransaction()
  def getActiveTransactions() = service.getActiveTransactions()
  def getCurrentTransaction() = service.getCurrentTransaction()
  def getCurrentTransaction(returnedIfNoTxn: Transaction) = service.getCurrentTransaction(returnedIfNoTxn)

  case class FutureWrapper[T, U](underlying: Future[T], f: T => U) extends Future[U] {
    def	cancel(mayInterruptIfRunning: Boolean) = underlying.cancel(mayInterruptIfRunning)
    def get(): U = f(underlying.get())
    def get(timeout: Long, unit: TimeUnit): U = f(underlying.get(timeout, unit))
    def isCancelled() = underlying.isCancelled
    def isDone() = underlying.isDone
  }

  private def wrapGet[T <: Mapper[T]: ClassManifest](mapper: T, entity: Entity): T =
    mapper.fromEntity(entity)
  private def wrapGet[T <: Mapper[T]: ClassManifest](mapper: T, entities: java.util.Map[Key, Entity]): Iterable[T] =
    entities.asScala.values.map(mapper.fromEntity(_))

  def get[T <: Mapper[T]: ClassManifest](mapper: T, key: Key): T =
    wrapGet(mapper, service.get(key))
  def get[T <: Mapper[T]: ClassManifest](mapper: T, keys: Key*): Iterable[T] =
    wrapGet(mapper, service.get(keys.asJava))
  def get[T <: Mapper[T]: ClassManifest](txn: Transaction, mapper: T, key: Key): T =
    wrapGet(mapper, service.get(txn, key))
  def get[T <: Mapper[T]: ClassManifest](txn: Transaction, mapper: T, keys: Key*): Iterable[T] =
    wrapGet(mapper, service.get(txn, keys.asJava))

  def getAsync[T <: Mapper[T]: ClassManifest](mapper: T, key: Key): Future[T] =
    FutureWrapper(asyncService.get(key), wrapGet(mapper, _: Entity)(implicitly[ClassManifest[T]]))
  def getAsync[T <: Mapper[T]: ClassManifest](mapper: T, keys: Key*): Future[Iterable[T]] =
    FutureWrapper(asyncService.get(keys.asJava), wrapGet(mapper, _: java.util.Map[Key, Entity])(implicitly[ClassManifest[T]]))
  def getAsync[T <: Mapper[T]: ClassManifest](txn: Transaction, mapper: T, key: Key): Future[T] =
    FutureWrapper(asyncService.get(txn, key), wrapGet(mapper, _: Entity)(implicitly[ClassManifest[T]]))
  def getAsync[T <: Mapper[T]: ClassManifest](txn: Transaction, mapper: T, keys: Key*): Future[Iterable[T]] =
    FutureWrapper(asyncService.get(txn, keys.asJava), wrapGet(mapper, _: java.util.Map[Key, Entity])(implicitly[ClassManifest[T]]))

  private def wrapPut[T <: Mapper[T]](mapper: T)(key: Key) = {
    mapper.key = Option(key)
    key
  }
  private def wrapPut[T <: Mapper[T]](mappers: T*)(keys: java.util.List[Key]) = {
    for ((mapper, key) <- mappers zip keys.asScala) {
      mapper.key = Option(key)
    }
    keys.asScala
  }

  def put[T <: Mapper[T]](mapper: T): Key =
    wrapPut(mapper)(service.put(mapper.toEntity))
  def put[T <: Mapper[T]](mappers: T*): Seq[Key] =
    wrapPut(mappers:_*)(service.put(mappers.map(_.toEntity).asJava))
  def put[T <: Mapper[T]](txn: Transaction, mapper: T): Key =
    wrapPut(mapper)(service.put(txn, mapper.toEntity))
  def put[T <: Mapper[T]](txn: Transaction, mappers: T*): Seq[Key] =
    wrapPut(mappers:_*)(service.put(txn, mappers.map(_.toEntity).asJava))

  def putAsync[T <: Mapper[T]](mapper: T): Future[Key] =
    FutureWrapper(asyncService.put(mapper.toEntity), wrapPut(mapper) _)
  def putAsync[T <: Mapper[T]](mappers: T*): Future[Seq[Key]] =
    FutureWrapper(asyncService.put(mappers.map(_.toEntity).asJava), wrapPut(mappers:_*) _)
  def putAsync[T <: Mapper[T]](txn: Transaction, mapper: T): Future[Key] =
    FutureWrapper(asyncService.put(txn, mapper.toEntity), wrapPut(mapper) _)
  def putAsync[T <: Mapper[T]](txn: Transaction, mappers: T*): Future[Seq[Key]] =
    FutureWrapper(asyncService.put(txn, mappers.map(_.toEntity).asJava), wrapPut(mappers:_*) _)

  def delete(keys: Key*) = service.delete(keys:_*)
  def delete(txn: Transaction, keys: Key*) = service.delete(txn, keys:_*)

  def deleteAsync(keys: Key*) = asyncService.delete(keys:_*)
  def deleteAsync(txn: Transaction, keys: Key*) = asyncService.delete(txn, keys:_*)

  def query[T <: Mapper[T]: ClassManifest](mapper: T) =
    new TypeSafeQuery(None, mapper, None)
  def query[T <: Mapper[T]: ClassManifest](mapper: T, ancestorKey: Key) =
    new TypeSafeQuery(None, mapper, Option(ancestorKey))
  def query[T <: Mapper[T]: ClassManifest](mapper: T, fetchOptions: FetchOptions) =
    new TypeSafeQuery(None, mapper, None, fetchOptions)
  def query[T <: Mapper[T]: ClassManifest](mapper: T, ancestorKey: Key, fetchOptions: FetchOptions) =
    new TypeSafeQuery(None, mapper, Some(ancestorKey), fetchOptions)
  def query[T <: Mapper[T]: ClassManifest](txn: Transaction, mapper: T) =
    new TypeSafeQuery(Option(txn), mapper, None)
  def query[T <: Mapper[T]: ClassManifest](txn: Transaction, mapper: T, ancestorKey: Key) =
    new TypeSafeQuery(Option(txn), mapper, Option(ancestorKey))
  def query[T <: Mapper[T]: ClassManifest](txn: Transaction, mapper: T, fetchOptions: FetchOptions) =
    new TypeSafeQuery(Option(txn), mapper, None, fetchOptions)
  def query[T <: Mapper[T]: ClassManifest](txn: Transaction, mapper: T, ancestorKey: Key, fetchOptions: FetchOptions) =
    new TypeSafeQuery(Some(txn), mapper, Some(ancestorKey), fetchOptions)

  def transaction[T](block: => T): T = {
    val t = service.beginTransaction
    try {
      val res = block
      t.commit()
      res
    } finally {
      if (t.isActive) {
        t.rollback()
      }
    }
  }
}

abstract class Mapper[T <: Mapper[T]: ClassManifest] {
  self: T =>

  assignPropertyName()

  var key: Option[Key] = None
  var parentKey: Option[Key] = None

  def kind = concreteClass.getName // override to customize

  def concreteClass = implicitly[ClassManifest[T]].erasure

  def put() = Datastore.put(this)
  def put(txn: Transaction) = Datastore.put(txn, this)

  def putAsync() = Datastore.putAsync(this)
  def putAsync(txn: Transaction) = Datastore.putAsync(txn, this)

  def get(key: Key) = Datastore.get(this, key)
  def get(key: Key*) = Datastore.get(this, key:_*)
  def get(txn: Transaction, key: Key) = Datastore.get(txn, this, key)
  def get(txn: Transaction, key: Key*) = Datastore.get(txn, this, key:_*)

  def getAsync(key: Key): Future[T] = Datastore.getAsync(this, key)
  def getAsync(key: Key*): Future[Iterable[T]] = Datastore.getAsync(this, key:_*)
  def getAsync(txn: Transaction, key: Key): Future[T] = Datastore.getAsync(txn, this, key)
  def getAsync(txn: Transaction, key: Key*): Future[Iterable[T]] = Datastore.getAsync(txn, this, key:_*)

  def query() = Datastore.query(this)
  def query(ancestorKey: Key) = Datastore.query(this, ancestorKey)
  def query(fetchOptions: FetchOptions) = Datastore.query(this, fetchOptions)
  def query(ancestorKey: Key, fetchOptions: FetchOptions) =
    Datastore.query(this, ancestorKey, fetchOptions)
  def query(txn: Transaction) = Datastore.query(txn, this)
  def query(txn: Transaction, ancestorKey: Key) = Datastore.query(txn, this, ancestorKey)
  def query(txn: Transaction, fetchOptions: FetchOptions) = Datastore.query(txn, this, fetchOptions)
  def query(txn: Transaction, ancestorKey: Key, fetchOptions: FetchOptions) =
    Datastore.query(txn, this, ancestorKey, fetchOptions)

  def fromEntity(entity: Entity): T = {
    val mapper = concreteClass.newInstance.asInstanceOf[T]
    for {
      (name, value) <- entity.getProperties.asScala
      field = concreteClass.getDeclaredField(name)
      p = findProperty(name).get
    } {
      field.setAccessible(true)
      val v = value match {
        case l: java.util.ArrayList[_] => l.asScala
        case s: java.util.HashSet[_] => s.asScala
        case null if p.__isSeq => Seq()
        case null if p.__isSet => Set()
        case _ => if (p.__isOption) Option(value) else value
      }
      val p2 = if (p.__isUnindexed) UnindexedProperty(v) else Property(v)
      field.set(mapper, p2)
    }
    mapper.key = Option(entity.getKey)
    mapper.parentKey = Option(entity.getParent)
    mapper.assignPropertyName()
    mapper
  }

  def properties: Seq[BaseProperty[_]] = zipPropertyAndMethod.map(_._1)

  def findProperty(name: String) = properties.find(_.__nameOfProperty == name)

  def toEntity = {
    val entity = (key, parentKey) match {
      case (Some(k), _      )  => new Entity(k)
      case (None,    Some(pk)) => new Entity(kind, pk)
      case (None,    None   )  => new Entity(kind)
    }
    assert(properties.size != 0, "define fields with Property[T]")
    properties foreach (_.__setToEntity(entity))
    entity
  }

  override def equals(that: Any) = that match {
    case that: Mapper[_] => that.key == key && that.properties == properties
    case _ => false
  }

  private def zipPropertyAndMethod: Seq[(BaseProperty[_], Method)] = {
    def isGetter(m: Method) = !m.isSynthetic && classOf[BaseProperty[_]].isAssignableFrom(m.getReturnType)
    for {
      m <- this.getClass.getMethods
      if isGetter(m)
      p = m.invoke(this).asInstanceOf[BaseProperty[_]]
    } yield (p, m)
  }

  private def assignPropertyName() {
    for ((p, m) <- zipPropertyAndMethod) {
      p.__nameOfProperty = m.getName
    }
  }
}

class TypeSafeQuery[T <: Mapper[T]: ClassManifest](
    txn: Option[Transaction],
    mapper: T,
    ancestorKey: Option[Key],
    fetchOptions: FetchOptions = FetchOptions.Builder.withDefaults,
    _reverse: Boolean = false,
    filterPredicate: List[FilterPredicate[_]] = List(),
    sortPredicate: List[SortPredicate] = List()) {
  def addFilter(f: T => FilterPredicate[_]) =
    new TypeSafeQuery(txn, mapper, ancestorKey, fetchOptions, _reverse, filterPredicate :+ f(mapper), sortPredicate)
  def filter(f: T => FilterPredicate[_]) = addFilter(f)

  def addSort(f: T => SortPredicate) =
    new TypeSafeQuery(txn, mapper, ancestorKey, fetchOptions, _reverse, filterPredicate, sortPredicate :+ f(mapper))
  def sort(f: T => SortPredicate) = addSort(f)

  def asEntityIterator(keysOnly: Boolean) = prepare(keysOnly).asIterator(fetchOptions).asScala
  def asQueryResultIterator(keysOnly: Boolean) = prepare(keysOnly).asQueryResultIterator(fetchOptions)

  def asIterator(): Iterator[T] = asEntityIterator(false).map(mapper.fromEntity(_))
  def asKeyIterator(): Iterator[Key] = asEntityIterator(true).map(_.getKey)

  def asIteratorWithCursorAndIndex(): Iterator[(T, () => Cursor, () => Seq[Index])] = {
    val iterator = asQueryResultIterator(false)
    iterator.asScala.map(entity => (mapper.fromEntity(entity), iterator.getCursor _, () => iterator.getIndexList.asScala.toSeq))
  }
  def asKeyIteratorWithCursorAndIndex(): Iterator[(Key, () => Cursor, () => Seq[Index])] = {
    val iterator = asQueryResultIterator(true)
    iterator.asScala.map(entity => (entity.getKey, iterator.getCursor _, () => iterator.getIndexList.asScala.toSeq))
  }

  def asSingleEntity(keysOnly: Boolean) = prepare(keysOnly).asSingleEntity
  def asSingle(): T = mapper.fromEntity(asSingleEntity(false))
  def asSingleKey(): Key = asSingleEntity(true).getKey

  def count() = prepare(false).countEntities(fetchOptions)

  def prepare(keysOnly: Boolean) = txn match {
    case Some(t) => Datastore.service.prepare(t, toQuery(keysOnly))
    case None => Datastore.service.prepare(toQuery(keysOnly))
  }

  def reverse() = new TypeSafeQuery(txn, mapper, ancestorKey, fetchOptions, !_reverse, filterPredicate, sortPredicate)

  def toQuery(keysOnly: Boolean) = {
    val query = ancestorKey match {
      case Some(k) => new Query(mapper.kind, k)
      case None => new Query(mapper.kind)
    }
    for (p <- filterPredicate) {
      if (p.operator == FilterOperator.IN) {
        query.addFilter(p.property.__nameOfProperty, p.operator, p.value.asJava)
      } else {
        query.addFilter(p.property.__nameOfProperty, p.operator, p.value(0))
      }
    }
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
}
