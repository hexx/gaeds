package com.github.hexx.gaeds

import java.io.{ ByteArrayInputStream, ObjectInputStream }
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

  private def wrapGet[T <: Mapper[T]: ClassManifest](entity: Entity): T =
    createMapper(entity)
  private def wrapGet[T <: Mapper[T]: ClassManifest](entities: java.util.Map[Key, Entity]): Iterable[T] =
    entities.asScala.values.map(createMapper(_))

  def get[T <: Mapper[T]: ClassManifest](key: Key): T =
    wrapGet(service.get(key))
  def get[T <: Mapper[T]: ClassManifest](keys: Key*): Iterable[T] =
    wrapGet(service.get(keys.asJava))
  def get[T <: Mapper[T]: ClassManifest](txn: Transaction, key: Key): T =
    wrapGet(service.get(txn, key))
  def get[T <: Mapper[T]: ClassManifest](txn: Transaction, keys: Key*): Iterable[T] =
    wrapGet(service.get(txn, keys.asJava))

  def getAsync[T <: Mapper[T]: ClassManifest](key: Key): Future[T] =
    FutureWrapper(asyncService.get(key), wrapGet(_: Entity)(implicitly[ClassManifest[T]]))
  def getAsync[T <: Mapper[T]: ClassManifest](keys: Key*): Future[Iterable[T]] =
    FutureWrapper(asyncService.get(keys.asJava), wrapGet(_: java.util.Map[Key, Entity])(implicitly[ClassManifest[T]]))
  def getAsync[T <: Mapper[T]: ClassManifest](txn: Transaction, key: Key): Future[T] =
    FutureWrapper(asyncService.get(txn, key), wrapGet(_: Entity)(implicitly[ClassManifest[T]]))
  def getAsync[T <: Mapper[T]: ClassManifest](txn: Transaction, keys: Key*): Future[Iterable[T]] =
    FutureWrapper(asyncService.get(txn, keys.asJava), wrapGet(_: java.util.Map[Key, Entity])(implicitly[ClassManifest[T]]))

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

  def transaction[T](block: Transaction => T): T = {
    val t = service.beginTransaction
    try {
      val res = block(t)
      t.commit()
      res
    } finally {
      if (t.isActive) {
        t.rollback()
      }
    }
  }
  def transaction[T](block: => T): T = transaction((t: Transaction) => block)

  def companion[T: ClassManifest]: Object = Class.forName(implicitly[ClassManifest[T]].erasure.getName + "$").getField("MODULE$").get()

  def createMapper[T <: Mapper[T]: ClassManifest](entity: Entity): T = {
    def loadFromBlob(b: Blob): Serializable = {
      val in = new ObjectInputStream(new ByteArrayInputStream(b.getBytes))
      val s = in.readObject.asInstanceOf[Serializable]
      in.close()
      s
    }

    def scalaValueOfProperty(p: BaseProperty[_]): Any => Any = {
      case l: java.util.ArrayList[_] =>
        if (p.__isContentSerializable) {
          l.asInstanceOf[java.util.ArrayList[Blob]].asScala.map(loadFromBlob)
        } else {
          l.asScala
        }
      case s: java.util.HashSet[_] =>
        if (p.__isContentSerializable) {
          s.asInstanceOf[java.util.HashSet[Blob]].asScala.map(loadFromBlob)
        } else {
          s.asScala
        }
      case null if p.__isSeq => Seq()
      case null if p.__isSet => Set()
      case b: Blob if p.__isSerializable && !p.__isOption => loadFromBlob(b)
      case v =>
        if (p.__isOption) {
          if (p.__isContentSerializable) {
            Option(v).asInstanceOf[Option[Blob]].map(loadFromBlob)
          } else {
            Option(v)
          }
        } else {
          v
        }
    }

    val companionMapper = companion[T].asInstanceOf[T]
    val concreteClass = implicitly[ClassManifest[T]].erasure
    val mapper = concreteClass.newInstance.asInstanceOf[T]
    for {
      (name, value) <- entity.getProperties.asScala
      field = concreteClass.getDeclaredField(name)
      p = companionMapper.findProperty(name).get
    } {
      val v = if (p.__isMapper) null else scalaValueOfProperty(p)(value)
      val manifest = p.__manifest.asInstanceOf[Manifest[Any]]
      val p2 = if (p.__isUnindexed) UnindexedProperty(v)(manifest) else Property(v)(manifest)
      if (p.__isMapper) {
        p2.__keyOfMapper = Option(value.asInstanceOf[Key])
      }
      field.setAccessible(true)
      field.set(mapper, p2)
    }
    mapper.key = Option(entity.getKey)
    mapper.parentKey = Option(entity.getParent)
    mapper
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

  def get(key: Key) = Datastore.get(key)
  def get(key: Key*) = Datastore.get(key:_*)
  def get(txn: Transaction, key: Key) = Datastore.get(txn, key)
  def get(txn: Transaction, key: Key*) = Datastore.get(txn, key:_*)

  def getAsync(key: Key): Future[T] = Datastore.getAsync(key)
  def getAsync(key: Key*): Future[Iterable[T]] = Datastore.getAsync(key:_*)
  def getAsync(txn: Transaction, key: Key): Future[T] = Datastore.getAsync(txn, key)
  def getAsync(txn: Transaction, key: Key*): Future[Iterable[T]] = Datastore.getAsync(txn, key:_*)

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

  def properties: Seq[BaseProperty[_]] = zipPropertyAndMethod.map(_._1)

  def findProperty(name: String) = properties.find(_.__nameOfProperty == name)

  def fromEntity(entity: Entity): T = Datastore.createMapper(entity)

  def toEntity = {
    assignPropertyName()
    val entity = (key, parentKey) match {
      case (Some(k), _       ) => new Entity(k)
      case (None,    Some(pk)) => new Entity(kind, pk)
      case (None,    None    ) => new Entity(kind)
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

  def assignPropertyName() {
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

  def startCursor(cursor: Cursor) =
    new TypeSafeQuery(txn, mapper, ancestorKey, fetchOptions.startCursor(cursor), _reverse, filterPredicate, sortPredicate)
  def endCursor(cursor: Cursor) =
    new TypeSafeQuery(txn, mapper, ancestorKey, fetchOptions.endCursor(cursor), _reverse, filterPredicate, sortPredicate)
  def chunkSize(size: Int) =
    new TypeSafeQuery(txn, mapper, ancestorKey, fetchOptions.chunkSize(size), _reverse, filterPredicate, sortPredicate)
  def limit(limit: Int) =
    new TypeSafeQuery(txn, mapper, ancestorKey, fetchOptions.limit(limit), _reverse, filterPredicate, sortPredicate)
  def offset(offset: Int) =
    new TypeSafeQuery(txn, mapper, ancestorKey, fetchOptions.offset(offset), _reverse, filterPredicate, sortPredicate)
  def prefetchSize(size: Int) =
    new TypeSafeQuery(txn, mapper, ancestorKey, fetchOptions.prefetchSize(size), _reverse, filterPredicate, sortPredicate)
}
