package com.github.hexx.gaeds

import java.lang.reflect.Method
import java.util.Date
import java.util.concurrent.{ Future, TimeUnit }
import scala.collection.JavaConverters._
import com.google.appengine.api.blobstore.BlobKey
import com.google.appengine.api.datastore._
import com.google.appengine.api.datastore.Query.FilterOperator
import com.google.appengine.api.datastore.Query.SortDirection
import com.google.appengine.api.users.User

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

  def createMapper[T <: Mapper[T]: ClassManifest](entity: Entity): T = {
    val concreteClass = implicitly[ClassManifest[T]].erasure
    val mapper = concreteClass.newInstance.asInstanceOf[T]
    for ((name, value) <- entity.getProperties.asScala) {
      val field = concreteClass.getDeclaredField(name)
      field.setAccessible(true)
      field.set(mapper, Property(value))
    }
    mapper.key = Option(entity.getKey)
    mapper.parentKey = Option(entity.getParent)
    mapper
  }

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

case class FilterPredicate[T](val property: Property[T], val operator: FilterOperator, val value: T*)
case class SortPredicate(val property: Property[_], val direction: SortDirection)

case class PropertyOperator[T: ClassManifest](property: Property[T]) {
  def #<(v: T) = FilterPredicate(property, FilterOperator.LESS_THAN, v)
  def #<=(v: T) = FilterPredicate(property, FilterOperator.LESS_THAN_OR_EQUAL, v)
  def #==(v: T) = FilterPredicate(property, FilterOperator.EQUAL, v)
  def #!=(v: T) = FilterPredicate(property, FilterOperator.NOT_EQUAL, v)
  def #>(v: T) = FilterPredicate(property, FilterOperator.GREATER_THAN, v)
  def #>=(v: T) = FilterPredicate(property, FilterOperator.GREATER_THAN_OR_EQUAL, v)
  def in(v: T*) = FilterPredicate(property, FilterOperator.IN, v:_*)
  def asc = SortPredicate(property, SortDirection.ASCENDING)
  def desc = SortPredicate(property, SortDirection.DESCENDING)
}

object Property {
  implicit def propertyToValue[T](property: Property[T]): T = property.value
  implicit def shortBlobValueToProperty(value: ShortBlob) = Property(value)
  implicit def blobValueToProperty(value: Blob) = Property(value)
  implicit def categoryValueToProperty(value: Category) = Property(value)
  implicit def booleanValueToProperty(value: Boolean) = Property(value)
  implicit def dateValueToProperty(value: Date) = Property(value)
  implicit def emailValueToProperty(value: Email) = Property(value)
  implicit def floatValueToProperty(value: Float) = Property(value)
  implicit def doubleValueToProperty(value: Double) = Property(value)
  implicit def geoPtValueToProperty(value: GeoPt) = Property(value)
  implicit def userValueToProperty(value: User) = Property(value)
  implicit def shortValueToProperty(value: Short) = Property(value)
  implicit def intValueToProperty(value: Int) = Property(value)
  implicit def longValueToProperty(value: Long) = Property(value)
  implicit def blobKeyValueToProperty(value: BlobKey) = Property(value)
  implicit def keyValueToProperty(value: Key) = Property(value)
  implicit def linkValueToProperty(value: Link) = Property(value)
  implicit def imHandleValueToProperty(value: IMHandle) = Property(value)
  implicit def postalAddressValueToProperty(value: PostalAddress) = Property(value)
  implicit def ratingValueToProperty(value: Rating) = Property(value)
  implicit def phoneNumberValueToProperty(value: PhoneNumber) = Property(value)
  implicit def stringValueToProperty(value: String) = Property(value)
  implicit def textValueToProperty(value: Text) = Property(value)

  implicit def propertyToOperator[T: ClassManifest](property: Property[T]) = PropertyOperator(property)
}

case class Property[T: ClassManifest](var value: T) {
  var name: String = _
  override def toString = value.toString
}

abstract class Mapper[T <: Mapper[T]: ClassManifest] {
  self: T =>

  assignPropertyName()

  var key: Option[Key] = None
  var parentKey: Option[Key] = None

  def kind = implicitly[ClassManifest[T]].erasure.getName // override to customize

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

  def fromEntity(entity: Entity): T = Datastore.createMapper(entity)

  def properties: Seq[Property[_]] = zipPropertyAndMethod.map(_._1)

  def toEntity = {
    val entity = (key, parentKey) match {
      case (Some(k), _      )  => new Entity(k)
      case (None,    Some(pk)) => new Entity(kind, pk)
      case (None,    None   )  => new Entity(kind)
    }
    assert(properties.size != 0, "define fields with Property[T]")
    for (p <- properties) {
      entity.setProperty(p.name, p.value)
    }
    entity
  }

  override def equals(that: Any) = that match {
    case that: Mapper[_] => that.key == key && that.properties == properties
    case _ => false
  }

  private def zipPropertyAndMethod: Seq[(Property[_], Method)] = {
    def isGetter(m: Method) = !m.isSynthetic && classOf[Property[_]].isAssignableFrom(m.getReturnType)
    for {
      m <- this.getClass.getMethods
      if isGetter(m)
      p = m.invoke(this).asInstanceOf[Property[_]]
    } yield (p, m)
  }

  private def assignPropertyName() {
    for ((p, m) <- zipPropertyAndMethod) {
      p.name = m.getName
    }
  }
}

class TypeSafeQuery[T <: Mapper[T]: ClassManifest](
    txn: Option[Transaction],
    mapper: T,
    ancestorKey: Option[Key],
    fetchOptions: FetchOptions = FetchOptions.Builder.withDefaults,
    var _reverse: Boolean = false,
    filterPredicate: List[FilterPredicate[_]] = List(),
    sortPredicate: List[SortPredicate] = List()) {
  def addFilter(f: T => FilterPredicate[_]) =
    new TypeSafeQuery(txn, mapper, ancestorKey, fetchOptions, _reverse, f(mapper) :: filterPredicate, sortPredicate)
  def addSort(f: T => SortPredicate) =
    new TypeSafeQuery(txn, mapper, ancestorKey, fetchOptions, _reverse, filterPredicate, f(mapper) :: sortPredicate)

  def asEntityIterator(keysOnly: Boolean) = prepare(keysOnly).asIterator(fetchOptions).asScala
  def asQueryResultIterator(keysOnly: Boolean) = prepare(keysOnly).asQueryResultIterator(fetchOptions)

  def asIterator(): Iterator[T] = asEntityIterator(false).map(mapper.fromEntity(_))
  def asKeysOnlyIterator(): Iterator[Key] = asEntityIterator(true).map(_.getKey)

  def asIteratorWithCursorAndIndex(): Iterator[(T, () => Cursor, () => Seq[Index])] = {
    val iterator = asQueryResultIterator(false)
    iterator.asScala.map(entity => (mapper.fromEntity(entity), iterator.getCursor _, () => iterator.getIndexList.asScala.toSeq))
  }
  def asKeysOnlyIteratorWithCursorAndIndex(): Iterator[(Key, () => Cursor, () => Seq[Index])] = {
    val iterator = asQueryResultIterator(true)
    iterator.asScala.map(entity => (entity.getKey, iterator.getCursor _, () => iterator.getIndexList.asScala.toSeq))
  }

  def asSingle(): T = mapper.fromEntity(prepare(false).asSingleEntity)

  def countEntities(): Int = prepare(false).countEntities(fetchOptions)

  def prepare(keysOnly: Boolean) = {
    txn match {
      case Some(t) => Datastore.service.prepare(t, toQuery(keysOnly))
      case None => Datastore.service.prepare(toQuery(keysOnly))
    }
  }

  def reverse() = {
    _reverse = !_reverse
    new TypeSafeQuery(txn, mapper, ancestorKey, fetchOptions, !_reverse, filterPredicate, sortPredicate)
  }

  def toQuery(keysOnly: Boolean) = {
    val query = ancestorKey match {
      case Some(k) => new Query(mapper.kind, k)
      case None => new Query(mapper.kind)
    }
    for (p <- filterPredicate) {
      if (p.operator == FilterOperator.IN) {
        query.addFilter(p.property.name, p.operator, p.value.asJava)
      } else {
        query.addFilter(p.property.name, p.operator, p.value(0))
      }
    }
    for (p <- sortPredicate) {
      query.addSort(p.property.name, p.direction)
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
