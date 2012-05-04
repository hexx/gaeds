package com.github.hexx.gaeds

import java.io.{ ByteArrayInputStream, ObjectInputStream }
import java.lang.reflect.{ Field, Method }
import java.util.concurrent.{ Future, TimeUnit }
import scala.collection.JavaConverters._
import scala.collection.mutable.Map
import scala.collection.mutable.ListBuffer
import com.google.appengine.api.datastore._
import com.google.appengine.api.datastore.Query.FilterOperator
import com.google.appengine.api.datastore.Query.SortDirection

object Datastore {
  val service = DatastoreServiceFactory.getDatastoreService
  val asyncService = DatastoreServiceFactory.getAsyncDatastoreService

  case class FutureWrapper[T, U](underlying: Future[T], f: T => U) extends Future[U] {
    def	cancel(mayInterruptIfRunning: Boolean) = underlying.cancel(mayInterruptIfRunning)
    def get(): U = f(underlying.get())
    def get(timeout: Long, unit: TimeUnit): U = f(underlying.get(timeout, unit))
    def isCancelled() = underlying.isCancelled
    def isDone() = underlying.isDone
  }

  private def wrapGet[T <: Mapper[T]: ClassManifest](entity: Entity): T =
    createMapper(entity)
  private def wrapGet[T <: Mapper[T]: ClassManifest](entities: java.util.Map[Key, Entity]): Map[TypeSafeKey[T], T] =
    entities.asScala.map(v => (TypeSafeKey(v._1), createMapper(v._2)))

  def get[T <: Mapper[T]: ClassManifest](key: TypeSafeKey[T]): T =
    wrapGet(service.get(key.key))
  def get[T <: Mapper[T]: ClassManifest](keys: TypeSafeKey[T]*): Map[TypeSafeKey[T], T] =
    wrapGet(service.get(keys.map(_.key).asJava))
  def get[T <: Mapper[T]: ClassManifest](txn: Transaction, key: TypeSafeKey[T]): T =
    wrapGet(service.get(txn, key.key))
  def get[T <: Mapper[T]: ClassManifest](txn: Transaction, keys: TypeSafeKey[T]*): Map[TypeSafeKey[T], T] =
    wrapGet(service.get(txn, keys.map(_.key).asJava))

  def getAsync[T <: Mapper[T]: ClassManifest](key: TypeSafeKey[T]): Future[T] =
    FutureWrapper(asyncService.get(key.key), wrapGet(_: Entity)(implicitly[ClassManifest[T]]))
  def getAsync[T <: Mapper[T]: ClassManifest](keys: TypeSafeKey[T]*): Future[Map[TypeSafeKey[T], T]] =
    FutureWrapper(asyncService.get(keys.map(_.key).asJava), wrapGet(_: java.util.Map[Key, Entity])(implicitly[ClassManifest[T]]))
  def getAsync[T <: Mapper[T]: ClassManifest](txn: Transaction, key: TypeSafeKey[T]): Future[T] =
    FutureWrapper(asyncService.get(txn, key.key), wrapGet(_: Entity)(implicitly[ClassManifest[T]]))
  def getAsync[T <: Mapper[T]: ClassManifest](txn: Transaction, keys: TypeSafeKey[T]*): Future[Map[TypeSafeKey[T], T]] =
    FutureWrapper(asyncService.get(txn, keys.map(_.key).asJava), wrapGet(_: java.util.Map[Key, Entity])(implicitly[ClassManifest[T]]))

  private def wrapPut[T <: Mapper[T]: ClassManifest](mapper: T)(key: Key) = {
    mapper.key = Option(TypeSafeKey(key))
    TypeSafeKey(key)
  }
  private def wrapPut[T <: Mapper[T]: ClassManifest](mappers: T*)(keys: java.util.List[Key]) = {
    for ((mapper, key) <- mappers zip keys.asScala) {
      mapper.key = Option(TypeSafeKey(key))
    }
    keys.asScala.map(TypeSafeKey(_))
  }

  def put[T <: Mapper[T]: ClassManifest](mapper: T) =
    wrapPut(mapper)(service.put(mapper.toEntity))
  def put[T <: Mapper[T]: ClassManifest](mappers: T*) =
    wrapPut(mappers:_*)(service.put(mappers.map(_.toEntity).asJava))
  def put[T <: Mapper[T]: ClassManifest](txn: Transaction, mapper: T) =
    wrapPut(mapper)(service.put(txn, mapper.toEntity))
  def put[T <: Mapper[T]: ClassManifest](txn: Transaction, mappers: T*) =
    wrapPut(mappers:_*)(service.put(txn, mappers.map(_.toEntity).asJava))

  def putAsync[T <: Mapper[T]: ClassManifest](mapper: T) =
    FutureWrapper(asyncService.put(mapper.toEntity), wrapPut(mapper) _)
  def putAsync[T <: Mapper[T]: ClassManifest](mappers: T*) =
    FutureWrapper(asyncService.put(mappers.map(_.toEntity).asJava), wrapPut(mappers:_*) _)
  def putAsync[T <: Mapper[T]: ClassManifest](txn: Transaction, mapper: T) =
    FutureWrapper(asyncService.put(txn, mapper.toEntity), wrapPut(mapper) _)
  def putAsync[T <: Mapper[T]: ClassManifest](txn: Transaction, mappers: T*) =
    FutureWrapper(asyncService.put(txn, mappers.map(_.toEntity).asJava), wrapPut(mappers:_*) _)

  def delete[T <: Mapper[T]](keys: TypeSafeKey[T]*) = service.delete(keys.map(_.key):_*)
  def delete[T <: Mapper[T]](txn: Transaction, keys: TypeSafeKey[T]*) = service.delete(txn, keys.map(_.key):_*)

  def deleteAsync[T <: Mapper[T]](keys: TypeSafeKey[T]*) = asyncService.delete(keys.map(_.key):_*)
  def deleteAsync[T <: Mapper[T]](txn: Transaction, keys: TypeSafeKey[T]*) = asyncService.delete(txn, keys.map(_.key):_*)

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

  def companion[T](implicit classManifest: ClassManifest[T]): Object = Class.forName(classManifest.erasure.getName + "$").getField("MODULE$").get()
  def mapperCompanion[T <: Mapper[T]: ClassManifest]: T = companion.asInstanceOf[T]

  def createMapper[T <: Mapper[T]: ClassManifest](entity: Entity): T = {
    def loadSerializable(b: Blob) = {
      val in = new ObjectInputStream(new ByteArrayInputStream(b.getBytes))
      val s = in.readObject.asInstanceOf[Serializable]
      in.close()
      s
    }

    def scalaValueOfProperty(p: BaseProperty[_], value: Any) = value match {
      case l: java.util.ArrayList[_] =>
        if (p.__isContentSerializable) {
          l.asInstanceOf[java.util.ArrayList[Blob]].asScala.map(loadSerializable)
        } else {
          l.asScala
        }
      case null if p.__isSeq => Seq()
      case b: Blob if p.__isSerializable && !p.__isOption => loadSerializable(b)
      case _ if p.__isOption => 
        val o = Option(value)
        if (p.__isContentSerializable) {
          o.asInstanceOf[Option[Blob]].map(loadSerializable)
        } else {
          o
        }
      case _ => value
    }

    def createPropertyAndSetToField(m: Mapper[_], f: Field, p: BaseProperty[_], v: Any) {
      val manifest = p.__manifest.asInstanceOf[Manifest[Any]]
      val p2 =
        if (p.__isUnindexed) {
          UnindexedProperty(v)(manifest)
        } else {
          Property(v)(manifest)
        }
      p2.__isModified = false
      f.setAccessible(true)
      f.set(m, p2)
    }

    val fieldAndKeys = new ListBuffer[(Field, BaseProperty[_], Key)]
    val fieldAndSeqKeys = new ListBuffer[(Field, BaseProperty[_], Seq[Key])]
    val fieldAndOptionKey = new ListBuffer[(Field, BaseProperty[_], Key)]
    val concreteClass = implicitly[ClassManifest[T]].erasure
    val mapper = concreteClass.newInstance.asInstanceOf[T]
    for {
      (name, value) <- entity.getProperties.asScala
      field = concreteClass.getDeclaredField(name)
      p = Datastore.mapperCompanion.findProperty(name).get
    } {
      if (p.__isMapper) {
        fieldAndKeys += ((field, p, value.asInstanceOf[Key]))
      } else if (p.__isSeq && p.__isContentMapper && value != null) {
        fieldAndSeqKeys += ((field, p, value.asInstanceOf[java.util.ArrayList[Key]].asScala))
      } else if (p.__isOption && p.__isContentMapper && value != null) {
        fieldAndOptionKey += ((field, p, value.asInstanceOf[Key]))
      } else {
        createPropertyAndSetToField(mapper, field, p, scalaValueOfProperty(p, value))
      }
    }
    val keys = fieldAndKeys.map(_._3) ++ fieldAndSeqKeys.flatMap(_._3) ++ fieldAndOptionKey.map(_._3)
    val entities = Datastore.service.get(keys.asJava).asScala
    for ((f, p, k) <- fieldAndKeys) {
      createPropertyAndSetToField(mapper, f, p, createMapper(entities(k)))
    }
    for ((f, p, ks) <- fieldAndSeqKeys) {
      createPropertyAndSetToField(mapper, f, p, ks.map(k => createMapper(entities(k))))
    }
    for ((f, p, k) <- fieldAndOptionKey) {
      createPropertyAndSetToField(mapper, f, p, Option(createMapper(entities(k))))
    }
    mapper.key = Option(TypeSafeKey(entity.getKey))
    mapper
  }

  // Transaction
  def beginTransaction() = service.beginTransaction()
  def activeTransactions() = service.getActiveTransactions()
  def currentTransaction() = service.getCurrentTransaction()
  def currentTransaction(returnedIfNoTxn: Transaction) = service.getCurrentTransaction(returnedIfNoTxn)

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

  // Key
  def allocateIdRange[T <: Mapper[T]](range: TypeSafeKeyRange[T]) =
    service.allocateIdRange(range.range)

  def allocateIds[T <: Mapper[T]: ClassManifest, U <: Mapper[U]](parent: TypeSafeKey[U], num: Long): TypeSafeKeyRange[T] =
    TypeSafeKeyRange(service.allocateIds(mapperCompanion[T].kind, num))
  def allocateIds[T <: Mapper[T]: ClassManifest](num: Long): TypeSafeKeyRange[T] =
    TypeSafeKeyRange(service.allocateIds(mapperCompanion[T].kind, num))

  def allocateId[T <: Mapper[T]: ClassManifest, U <: Mapper[U]](parent: TypeSafeKey[U]): TypeSafeKey[T] =
    allocateIds(parent, 1).iterator.next
  def allocateId[T <: Mapper[T]: ClassManifest](): TypeSafeKey[T] = allocateIds(1).iterator.next

  // wrapping KeyFactory
  def createKey[T <: Mapper[T]: ClassManifest](id: Long): TypeSafeKey[T] =
    TypeSafeKey(KeyFactory.createKey(mapperCompanion[T].kind, id))
  def createKey[T <: Mapper[T]: ClassManifest](name: String): TypeSafeKey[T] =
    TypeSafeKey(KeyFactory.createKey(mapperCompanion[T].kind, name))
  def createKey[T <: Mapper[T]: ClassManifest, U <: Mapper[U]](parent: TypeSafeKey[U], id: Long): TypeSafeKey[T] =
    TypeSafeKey(KeyFactory.createKey(parent.key, mapperCompanion[T].kind, id))
  def createKey[T <: Mapper[T]: ClassManifest, U <: Mapper[U]](parent: TypeSafeKey[U], name: String): TypeSafeKey[T] =
    TypeSafeKey(KeyFactory.createKey(parent.key, mapperCompanion[T].kind, name))

  def createKeyString[T <: Mapper[T]: ClassManifest](id: Long): String =
    KeyFactory.createKeyString(mapperCompanion[T].kind, id)
  def createKeyString[T <: Mapper[T]: ClassManifest](name: String): String =
    KeyFactory.createKeyString(mapperCompanion[T].kind, name)
  def createKeyString[T <: Mapper[T]: ClassManifest, U <: Mapper[U]](parent: TypeSafeKey[U], id: Long): String =
    KeyFactory.createKeyString(parent.key, mapperCompanion[T].kind, id)
  def createKeyString[T <: Mapper[T]: ClassManifest, U <: Mapper[U]](parent: TypeSafeKey[U], name: String): String =
    KeyFactory.createKeyString(parent.key, mapperCompanion[T].kind, name)

  def keyToString[T <: Mapper[T]](key: TypeSafeKey[T]) = KeyFactory.keyToString(key.key)
  def stringToKey[T <: Mapper[T]](encoded: String) = TypeSafeKey(KeyFactory.stringToKey(encoded))

  // wrapping KeyBuilder
  class TypeSafeKeyBuilder[T <: Mapper[T]: ClassManifest](builder: KeyFactory.Builder) {
    def addChild[U <: Mapper[U]: ClassManifest](id: Long): TypeSafeKeyBuilder[U] =
      new TypeSafeKeyBuilder(builder.addChild(mapperCompanion[U].kind, id))
    def addChild[U <: Mapper[U]: ClassManifest](name: String): TypeSafeKeyBuilder[U] =
      new TypeSafeKeyBuilder(builder.addChild(mapperCompanion[U].kind, name))
    def key = TypeSafeKey(builder.getKey)
  }

  def keyBuilder[T <: Mapper[T]: ClassManifest](key: TypeSafeKey[T]) = new TypeSafeKeyBuilder(new KeyFactory.Builder(key.key))
  def keyBuilder[T <: Mapper[T]: ClassManifest](id: Long) = new TypeSafeKeyBuilder(new KeyFactory.Builder(mapperCompanion.kind, id))
  def keyBuilder[T <: Mapper[T]: ClassManifest](name: String) = new TypeSafeKeyBuilder(new KeyFactory.Builder(mapperCompanion.kind, name))
}

abstract class Mapper[T <: Mapper[T]: ClassManifest] {
  self: T =>

  assignPropertyName()

  var key: Option[TypeSafeKey[T]] = None

  def kind = concreteClass.getName // override to customize

  def concreteClass = implicitly[ClassManifest[T]].erasure

  def put() = Datastore.put(this)
  def put(txn: Transaction) = Datastore.put(txn, this)

  def putAsync() = Datastore.putAsync(this)
  def putAsync(txn: Transaction) = Datastore.putAsync(txn, this)

  def get(key: TypeSafeKey[T]) = Datastore.get(key)
  def get(keys: TypeSafeKey[T]*) = Datastore.get(keys:_*)
  def get(txn: Transaction, key: TypeSafeKey[T]) = Datastore.get(txn, key)
  def get(txn: Transaction, keys: TypeSafeKey[T]*) = Datastore.get(txn, keys:_*)

  def getAsync(key: TypeSafeKey[T]): Future[T] = Datastore.getAsync(key)
  def getAsync(keys: TypeSafeKey[T]*): Future[Map[TypeSafeKey[T], T]] = Datastore.getAsync(keys:_*)
  def getAsync(txn: Transaction, key: TypeSafeKey[T]): Future[T] = Datastore.getAsync(txn, key)
  def getAsync(txn: Transaction, keys: TypeSafeKey[T]*): Future[Map[TypeSafeKey[T], T]] = Datastore.getAsync(txn, keys:_*)

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

  def allocateIdRange(range: TypeSafeKeyRange[T]) = Datastore.allocateIdRange(range)

  def allocateIds(num: Long) = Datastore.allocateIds(num)
  def allocateIds[U <: Mapper[U]](parent: TypeSafeKey[U], num: Long) = Datastore.allocateIds(parent, num)

  def allocateId(num: Long) = Datastore.allocateId()
  def allocateId[U <: Mapper[U]](parent: TypeSafeKey[U], num: Long) = Datastore.allocateId(parent)

  def createKey(id: Long) = Datastore.createKey(id)
  def createKey(name: String) = Datastore.createKey(name)
  def createKey[U <: Mapper[U]](parent: TypeSafeKey[U], id: Long) = Datastore.createKey(parent, id)
  def createKey[U <: Mapper[U]](parent: TypeSafeKey[U], name: String) = Datastore.createKey(parent, name)

  def createKeyString(id: Long) = Datastore.createKeyString(id)
  def createKeyString(name: String) = Datastore.createKeyString(name)
  def createKeyString[U <: Mapper[U]](parent: TypeSafeKey[U], id: Long) = Datastore.createKeyString(parent, id)
  def createKeyString[U <: Mapper[U]](parent: TypeSafeKey[U], name: String) = Datastore.createKeyString(parent, name)

  def keyToString(key: TypeSafeKey[T]) = Datastore.keyToString(key)
  def stringToKey(encoded: String) = Datastore.stringToKey(encoded)

  def keyBuilder(key: TypeSafeKey[T]) = Datastore.keyBuilder(key)
  def keyBuilder(id: Long) = Datastore.keyBuilder(id)
  def keyBuilder(name: String) = Datastore.keyBuilder(name)

  def properties: Seq[BaseProperty[_]] = zipPropertyAndMethod.map(_._1)

  def findProperty(name: String) = properties.find(_.__nameOfProperty == name)

  def fromEntity(entity: Entity): T = Datastore.createMapper(entity)

  def toEntity = {
    assignPropertyName()
    val entity = key match {
      case Some(k) => new Entity(k.key)
      case None => new Entity(kind)
    }
    assert(properties.size != 0, "define fields with Property[T]")
    properties foreach (_.__setToEntity(entity))
    entity
  }

  def isModified = properties.exists(_.__isModified)

  override def equals(that: Any) = that match {
    // case that: Mapper[_] => that.key == key && that.properties == properties
    case that: Mapper[_] => true
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

  // wrapping FetchOptions
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

case class TypeSafeKey[T <: Mapper[T]: ClassManifest](val key: Key) extends Ordered[TypeSafeKey[T]] {
  def id = key.getId
  def kind = key.getKind
  def name = key.getName
  def namespace = key.getNamespace
  def isComplete = key.isComplete
  def parent[U <: Mapper[U]: ClassManifest]: Option[TypeSafeKey[U]] = Option(key.getParent).map(TypeSafeKey(_))
  override def toString = key.toString
  override def compare(that: TypeSafeKey[T]) = key compareTo that.key
}

case class TypeSafeKeyRange[T <: Mapper[T]: ClassManifest](range: KeyRange) extends Iterable[TypeSafeKey[T]] {
  override def iterator = range.iterator.asScala.map(TypeSafeKey(_))
}
