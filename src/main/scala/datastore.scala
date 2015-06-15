package com.github.hexx.gaeds

import java.lang.reflect.{ Field, Method }
import java.util.concurrent.{ Future, TimeUnit }
import scala.collection.JavaConverters._
import scala.collection.mutable.Map
import scala.reflect.{classTag, ClassTag}
import com.google.appengine.api.datastore.{ DatastoreServiceFactory, KeyFactory, Entity, FetchOptions, Transaction }
import com.google.appengine.api.datastore.{ Key => LLKey }
import org.json4s._

object Datastore {
  val service = DatastoreServiceFactory.getDatastoreService
  val asyncService = DatastoreServiceFactory.getAsyncDatastoreService

  case class FutureWrapper[T, U](underlying: Future[T], f: T => U) extends Future[U] {
    def cancel(mayInterruptIfRunning: Boolean) = underlying.cancel(mayInterruptIfRunning)
    def get(): U = f(underlying.get())
    def get(timeout: Long, unit: TimeUnit): U = f(underlying.get(timeout, unit))
    def isCancelled() = underlying.isCancelled
    def isDone() = underlying.isDone
  }

  private def wrapGet[T <: Mapper[T]: ClassTag](entity: Entity): T =
    createMapper(entity)
  private def wrapGet[T <: Mapper[T]: ClassTag](entities: java.util.Map[LLKey, Entity]): Map[Key[T], T] =
    entities.asScala.map(v => (Key(v._1), createMapper(v._2)))

  def get[T <: Mapper[T]: ClassTag](key: Key[T]): T =
    wrapGet(service.get(key.key))
  def get[T <: Mapper[T]: ClassTag](keys: Key[T]*): Map[Key[T], T] =
    wrapGet(service.get(keys.map(_.key).asJava))
  def get[T <: Mapper[T]: ClassTag](txn: Transaction, key: Key[T]): T =
    wrapGet(service.get(txn, key.key))
  def get[T <: Mapper[T]: ClassTag](txn: Transaction, keys: Key[T]*): Map[Key[T], T] =
    wrapGet(service.get(txn, keys.map(_.key).asJava))

  def getAsync[T <: Mapper[T]: ClassTag](key: Key[T]): Future[T] =
    FutureWrapper(asyncService.get(key.key), wrapGet(_: Entity)(implicitly[ClassTag[T]]))
  def getAsync[T <: Mapper[T]: ClassTag](keys: Key[T]*): Future[Map[Key[T], T]] =
    FutureWrapper(asyncService.get(keys.map(_.key).asJava), wrapGet(_: java.util.Map[LLKey, Entity])(implicitly[ClassTag[T]]))
  def getAsync[T <: Mapper[T]: ClassTag](txn: Transaction, key: Key[T]): Future[T] =
    FutureWrapper(asyncService.get(txn, key.key), wrapGet(_: Entity)(implicitly[ClassTag[T]]))
  def getAsync[T <: Mapper[T]: ClassTag](txn: Transaction, keys: Key[T]*): Future[Map[Key[T], T]] =
    FutureWrapper(asyncService.get(txn, keys.map(_.key).asJava), wrapGet(_: java.util.Map[LLKey, Entity])(implicitly[ClassTag[T]]))

  private def wrapPut[T <: Mapper[T]: ClassTag](mapper: T)(llkey: LLKey) = {
    val key = Key(llkey)
    mapper.key = Option(key)
    key
  }
  private def wrapPut[T <: Mapper[T]: ClassTag](mappers: T*)(llkeys: java.util.List[LLKey]) = {
    val keys = llkeys.asScala.map(Key(_))
    for ((mapper, key) <- mappers zip keys) {
      mapper.key = Option(key)
    }
    keys
  }

  def put[T <: Mapper[T]: ClassTag](mapper: T) =
    wrapPut(mapper)(service.put(mapper.toEntity))
  def put[T <: Mapper[T]: ClassTag](mappers: T*): Seq[Key[T]] =
    wrapPut(mappers:_*)(service.put(mappers.map(_.toEntity).asJava))
  def put[T <: Mapper[T]: ClassTag](txn: Transaction, mapper: T) =
    wrapPut(mapper)(service.put(txn, mapper.toEntity))
  def put[T <: Mapper[T]: ClassTag](txn: Transaction, mappers: T*): Seq[Key[T]] =
    wrapPut(mappers:_*)(service.put(txn, mappers.map(_.toEntity).asJava))

  def putAsync[T <: Mapper[T]: ClassTag](mapper: T): Future[Key[T]] =
    FutureWrapper(asyncService.put(mapper.toEntity), wrapPut(mapper) _)
  def putAsync[T <: Mapper[T]: ClassTag](mappers: T*): Future[Seq[Key[T]]] =
    FutureWrapper(asyncService.put(mappers.map(_.toEntity).asJava), wrapPut(mappers:_*) _)
  def putAsync[T <: Mapper[T]: ClassTag](txn: Transaction, mapper: T): Future[Key[T]] =
    FutureWrapper(asyncService.put(txn, mapper.toEntity), wrapPut(mapper) _)
  def putAsync[T <: Mapper[T]: ClassTag](txn: Transaction, mappers: T*): Future[Seq[Key[T]]] =
    FutureWrapper(asyncService.put(txn, mappers.map(_.toEntity).asJava), wrapPut(mappers:_*) _)

  def delete[T <: Mapper[T]](keys: Key[T]*) = service.delete(keys.map(_.key):_*)
  def delete[T <: Mapper[T]](txn: Transaction, keys: Key[T]*) = service.delete(txn, keys.map(_.key):_*)

  def deleteAsync[T <: Mapper[T]](keys: Key[T]*) = asyncService.delete(keys.map(_.key):_*)
  def deleteAsync[T <: Mapper[T]](txn: Transaction, keys: Key[T]*) = asyncService.delete(txn, keys.map(_.key):_*)

  def query[T <: Mapper[T]: ClassTag](mapper: T) =
    new Query(None, mapper, None)
  def query[T <: Mapper[T]: ClassTag, U <: Mapper[U]](mapper: T, ancestorKey: Key[U]) =
    new Query(None, mapper, Option(ancestorKey))
  def query[T <: Mapper[T]: ClassTag](mapper: T, fetchOptions: FetchOptions) =
    new Query(None, mapper, None, fetchOptions)
  def query[T <: Mapper[T]: ClassTag, U <: Mapper[U]](mapper: T, ancestorKey: Key[U], fetchOptions: FetchOptions) =
    new Query(None, mapper, Some(ancestorKey), fetchOptions)
  def query[T <: Mapper[T]: ClassTag](txn: Transaction, mapper: T) =
    new Query(Option(txn), mapper, None)
  def query[T <: Mapper[T]: ClassTag, U <: Mapper[U]](txn: Transaction, mapper: T, ancestorKey: Key[U]) =
    new Query(Option(txn), mapper, Option(ancestorKey))
  def query[T <: Mapper[T]: ClassTag](txn: Transaction, mapper: T, fetchOptions: FetchOptions) =
    new Query(Option(txn), mapper, None, fetchOptions)
  def query[T <: Mapper[T]: ClassTag, U <: Mapper[U]](txn: Transaction, mapper: T, ancestorKey: Key[U], fetchOptions: FetchOptions) =
    new Query(Some(txn), mapper, Some(ancestorKey), fetchOptions)

  def companion[T: ClassTag]: Object = Class.forName(classTag.runtimeClass.getName + "$").getField("MODULE$").get(())
  def mapperCompanion[T <: Mapper[T]: ClassTag]: T = companion.asInstanceOf[T]

  def createMapper[T <: Mapper[T]: ClassTag](entity: Entity): T = {
    val concreteClass = implicitly[ClassTag[T]].runtimeClass
    val mapper = concreteClass.newInstance.asInstanceOf[T]
    for {
      (name, value) <- entity.getProperties.asScala
      p <- Datastore.mapperCompanion.findProperty(name)
      field = concreteClass.getDeclaredField(name)
    } {
      createPropertyAndSetToField(mapper, field, p, p.__llvalueToScalaValue(value))
    }
    mapper.key = Option(Key(entity.getKey))
    mapper
  }

  def createMapperFromJObject[T <: Mapper[T]: ClassTag](jobject: JObject): T = {
    val concreteClass = implicitly[ClassTag[T]].runtimeClass
    val mapper = concreteClass.newInstance.asInstanceOf[T]
    for {
      (name, value) <- jobject.obj
      p <- Datastore.mapperCompanion.findProperty(name)
      field = concreteClass.getDeclaredField(name)
    } {
      createPropertyAndSetToField(mapper, field, p, p.__jvalueToScalaValue(value))
    }
    mapper.key = jobject.obj.find(_._1 == "key") map { f =>
      Key(KeyFactory stringToKey f._2.asInstanceOf[JString].s)
    }
    mapper
  }

  private def createPropertyAndSetToField(m: Mapper[_], f: Field, p: BaseProperty[_], v: Any) {
    val manifest = p.__manifest.asInstanceOf[Manifest[Any]]
    val p2 =
      if (p.__isUnindexed) {
        UnindexedProperty(v)(manifest)
      } else {
        Property(v)(manifest)
      }
    f.setAccessible(true)
    f.set(m, p2)
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
  def allocateIdRange[T <: Mapper[T]](range: KeyRange[T]) =
    service.allocateIdRange(range.range)

  def allocateIds[T <: Mapper[T]: ClassTag, U <: Mapper[U]](parent: Key[U], num: Long): KeyRange[T] =
    KeyRange(service.allocateIds(mapperCompanion[T].kind, num))
  def allocateIds[T <: Mapper[T]: ClassTag](num: Long): KeyRange[T] =
    KeyRange(service.allocateIds(mapperCompanion[T].kind, num))

  def allocateId[T <: Mapper[T]: ClassTag, U <: Mapper[U]](parent: Key[U]): Key[T] =
    allocateIds(parent, 1).iterator.next
  def allocateId[T <: Mapper[T]: ClassTag](): Key[T] = allocateIds(1).iterator.next

  // wrapping KeyFactory
  def createKey[T <: Mapper[T]: ClassTag](id: Long): Key[T] =
    Key(KeyFactory.createKey(mapperCompanion[T].kind, id))
  def createKey[T <: Mapper[T]: ClassTag](name: String): Key[T] =
    Key(KeyFactory.createKey(mapperCompanion[T].kind, name))
  def createKey[T <: Mapper[T]: ClassTag, U <: Mapper[U]](parent: Key[U], id: Long): Key[T] =
    Key(KeyFactory.createKey(parent.key, mapperCompanion[T].kind, id))
  def createKey[T <: Mapper[T]: ClassTag, U <: Mapper[U]](parent: Key[U], name: String): Key[T] =
    Key(KeyFactory.createKey(parent.key, mapperCompanion[T].kind, name))

  def createKeyString[T <: Mapper[T]: ClassTag](id: Long): String =
    KeyFactory.createKeyString(mapperCompanion[T].kind, id)
  def createKeyString[T <: Mapper[T]: ClassTag](name: String): String =
    KeyFactory.createKeyString(mapperCompanion[T].kind, name)
  def createKeyString[T <: Mapper[T]: ClassTag, U <: Mapper[U]](parent: Key[U], id: Long): String =
    KeyFactory.createKeyString(parent.key, mapperCompanion[T].kind, id)
  def createKeyString[T <: Mapper[T]: ClassTag, U <: Mapper[U]](parent: Key[U], name: String): String =
    KeyFactory.createKeyString(parent.key, mapperCompanion[T].kind, name)

  def keyToString[T <: Mapper[T]](key: Key[T]) = KeyFactory.keyToString(key.key)
  def stringToKey[T <: Mapper[T]: ClassTag](encoded: String) = Key[T](KeyFactory.stringToKey(encoded))

  // wrapping KeyBuilder
  class KeyBuilder[T <: Mapper[T]: ClassTag](builder: KeyFactory.Builder) {
    def addChild[U <: Mapper[U]: ClassTag](id: Long): KeyBuilder[U] =
      new KeyBuilder(builder.addChild(mapperCompanion[U].kind, id))
    def addChild[U <: Mapper[U]: ClassTag](name: String): KeyBuilder[U] =
      new KeyBuilder(builder.addChild(mapperCompanion[U].kind, name))
    def key = Key(builder.getKey)
  }

  def keyBuilder[T <: Mapper[T]: ClassTag](key: Key[T]) = new KeyBuilder(new KeyFactory.Builder(key.key))
  def keyBuilder[T <: Mapper[T]: ClassTag](id: Long) = new KeyBuilder(new KeyFactory.Builder(mapperCompanion.kind, id))
  def keyBuilder[T <: Mapper[T]: ClassTag](name: String) = new KeyBuilder(new KeyFactory.Builder(mapperCompanion.kind, name))
}

trait DatastoreDelegate[T <: Mapper[T]] {
  implicit val mapperClassTag: ClassTag[T]

  def get(key: Key[T]) = Datastore.get(key)
  def get(keys: Key[T]*) = Datastore.get(keys:_*)
  def get(txn: Transaction, key: Key[T]) = Datastore.get(txn, key)
  def get(txn: Transaction, keys: Key[T]*) = Datastore.get(txn, keys:_*)

  def getAsync(key: Key[T]): Future[T] = Datastore.getAsync(key)
  def getAsync(keys: Key[T]*): Future[Map[Key[T], T]] = Datastore.getAsync(keys:_*)
  def getAsync(txn: Transaction, key: Key[T]): Future[T] = Datastore.getAsync(txn, key)
  def getAsync(txn: Transaction, keys: Key[T]*): Future[Map[Key[T], T]] = Datastore.getAsync(txn, keys:_*)

  def put(mapper: T) = Datastore.put(mapper)
  def put(mappers: T*) = Datastore.put(mappers:_*)
  def put(txn: Transaction, mapper: T) = Datastore.put(txn, mapper)
  def put(txn: Transaction, mappers: T*) = Datastore.put(txn, mappers:_*)

  def putAsync(mapper: T) = Datastore.putAsync(mapper)
  def putAsync(mappers: T*) = Datastore.putAsync(mappers:_*)
  def putAsync(txn: Transaction, mapper: T) = Datastore.putAsync(txn, mapper)
  def putAsync(txn: Transaction, mappers: T*) = Datastore.putAsync(txn, mappers:_*)

  def delete(keys: Key[T]*) = Datastore.delete(keys:_*)
  def delete(txn: Transaction, keys: Key[T]*) = Datastore.delete(txn, keys:_*)

  def deleteAsync(keys: Key[T]*) = Datastore.delete(keys:_*)
  def deleteAsync(txn: Transaction, keys: Key[T]*) = Datastore.delete(txn, keys:_*)

  def allocateIdRange(range: KeyRange[T]) = Datastore.allocateIdRange(range)

  def allocateIds(num: Long) = Datastore.allocateIds(num)
  def allocateIds[U <: Mapper[U]](parent: Key[U], num: Long) = Datastore.allocateIds(parent, num)

  def allocateId() = Datastore.allocateId()
  def allocateId[U <: Mapper[U]](parent: Key[U]) = Datastore.allocateId(parent)

  def createKey(id: Long) = Datastore.createKey(id)
  def createKey(name: String) = Datastore.createKey(name)
  def createKey[U <: Mapper[U]](parent: Key[U], id: Long) = Datastore.createKey(parent, id)
  def createKey[U <: Mapper[U]](parent: Key[U], name: String) = Datastore.createKey(parent, name)

  def createKeyString(id: Long) = Datastore.createKeyString(id)
  def createKeyString(name: String) = Datastore.createKeyString(name)
  def createKeyString[U <: Mapper[U]](parent: Key[U], id: Long) = Datastore.createKeyString(parent, id)
  def createKeyString[U <: Mapper[U]](parent: Key[U], name: String) = Datastore.createKeyString(parent, name)

  def keyToString(key: Key[T]) = Datastore.keyToString(key)
  def stringToKey(encoded: String) = Datastore.stringToKey(encoded)

  def keyBuilder(key: Key[T]) = Datastore.keyBuilder(key)
  def keyBuilder(id: Long) = Datastore.keyBuilder(id)
  def keyBuilder(name: String) = Datastore.keyBuilder(name)
}
