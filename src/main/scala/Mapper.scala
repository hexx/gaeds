package com.github.hexx.gaeds

import java.lang.reflect.{ Field, Method }
import com.google.appengine.api.datastore.{ Entity, FetchOptions, Transaction }

abstract class Mapper[T <: Mapper[T]: ClassManifest] extends DatastoreDelegate[T] {
  self: T =>

  assignPropertyName()

  var key: Option[Key[T]] = None

  def kind = concreteClass.getName // override to customize

  def concreteClass = implicitly[ClassManifest[T]].erasure

  def put() = Datastore.put(this)
  def put(txn: Transaction) = Datastore.put(txn, this)

  def putAsync() = Datastore.putAsync(this)
  def putAsync(txn: Transaction) = Datastore.putAsync(txn, this)

  def query() = Datastore.query(this)
  def query[U <: Mapper[U]](ancestorKey: Key[U]) = Datastore.query(this, ancestorKey)
  def query(fetchOptions: FetchOptions) = Datastore.query(this, fetchOptions)
  def query[U <: Mapper[U]](ancestorKey: Key[U], fetchOptions: FetchOptions) =
    Datastore.query(this, ancestorKey, fetchOptions)
  def query(txn: Transaction) = Datastore.query(txn, this)
  def query[U <: Mapper[U]](txn: Transaction, ancestorKey: Key[U]) = Datastore.query(txn, this, ancestorKey)
  def query(txn: Transaction, fetchOptions: FetchOptions) = Datastore.query(txn, this, fetchOptions)
  def query[U <: Mapper[U]](txn: Transaction, ancestorKey: Key[U], fetchOptions: FetchOptions) =
    Datastore.query(txn, this, ancestorKey, fetchOptions)

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

  override def equals(that: Any) = that match {
    case that: Mapper[_] => that.key == key && that.properties == properties
    case _ => false
  }

  override val mapperClassManifest = implicitly[ClassManifest[T]]

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
