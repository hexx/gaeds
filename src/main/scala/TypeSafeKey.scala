package com.github.hexx.gaeds

import scala.collection.JavaConverters._
import com.google.appengine.api.datastore.{ Key => GAEKey, KeyRange => GAEKeyRange }

case class Key[T <: Mapper[T]: ClassManifest](val key: GAEKey) extends Ordered[Key[T]] {
  assert(key != null)
  def id = key.getId
  def kind = key.getKind
  def name = key.getName
  def namespace = key.getNamespace
  def isComplete = key.isComplete
  def parent[U <: Mapper[U]: ClassManifest]: Option[Key[U]] = Option(key.getParent).map(Key(_))

  def get = Datastore.get(this)
  def getAync = Datastore.getAsync(this)
  def delete = Datastore.delete(this)
  def deleteAsync = Datastore.deleteAsync(this)
  def encode = Datastore.keyToString(this)
  def builder = Datastore.keyBuilder(this)

  override def toString = key.toString
  override def compare(that: Key[T]) = key compareTo that.key
}

case class KeyRange[T <: Mapper[T]: ClassManifest](range: GAEKeyRange) extends Iterable[Key[T]] {
  override def iterator = range.iterator.asScala.map(Key(_))
}
