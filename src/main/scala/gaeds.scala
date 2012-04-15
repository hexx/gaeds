package com.github.hexx.gaeds

import java.lang.reflect.Method
import java.util.Date
import scala.collection.JavaConverters._
import scala.reflect.Manifest
import com.google.appengine.api.blobstore.BlobKey
import com.google.appengine.api.datastore._
import com.google.appengine.api.datastore.Query.FilterOperator
import com.google.appengine.api.datastore.Query.SortDirection
import com.google.appengine.api.users.User

object Datastore {
  val service = DatastoreServiceFactory.getDatastoreService

  def put[T <: Mapper[T]](mapper: T): Key = {
    val key = service.put(mapper.toEntity)
    mapper.key = Option(key)
    key
  }
  def put[T <: Mapper[T]](mappers: T*): Seq[Key] = {
    val keys = service.put(mappers.map(_.toEntity).asJava).asScala
    for ((mapper, key) <- mappers zip keys) {
      mapper.key = Option(key)
    }
    keys
  }

  def get[T <: Mapper[T]: ClassManifest](key: Key): T = createMapper(Datastore.service get key)
  def get[T <: Mapper[T]: ClassManifest](keys: Key*): Seq[T] =
    for ((_, entity) <- Datastore.service.get(keys.asJava).asScala.toSeq) yield createMapper(entity)

  def delete(keys: Key*) = service.delete(keys:_*)
  def query[T <: Mapper[T]: ClassManifest](mapper: T) = new TypeSafeQuery(mapper, None)
  def query[T <: Mapper[T]: ClassManifest](mapper: T, ancestorKey: Key) = new TypeSafeQuery(mapper, Option(ancestorKey))

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
}

class FilterPredicate[T](val property: Property[T], val operator: FilterOperator, val value: T)
class SortPredicate(val property: Property[_], val direction: SortDirection)

class TypeSafeQuery[T <: Mapper[T]: ClassManifest](
    mapper: T,
    ancestorKey: Option[Key],
    filterPredicate: List[FilterPredicate[_]] = List(),
    sortPredicate: List[SortPredicate] = List()) {
  def addFilter(f: T => FilterPredicate[_]) = new TypeSafeQuery(mapper, ancestorKey, f(mapper) :: filterPredicate, sortPredicate)
  def addSort(f: T => SortPredicate) = new TypeSafeQuery(mapper, ancestorKey, filterPredicate, f(mapper) :: sortPredicate)

  def prepare(): Iterator[T] = prepare(FetchOptions.Builder.withDefaults)
  def prepare(fetchOptions: FetchOptions): Iterator[T] = {
    val pquery = Datastore.service.prepare(toQuery)
    pquery.asIterator(fetchOptions).asScala.map(mapper.fromEntity(_))
  }

  def toQuery = {
    val query = ancestorKey match {
      case Some(k) => new Query(mapper.kind, k)
      case None => new Query(mapper.kind)
    }
    for (p <- filterPredicate) {
      query.addFilter(p.property.name, p.operator, p.value)
    }
    for (p <- sortPredicate) {
      query.addSort(p.property.name, p.direction)
    }
    query
  }
}

abstract class Mapper[T <: Mapper[T]: ClassManifest] {
  self: T =>

  assignPropertyName()

  var key: Option[Key] = None
  var parentKey: Option[Key] = None

  def kind = implicitly[ClassManifest[T]].erasure.getName // override to customize

  def put() = Datastore.put(this)
  def get(key: Key): T = Datastore.get(key)
  def get(key: Key*): Seq[T] = Datastore.get(key:_*)
  def query = Datastore.query(this)
  def query(ancestorKey: Key) = Datastore.query(this, ancestorKey)
  def fromEntity(entity: Entity): T = Datastore.createMapper(entity)
  def properties: Seq[Property[_]] = zipPropertyAndMethod.map(_._1)
  def copy: T = fromEntity(toEntity)

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

case class Property[T: ClassManifest](var value: T) {
  var name: String = _
  def <(v: T) = new FilterPredicate(this, FilterOperator.LESS_THAN, v)
  def <=(v: T) = new FilterPredicate(this, FilterOperator.LESS_THAN_OR_EQUAL, v)
  def ===(v: T) = new FilterPredicate(this, FilterOperator.EQUAL, v)
  def !==(v: T) = new FilterPredicate(this, FilterOperator.NOT_EQUAL, v)
  def >(v: T) = new FilterPredicate(this, FilterOperator.GREATER_THAN, v)
  def >=(v: T) = new FilterPredicate(this, FilterOperator.GREATER_THAN_OR_EQUAL, v)
  def in(v: T) = new FilterPredicate(this, FilterOperator.IN, v)
  def asc = new SortPredicate(this, SortDirection.ASCENDING)
  def desc = new SortPredicate(this, SortDirection.DESCENDING)
  override def toString = value.toString
}
