package com.github.hexx.gaeds

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream }
import java.util.Date
import scala.collection.JavaConverters._
import com.google.appengine.api.blobstore.BlobKey
import com.google.appengine.api.datastore.{ Blob, Category, Email, Entity, GeoPt, IMHandle }
import com.google.appengine.api.datastore.{ Link, PhoneNumber, PostalAddress, Rating, ShortBlob, Text }
import com.google.appengine.api.datastore.{ Key => LLKey }
import com.google.appengine.api.datastore.Query.{ FilterOperator, SortDirection }
import com.google.appengine.api.users.User
import net.liftweb.json._
import net.liftweb.json.JsonDSL._
import org.apache.commons.codec.binary.Base64

class BaseProperty[T: Manifest](var __valueOfProperty: T) {
  val __manifest = implicitly[Manifest[T]]
  var __nameOfProperty: String = _
  def __isOption = classOf[Option[_]].isAssignableFrom(__valueClass)
  def __isSeq = classOf[Seq[_]].isAssignableFrom(__valueClass)
  def __isDate = classOf[Date].isAssignableFrom(__valueClass)
  def __isCategory = classOf[Category].isAssignableFrom(__valueClass)
  def __isShortBlob = classOf[ShortBlob].isAssignableFrom(__valueClass)
  def __isBlob = classOf[Blob].isAssignableFrom(__valueClass)
  def __isEmail = classOf[Email].isAssignableFrom(__valueClass)
  def __isGeoPt = classOf[GeoPt].isAssignableFrom(__valueClass)
  def __isUser = classOf[User].isAssignableFrom(__valueClass)
  def __isBlobKey = classOf[BlobKey].isAssignableFrom(__valueClass)
  def __isKey = classOf[Key[_]].isAssignableFrom(__valueClass)
  def __isLink = classOf[Link].isAssignableFrom(__valueClass)
  def __isIMHandle = classOf[IMHandle].isAssignableFrom(__valueClass)
  def __isPostalAddress = classOf[PostalAddress].isAssignableFrom(__valueClass)
  def __isRating = classOf[Rating].isAssignableFrom(__valueClass)
  def __isPhoneNumber = classOf[PhoneNumber].isAssignableFrom(__valueClass)
  def __isText = classOf[Text].isAssignableFrom(__valueClass)
  def __isSerializable = classOf[Serializable].isAssignableFrom(__valueClass)
  def __isContentSerializable = classOf[Serializable].isAssignableFrom(__contentClass)
  def __isContentKey = classOf[Key[_]].isAssignableFrom(__contentClass)
  def __isUnindexed = false
  def __setToEntity(entity: Entity) = entity.setProperty(__nameOfProperty, __javaValueOfProperty)
  def __contentManifest = __manifest.typeArguments(0)
  def __contentMapperManifest = __contentManifest.asInstanceOf[Manifest[T] forSome { type T <: Mapper[T] }]
  def __contentContentMapperManifest = __contentManifest.typeArguments(0).asInstanceOf[Manifest[T] forSome { type T <: Mapper[T] }]

  def __javaValueOfProperty = __valueOfProperty match {
    case l: Seq[_] =>
      val l2 = if (__isContentKey) {
        l.asInstanceOf[Seq[Key[_]]].map(_.key)
      } else if (__isContentSerializable) {
        l.asInstanceOf[Seq[Serializable]].map(dumpToBlob)
      } else {
        l
      }
      l2.asJava
    case o: Option[_] => o match {
      case Some(v) =>
        if (__isContentKey) {
          v.asInstanceOf[Key[_]].key
        } else if (__isContentSerializable) {
          dumpToBlob(v.asInstanceOf[Serializable])
        } else {
          v
        }
      case None => null
    }
    case k: Key[_] => k.key
    case m: Mapper[_] => m.key.get.key
    case s: Serializable => dumpToBlob(s)
    case _ => __valueOfProperty
  }

  def __jvalueOfProperty(implicit formats: Formats = DefaultFormats) = valueToJValue(__valueOfProperty)(formats)

  def __jfieldOfProperty(implicit formats: Formats = DefaultFormats) = propertyToJField(this)(formats)

  def __llvalueToScalaValue(value: Any) = {
    def loadSerializable(b: Blob) = {
      val in = new ObjectInputStream(new ByteArrayInputStream(b.getBytes))
      val s = in.readObject.asInstanceOf[Serializable]
      in.close()
      s
    }

    value match {
      case l: java.util.ArrayList[_] => {
        val l2 = l.asScala
        if (__isContentKey) {
          l2.asInstanceOf[Seq[LLKey]].map(Key(_)(__contentContentMapperManifest))
        } else if (__isContentSerializable) {
          l2.asInstanceOf[Seq[Blob]].map(loadSerializable)
        } else {
          l2
        }
      }
      case null if __isSeq => Seq()
      case k: LLKey if k != null && !__isOption => Key(k)(__contentMapperManifest)
      case b: Blob if __isSerializable && !__isOption => loadSerializable(b)
      case _ if __isOption => {
        val o = Option(value)
        if (__isContentKey) {
          o.asInstanceOf[Option[LLKey]].map(Key(_)(__contentContentMapperManifest))
        } else if (__isContentSerializable) {
          o.asInstanceOf[Option[Blob]].map(loadSerializable)
        } else {
          o
        }
      }
      case _ => value
    }
  }

  def __jvalueToScalaValue(value: JValue)(implicit formats: Formats = DefaultFormats): Any = {
    def jobjectToGeoPt(jobject: JObject): GeoPt = {
      val map = jobject.values
      new GeoPt(
        map("latitude").asInstanceOf[Double].asInstanceOf[Float],
        map("longitude").asInstanceOf[Double].asInstanceOf[Float]
      )
    }

    def jobjectToUser(jobject: JObject): User = {
      val map = jobject.values
      (map("email").asInstanceOf[String],
       map("authDomain").asInstanceOf[String],
       map("userId").asInstanceOf[String],
       map("federatedIdentity").asInstanceOf[String]) match {
         case (e, a, null, null) => new User(e, a)
         case (e, a, u,    null) => new User(e, a, u)
         case (e, a, u,    i   ) => new User(e, a, u, i)
       }
    }

    def jobjectToIMHandle(jobject: JObject) = {
      val map = jobject.values
      new IMHandle(
        IMHandle.Scheme.valueOf(map("protocol").asInstanceOf[String]),
        map("address").asInstanceOf[String]
      )
    }

    value match {
      case JBool(b)                        => b
      case JString(s) if __isDate          => DefaultFormats.lossless.dateFormat.parse(s).get
      case JString(s) if __isCategory      => new Category(s)
      case JString(s) if __isEmail         => new Email(s)
      case JString(s) if __isBlobKey       => new BlobKey(s)
      case JString(s) if __isKey           => Key.fromWebSafeString(s)(__contentMapperManifest)
      case JString(s) if __isLink          => new Link(s)
      case JString(s) if __isPostalAddress => new PostalAddress(s)
      case JString(s) if __isPhoneNumber   => new PhoneNumber(s)
      case JString(s) if __isText          => new Text(s)
      case JString(s) if __isShortBlob     => new ShortBlob(Base64.decodeBase64(s))
      case JString(s) if __isBlob          => new Blob(Base64.decodeBase64(s))
      case JString(s) if __isSerializable  => Serialization.read(s)(formats, __manifest)
      case JString(s)                      => s
      case JInt(i) if (__isRating)         => new Rating(i.intValue)
      case JInt(i)                         => i.longValue
      case JArray(l)                       => l.map(__jvalueToScalaValue(_))
      case JDouble(d)                      => d
      case JNull if __isSeq                => Seq()
      case JNull if __isOption             => None
      case o: JObject if (__isGeoPt)       => jobjectToGeoPt(o)
      case o: JObject if (__isUser)        => jobjectToUser(o)
      case o: JObject if (__isIMHandle)    => jobjectToIMHandle(o)
      case _ => println(value)
    }
  }

  override def toString = if (__valueOfProperty == null) "null" else __valueOfProperty.toString

  private def __valueClass = __manifest.erasure
  private def __contentClass = __contentManifest.erasure

  private def dumpToBlob(s: Serializable) = {
    val ba = new ByteArrayOutputStream
    val out = new ObjectOutputStream(ba)
    out.writeObject(s)
    out.close()
    new Blob(ba.toByteArray)
  }

  private def propertyToJField(p: BaseProperty[_])(implicit formats: Formats) = JField(p.__nameOfProperty, valueToJValue(p.__valueOfProperty))

  private def valueToJValue(value: Any)(implicit formats: Formats): JValue = {
    def getptToJValue(g: GeoPt) =
      ("latitude" -> g.getLatitude) ~
      ("longitude" -> g.getLongitude)

    def userToJValue(u: User) =
      ("authDomain" -> u.getAuthDomain) ~
      ("email" -> u.getEmail) ~
      ("federatedIdentity" -> u.getFederatedIdentity) ~
      ("userId" -> u.getUserId)

    def imhandleToJValue(h: IMHandle) =
      ("address" -> h.getAddress) ~
      ("protocol" -> h.getProtocol)

    value match {
      case b: Boolean       => JBool(b)
      case b: ShortBlob     => JString(Base64.encodeBase64String(b.getBytes))
      case b: Blob          => JString(Base64.encodeBase64String(b.getBytes))
      case c: Category      => JString(c.getCategory)
      case d: Date          => JString(DefaultFormats.lossless.dateFormat.format(d))
      case m: Email         => JString(m.getEmail)
      case g: GeoPt         => getptToJValue(g)
      case u: User          => userToJValue(u)
      case l: Long          => JInt(l)
      case d: Double        => JDouble(d)
      case b: BlobKey       => JString(b.getKeyString)
      case k: Key[_]        => JString(k.toWebSafeString)
      case l: Link          => JString(l.getValue)
      case h: IMHandle      => imhandleToJValue(h)
      case a: PostalAddress => JString(a.getAddress)
      case r: Rating        => JInt(r.getRating)
      case n: PhoneNumber   => JString(n.getNumber)
      case s: String        => JString(s)
      case t: Text          => JString(t.getValue)
      case l: Seq[_]        => JArray(l.toList map valueToJValue)
      case Some(o)          => valueToJValue(o)
      case None             => JNull
      case s: Serializable  => Serialization.write(s)
    }
  }
}

case class Property[T: Manifest](__valueOfPropertyArg: T) extends BaseProperty[T](__valueOfPropertyArg)

case class UnindexedProperty[T: Manifest](__valueOfPropertyArg: T) extends BaseProperty[T](__valueOfPropertyArg) {
  override def __isUnindexed = true
  override def __setToEntity(entity: Entity) = entity.setUnindexedProperty(__nameOfProperty, __javaValueOfProperty)
}

case class SortPredicate(property: BaseProperty[_], direction: SortDirection)

case class PropertyOperator[T: ClassManifest](property: BaseProperty[T]) {
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
  def mock[T: Manifest] = Property(null.asInstanceOf[T])
  def unindexedMock[T: Manifest] = UnindexedProperty(null.asInstanceOf[T])

  implicit def propertyToValue[T](property: BaseProperty[T]): T = property.__valueOfProperty

  implicit def shortBlobValueToProperty(value: ShortBlob) = Property(value)
  implicit def blobValueToProperty(value: Blob) = Property(value)
  implicit def categoryValueToProperty(value: Category) = Property(value)
  implicit def booleanValueToProperty(value: Boolean) = Property(value)
  implicit def dateValueToProperty(value: Date) = Property(value)
  implicit def emailValueToProperty(value: Email) = Property(value)
  implicit def doubleValueToProperty(value: Double) = Property(value)
  implicit def geoPtValueToProperty(value: GeoPt) = Property(value)
  implicit def userValueToProperty(value: User) = Property(value)
  implicit def longValueToProperty(value: Long) = Property(value)
  implicit def blobKeyValueToProperty(value: BlobKey) = Property(value)
  implicit def keyValueToProperty[T <: Mapper[T]: Manifest](value: Key[T]) = Property(value)
  implicit def linkValueToProperty(value: Link) = Property(value)
  implicit def imHandleValueToProperty(value: IMHandle) = Property(value)
  implicit def postalAddressValueToProperty(value: PostalAddress) = Property(value)
  implicit def ratingValueToProperty(value: Rating) = Property(value)
  implicit def phoneNumberValueToProperty(value: PhoneNumber) = Property(value)
  implicit def stringValueToProperty(value: String) = Property(value)
  implicit def textValueToProperty(value: Text) = Property(value)
  implicit def serializableValueToProperty[T <: Serializable: Manifest](value: T) = Property(value)

  implicit def shortBlobSeqValueToProperty(value: Seq[ShortBlob]) = Property(value)
  implicit def blobSeqValueToProperty(value: Seq[Blob]) = Property(value)
  implicit def categorySeqValueToProperty(value: Seq[Category]) = Property(value)
  implicit def booleanSeqValueToProperty(value: Seq[Boolean]) = Property(value)
  implicit def dateSeqValueToProperty(value: Seq[Date]) = Property(value)
  implicit def emailSeqValueToProperty(value: Seq[Email]) = Property(value)
  implicit def doubleSeqValueToProperty(value: Seq[Double]) = Property(value)
  implicit def geoPtSeqValueToProperty(value: Seq[GeoPt]) = Property(value)
  implicit def userSeqValueToProperty(value: Seq[User]) = Property(value)
  implicit def longSeqValueToProperty(value: Seq[Long]) = Property(value)
  implicit def blobKeySeqValueToProperty(value: Seq[BlobKey]) = Property(value)
  implicit def keySeqValueToProperty[T <: Mapper[T]: Manifest](value: Seq[Key[T]]) = Property(value)
  implicit def linkSeqValueToProperty(value: Seq[Link]) = Property(value)
  implicit def imHandleSeqValueToProperty(value: Seq[IMHandle]) = Property(value)
  implicit def postalAddressSeqValueToProperty(value: Seq[PostalAddress]) = Property(value)
  implicit def ratingSeqValueToProperty(value: Seq[Rating]) = Property(value)
  implicit def phoneNumberSeqValueToProperty(value: Seq[PhoneNumber]) = Property(value)
  implicit def stringSeqValueToProperty(value: Seq[String]) = Property(value)
  implicit def textSeqValueToProperty(value: Seq[Text]) = Property(value)
  implicit def serializableSeqValueToProperty[T <: Serializable: Manifest](value: Seq[T]) = Property(value)

  implicit def shortBlobOptionValueToProperty(value: Option[ShortBlob]) = Property(value)
  implicit def blobOptionValueToProperty(value: Option[Blob]) = Property(value)
  implicit def categoryOptionValueToProperty(value: Option[Category]) = Property(value)
  implicit def booleanOptionValueToProperty(value: Option[Boolean]) = Property(value)
  implicit def dateOptionValueToProperty(value: Option[Date]) = Property(value)
  implicit def emailOptionValueToProperty(value: Option[Email]) = Property(value)
  implicit def doubleOptionValueToProperty(value: Option[Double]) = Property(value)
  implicit def geoPtOptionValueToProperty(value: Option[GeoPt]) = Property(value)
  implicit def userOptionValueToProperty(value: Option[User]) = Property(value)
  implicit def longOptionValueToProperty(value: Option[Long]) = Property(value)
  implicit def blobKeyOptionValueToProperty(value: Option[BlobKey]) = Property(value)
  implicit def keyOptionValueToProperty[T <: Mapper[T]: Manifest](value: Option[Key[T]]) = Property(value)
  implicit def linkOptionValueToProperty(value: Option[Link]) = Property(value)
  implicit def imHandleOptionValueToProperty(value: Option[IMHandle]) = Property(value)
  implicit def postalAddressOptionValueToProperty(value: Option[PostalAddress]) = Property(value)
  implicit def ratingOptionValueToProperty(value: Option[Rating]) = Property(value)
  implicit def phoneNumberOptionValueToProperty(value: Option[PhoneNumber]) = Property(value)
  implicit def stringOptionValueToProperty(value: Option[String]) = Property(value)
  implicit def textOptionValueToProperty(value: Option[Text]) = Property(value)
  implicit def serializableOptionValueToProperty[T <: Serializable: Manifest](value: Option[T]) = Property(value)

  implicit def shortBlobValueToUnindexedProperty(value: ShortBlob) = UnindexedProperty(value)
  implicit def blobValueToUnindexedProperty(value: Blob) = UnindexedProperty(value)
  implicit def categoryValueToUnindexedProperty(value: Category) = UnindexedProperty(value)
  implicit def booleanValueToUnindexedProperty(value: Boolean) = UnindexedProperty(value)
  implicit def dateValueToUnindexedProperty(value: Date) = UnindexedProperty(value)
  implicit def emailValueToUnindexedProperty(value: Email) = UnindexedProperty(value)
  implicit def doubleValueToUnindexedProperty(value: Double) = UnindexedProperty(value)
  implicit def geoPtValueToUnindexedProperty(value: GeoPt) = UnindexedProperty(value)
  implicit def userValueToUnindexedProperty(value: User) = UnindexedProperty(value)
  implicit def longValueToUnindexedProperty(value: Long) = UnindexedProperty(value)
  implicit def blobKeyValueToUnindexedProperty(value: BlobKey) = UnindexedProperty(value)
  implicit def keyValueToUnindexedProperty[T <: Mapper[T]: Manifest](value: Key[T]) = UnindexedProperty(value)
  implicit def linkValueToUnindexedProperty(value: Link) = UnindexedProperty(value)
  implicit def imHandleValueToUnindexedProperty(value: IMHandle) = UnindexedProperty(value)
  implicit def postalAddressValueToUnindexedProperty(value: PostalAddress) = UnindexedProperty(value)
  implicit def ratingValueToUnindexedProperty(value: Rating) = UnindexedProperty(value)
  implicit def phoneNumberValueToUnindexedProperty(value: PhoneNumber) = UnindexedProperty(value)
  implicit def stringValueToUnindexedProperty(value: String) = UnindexedProperty(value)
  implicit def textValueToUnindexedProperty(value: Text) = UnindexedProperty(value)
  implicit def serializableValueToUnindexedProperty[T <: Serializable: Manifest](value: T) = UnindexedProperty(value)

  implicit def shortBlobSeqValueToUnindexedProperty(value: Seq[ShortBlob]) = UnindexedProperty(value)
  implicit def blobSeqValueToUnindexedProperty(value: Seq[Blob]) = UnindexedProperty(value)
  implicit def categorySeqValueToUnindexedProperty(value: Seq[Category]) = UnindexedProperty(value)
  implicit def booleanSeqValueToUnindexedProperty(value: Seq[Boolean]) = UnindexedProperty(value)
  implicit def dateSeqValueToUnindexedProperty(value: Seq[Date]) = UnindexedProperty(value)
  implicit def emailSeqValueToUnindexedProperty(value: Seq[Email]) = UnindexedProperty(value)
  implicit def doubleSeqValueToUnindexedProperty(value: Seq[Double]) = UnindexedProperty(value)
  implicit def geoPtSeqValueToUnindexedProperty(value: Seq[GeoPt]) = UnindexedProperty(value)
  implicit def userSeqValueToUnindexedProperty(value: Seq[User]) = UnindexedProperty(value)
  implicit def longSeqValueToUnindexedProperty(value: Seq[Long]) = UnindexedProperty(value)
  implicit def blobKeySeqValueToUnindexedProperty(value: Seq[BlobKey]) = UnindexedProperty(value)
  implicit def keySeqValueToUnindexedProperty[T <: Mapper[T]: Manifest](value: Seq[Key[T]]) = UnindexedProperty(value)
  implicit def linkSeqValueToUnindexedProperty(value: Seq[Link]) = UnindexedProperty(value)
  implicit def imHandleSeqValueToUnindexedProperty(value: Seq[IMHandle]) = UnindexedProperty(value)
  implicit def postalAddressSeqValueToUnindexedProperty(value: Seq[PostalAddress]) = UnindexedProperty(value)
  implicit def ratingSeqValueToUnindexedProperty(value: Seq[Rating]) = UnindexedProperty(value)
  implicit def phoneNumberSeqValueToUnindexedProperty(value: Seq[PhoneNumber]) = UnindexedProperty(value)
  implicit def stringSeqValueToUnindexedProperty(value: Seq[String]) = UnindexedProperty(value)
  implicit def textSeqValueToUnindexedProperty(value: Seq[Text]) = UnindexedProperty(value)
  implicit def serializableSeqValueToUnindexedProperty[T <: Serializable: Manifest](value: Seq[T]) = UnindexedProperty(value)

  implicit def shortBlobOptionValueToUnindexedProperty(value: Option[ShortBlob]) = UnindexedProperty(value)
  implicit def blobOptionValueToUnindexedProperty(value: Option[Blob]) = UnindexedProperty(value)
  implicit def categoryOptionValueToUnindexedProperty(value: Option[Category]) = UnindexedProperty(value)
  implicit def booleanOptionValueToUnindexedProperty(value: Option[Boolean]) = UnindexedProperty(value)
  implicit def dateOptionValueToUnindexedProperty(value: Option[Date]) = UnindexedProperty(value)
  implicit def emailOptionValueToUnindexedProperty(value: Option[Email]) = UnindexedProperty(value)
  implicit def doubleOptionValueToUnindexedProperty(value: Option[Double]) = UnindexedProperty(value)
  implicit def geoPtOptionValueToUnindexedProperty(value: Option[GeoPt]) = UnindexedProperty(value)
  implicit def userOptionValueToUnindexedProperty(value: Option[User]) = UnindexedProperty(value)
  implicit def longOptionValueToUnindexedProperty(value: Option[Long]) = UnindexedProperty(value)
  implicit def blobKeyOptionValueToUnindexedProperty(value: Option[BlobKey]) = UnindexedProperty(value)
  implicit def keyOptionValueToUnindexedProperty[T <: Mapper[T]: Manifest](value: Option[Key[T]]) = UnindexedProperty(value)
  implicit def linkOptionValueToUnindexedProperty(value: Option[Link]) = UnindexedProperty(value)
  implicit def imHandleOptionValueToUnindexedProperty(value: Option[IMHandle]) = UnindexedProperty(value)
  implicit def postalAddressOptionValueToUnindexedProperty(value: Option[PostalAddress]) = UnindexedProperty(value)
  implicit def ratingOptionValueToUnindexedProperty(value: Option[Rating]) = UnindexedProperty(value)
  implicit def phoneNumberOptionValueToUnindexedProperty(value: Option[PhoneNumber]) = UnindexedProperty(value)
  implicit def stringOptionValueToUnindexedProperty(value: Option[String]) = UnindexedProperty(value)
  implicit def textOptionValueToUnindexedProperty(value: Option[Text]) = UnindexedProperty(value)
  implicit def serializableOptionValueToUnindexedProperty[T <: Serializable: Manifest](value: Option[T]) = UnindexedProperty(value)

  implicit def propertyToOperator[T: ClassManifest](property: BaseProperty[T]) = PropertyOperator(property)
}
