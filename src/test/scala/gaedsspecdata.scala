import java.util.{ Date, GregorianCalendar }

import com.google.appengine.api.blobstore.BlobKey
import com.google.appengine.api.datastore.{ Key => GAEKey, _ }
import com.google.appengine.api.users.User

import com.github.hexx.gaeds._
import com.github.hexx.gaeds.Property._

object Util {
  def stringToByteArray(s: String) = s.toArray.map(_.toByte)
  def createShortBlob(s: String) = new ShortBlob(stringToByteArray(s))
  def createBlob(s: String) = new Blob(stringToByteArray(s))
  def date2012 = (new GregorianCalendar(2012, 0, 1)).getTime
}

case class Person(name: String, age: Long)

class Person2(val name: Property[String], val age: Property[Long]) extends Mapper[Person2] {
  def this() = this(mock, mock)
}
object Person2 extends Person2

class Data(
    var boolean: Property[Boolean],
    var shortBlob: Property[ShortBlob],
    var blob: Property[Blob],
    var category: Property[Category],
    var date: Property[Date],
    var email: Property[Email],
    var double: Property[Double],
    var geoPt: Property[GeoPt],
    var user: Property[User],
    var long: Property[Long],
    var blobKey: Property[BlobKey],
    var keyValue: Property[Key[Data]],
    var link: Property[Link],
    var imHandle: Property[IMHandle],
    var postalAddress: Property[PostalAddress],
    var rating: Property[Rating],
    var phoneNumber: Property[PhoneNumber],
    var string: Property[String],
    var text: Property[Text],
    var person: Property[Person])
  extends Mapper[Data] {
  def this() =
    this(mock, mock, mock, mock, mock, mock, mock, mock, mock, mock, mock, mock, mock, mock, mock, mock, mock, mock, mock, mock)
  override def toString() = {
    boolean.toString +
    shortBlob.toString +
    blob.toString +
    category.toString +
    date.toString +
    email.toString +
    double.toString +
    geoPt.toString +
    user.toString +
    long.toString +
    blobKey.toString +
    keyValue.toString +
    link.toString +
    imHandle.toString +
    postalAddress.toString +
    rating.toString +
    phoneNumber.toString +
    string.toString +
    text.toString +
    person.toString
  }
}

object Data extends Data

class UnindexedData(
    var boolean: UnindexedProperty[Boolean],
    var shortBlob: UnindexedProperty[ShortBlob],
    var blob: UnindexedProperty[Blob],
    var category: UnindexedProperty[Category],
    var date: UnindexedProperty[Date],
    var email: UnindexedProperty[Email],
    var double: UnindexedProperty[Double],
    var geoPt: UnindexedProperty[GeoPt],
    var user: UnindexedProperty[User],
    var long: UnindexedProperty[Long],
    var blobKey: UnindexedProperty[BlobKey],
    var keyValue: UnindexedProperty[Key[UnindexedData]],
    var link: UnindexedProperty[Link],
    var imHandle: UnindexedProperty[IMHandle],
    var postalAddress: UnindexedProperty[PostalAddress],
    var rating: UnindexedProperty[Rating],
    var phoneNumber: UnindexedProperty[PhoneNumber],
    var string: UnindexedProperty[String],
    var text: UnindexedProperty[Text],
    var person: UnindexedProperty[Person])
  extends Mapper[UnindexedData] {
  def this() =
    this(unindexedMock, unindexedMock, unindexedMock, unindexedMock, unindexedMock, unindexedMock, unindexedMock, unindexedMock, unindexedMock, unindexedMock, unindexedMock, unindexedMock, unindexedMock, unindexedMock, unindexedMock, unindexedMock, unindexedMock, unindexedMock, unindexedMock, unindexedMock)
  override def toString() = {
    boolean.toString +
    shortBlob.toString +
    blob.toString +
    category.toString +
    date.toString +
    email.toString +
    double.toString +
    geoPt.toString +
    user.toString +
    long.toString +
    blobKey.toString +
    keyValue.toString +
    link.toString +
    imHandle.toString +
    postalAddress.toString +
    rating.toString +
    phoneNumber.toString +
    string.toString +
    text.toString +
    person.toString
  }
}

object UnindexedData extends UnindexedData

class SeqData(
    var boolean: Property[Seq[Boolean]],
    var shortBlob: Property[Seq[ShortBlob]],
    var blob: Property[Seq[Blob]],
    var category: Property[Seq[Category]],
    var date: Property[Seq[Date]],
    var email: Property[Seq[Email]],
    var double: Property[Seq[Double]],
    var geoPt: Property[Seq[GeoPt]],
    var user: Property[Seq[User]],
    var long: Property[Seq[Long]],
    var blobKey: Property[Seq[BlobKey]],
    var keyValue: Property[Seq[Key[SeqData]]],
    var link: Property[Seq[Link]],
    var imHandle: Property[Seq[IMHandle]],
    var postalAddress: Property[Seq[PostalAddress]],
    var rating: Property[Seq[Rating]],
    var phoneNumber: Property[Seq[PhoneNumber]],
    var string: Property[Seq[String]],
    var text: Property[Seq[Text]],
    var person: Property[Seq[Person]])
  extends Mapper[SeqData] {
  def this() =
    this(mock, mock, mock, mock, mock, mock, mock, mock, mock, mock, mock, mock, mock, mock, mock, mock, mock, mock, mock, mock)
  override def toString() = {
    boolean.mkString +
    shortBlob.mkString +
    blob.mkString +
    category.mkString +
    date.mkString +
    email.mkString +
    double.mkString +
    geoPt.mkString +
    user.mkString +
    long.mkString +
    blobKey.mkString +
    keyValue.mkString +
    link.mkString +
    imHandle.mkString +
    postalAddress.mkString +
    rating.mkString +
    phoneNumber.mkString +
    string.mkString +
    text.mkString +
    person.mkString
  }
}

object SeqData extends SeqData

class OptionData(
    var boolean: Property[Option[Boolean]],
    var shortBlob: Property[Option[ShortBlob]],
    var blob: Property[Option[Blob]],
    var category: Property[Option[Category]],
    var date: Property[Option[Date]],
    var email: Property[Option[Email]],
    var double: Property[Option[Double]],
    var geoPt: Property[Option[GeoPt]],
    var user: Property[Option[User]],
    var long: Property[Option[Long]],
    var blobKey: Property[Option[BlobKey]],
    var keyValue: Property[Option[Key[OptionData]]],
    var link: Property[Option[Link]],
    var imHandle: Property[Option[IMHandle]],
    var postalAddress: Property[Option[PostalAddress]],
    var rating: Property[Option[Rating]],
    var phoneNumber: Property[Option[PhoneNumber]],
    var string: Property[Option[String]],
    var text: Property[Option[Text]],
    var person: Property[Option[Person]])
  extends Mapper[OptionData] {
  def this() =
    this(mock, mock, mock, mock, mock, mock, mock, mock, mock, mock, mock, mock, mock, mock, mock, mock, mock, mock, mock, mock)
  override def toString() = {
    boolean.toString +
    shortBlob.toString +
    blob.toString +
    category.toString +
    date.toString +
    email.toString +
    double.toString +
    geoPt.toString +
    user.toString +
    long.toString +
    blobKey.toString +
    keyValue.toString +
    link.toString +
    imHandle.toString +
    postalAddress.toString +
    rating.toString +
    phoneNumber.toString +
    string.toString +
    text.toString +
    person.toString
  }
}

object OptionData extends OptionData

class KeyTestData(
    val dataKey: Property[Key[Data]],
    val dataKeys: Property[Seq[Key[Data]]],
    val dataKeyOption: Property[Option[Key[Data]]])
  extends Mapper[KeyTestData] {
  def this() = this(mock, mock, mock)
}

object KeyTestData extends KeyTestData

object SampleData {
  def data =
    new Data(
      true,
      Util.createShortBlob("shortBlob"),
      Util.createBlob("blob"),
      new Category("category"),
      Util.date2012,
      new Email("email"),
      1.23,
      new GeoPt(1.23F, 1.23F),
      new User("test@gmail.com", "gmail.com"),
      123L,
      new BlobKey("blobKey"),
      Data.createKey(2L),
      new Link("http://www.google.com/"),
      new IMHandle(IMHandle.Scheme.sip, "imHandle"),
      new PostalAddress("postalAddress"),
      new Rating(1),
      new PhoneNumber("0"),
      "string",
      new Text("text"),
      Person("John", 15))

  def unindexedData =
    new UnindexedData(
      true,
      Util.createShortBlob("shortBlob"),
      Util.createBlob("blob"),
      new Category("category"),
      Util.date2012,
      new Email("email"),
      1.23,
      new GeoPt(1.23F, 1.23F),
      new User("test@gmail.com", "gmail.com"),
      123L,
      new BlobKey("blobKey"),
      UnindexedData.createKey(2L),
      new Link("http://www.google.com/"),
      new IMHandle(IMHandle.Scheme.sip, "imHandle"),
      new PostalAddress("postalAddress"),
      new Rating(1),
      new PhoneNumber("0"),
      "string",
      new Text("text"),
      Person("John", 15))

  def seqData =
    new SeqData(
      Seq(true, false),
      Seq(Util.createShortBlob("shortBlob1"), Util.createShortBlob("shortBlob2")),
      Seq(Util.createBlob("blob1"), Util.createBlob("blob2")),
      Seq(new Category("category1"), new Category("category2")),
      Seq(Util.date2012, Util.date2012),
      Seq(new Email("email1"), new Email("email2")),
      Seq(1.23D, 4.56D),
      Seq(new GeoPt(1.23F, 1.23F), new GeoPt(4.56F, 4.56F)),
      Seq(new User("test@gmail.com", "gmail.com"), new User("test@yahoo.com", "yahoo.com")),
      Seq(123L, 456L),
      Seq(new BlobKey("blobKey1"), new BlobKey("blobKey2")),
      Seq(SeqData.createKey(2L), SeqData.createKey(3L)),
      Seq(new Link("http://www.google.com/"), new Link("http://www.yahoo.com/")),
      Seq(new IMHandle(IMHandle.Scheme.sip, "imHandle1"), new IMHandle(IMHandle.Scheme.sip, "imHandle2")),
      Seq(new PostalAddress("postalAddress1"), new PostalAddress("postalAddress2")),
      Seq(new Rating(1), new Rating(2)),
      Seq(new PhoneNumber("1"), new PhoneNumber("2")),
      Seq("string1", "string2"),
      Seq(new Text("text1"), new Text("text2")),
      Seq(Person("John", 15), Person("Mike", 10)))

  def emptySeqData =
    new SeqData(
      Seq(),
      Seq(),
      Seq(),
      Seq(),
      Seq(),
      Seq(),
      Seq(),
      Seq(),
      Seq(),
      Seq(),
      Seq(),
      Seq(),
      Seq(),
      Seq(),
      Seq(),
      Seq(),
      Seq(),
      Seq(),
      Seq(),
      Seq())

  def optionData =
    new OptionData(
      Option(true),
      Option(Util.createShortBlob("shortBlob")),
      Option(Util.createBlob("blob")),
      Option(new Category("category")),
      Option(Util.date2012),
      Option(new Email("email")),
      Option(1.23D),
      Option(new GeoPt(1.23F, 1.23F)),
      Option(new User("test@gmail.com", "gmail.com")),
      Option(123L),
      Option(new BlobKey("blobKey1")),
      Option(OptionData.createKey(2L)),
      Option(new Link("http://www.google.com/")),
      Option(new IMHandle(IMHandle.Scheme.sip, "imHandle1")),
      Option(new PostalAddress("postalAddress1")),
      Option(new Rating(1)),
      Option(new PhoneNumber("1")),
      Option("string1"),
      Option(new Text("text1")),
      Option(Person("John", 15)))

  def noneOptionData =
    new OptionData(
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None)

  def checkUnindexedProperty[T <: Mapper[T]](ds: T) = {
    var entity = ds.toEntity
    entity.isUnindexedProperty("boolean") &&
    entity.isUnindexedProperty("shortBlob") &&
    entity.isUnindexedProperty("blob") &&
    entity.isUnindexedProperty("category") &&
    entity.isUnindexedProperty("date") &&
    entity.isUnindexedProperty("email") &&
    entity.isUnindexedProperty("double") &&
    entity.isUnindexedProperty("geoPt") &&
    entity.isUnindexedProperty("user") &&
    entity.isUnindexedProperty("long") &&
    entity.isUnindexedProperty("blobKey") &&
    entity.isUnindexedProperty("keyValue") &&
    entity.isUnindexedProperty("link") &&
    entity.isUnindexedProperty("imHandle") &&
    entity.isUnindexedProperty("postalAddress") &&
    entity.isUnindexedProperty("rating") &&
    entity.isUnindexedProperty("phoneNumber") &&
    entity.isUnindexedProperty("string") &&
    entity.isUnindexedProperty("text") &&
    entity.isUnindexedProperty("person")
  }

  def printSeqData(ds: SeqData) {
    for (d <- ds.boolean) {
      println(d)
    }
    for (d <- ds.shortBlob) {
      println(d)
    }
    for (p <- ds.blob) {
      println(p)
    }
    for (p <- ds.category) {
      println(p)
    }
    for (p <- ds.date) {
      println(p)
    }
    for (p <- ds.email) {
      println(p)
    }
    for (p <- ds.double) {
      println(p)
    }
    for (p <- ds.geoPt) {
      println(p)
    }
    for (p <- ds.user) {
      println(p)
    }
    for (p <- ds.long) {
      println(p)
    }
    for (p <- ds.blobKey) {
      println(p)
    }
    for (p <- ds.keyValue) {
      println(p)
    }
    for (p <- ds.link) {
      println(p)
    }
    for (p <- ds.imHandle) {
      println(p)
    }
    for (p <- ds.postalAddress) {
      println(p)
    }
    for (p <- ds.rating) {
      println(p)
    }
    for (p <- ds.phoneNumber) {
      println(p)
    }
    for (p <- ds.string) {
      println(p)
    }
    for (p <- ds.text) {
      println(p)
    }
    for (p <- ds.person) {
      println(p)
    }
  }

  def printOptionData(ds: OptionData) {
    for (d <- ds.boolean) {
      println(d)
    }
    for (d <- ds.shortBlob) {
      println(d)
    }
    for (p <- ds.blob) {
      println(p)
    }
    for (p <- ds.category) {
      println(p)
    }
    for (p <- ds.date) {
      println(p)
    }
    for (p <- ds.email) {
      println(p)
    }
    for (p <- ds.double) {
      println(p)
    }
    for (p <- ds.geoPt) {
      println(p)
    }
    for (p <- ds.user) {
      println(p)
    }
    for (p <- ds.long) {
      println(p)
    }
    for (p <- ds.blobKey) {
      println(p)
    }
    for (p <- ds.keyValue) {
      println(p)
    }
    for (p <- ds.link) {
      println(p)
    }
    for (p <- ds.imHandle) {
      println(p)
    }
    for (p <- ds.postalAddress) {
      println(p)
    }
    for (p <- ds.rating) {
      println(p)
    }
    for (p <- ds.phoneNumber) {
      println(p)
    }
    for (p <- ds.string) {
      println(p)
    }
    for (p <- ds.text) {
      println(p)
    }
    for (p <- ds.person) {
      println(p)
    }
  }

  def printKey(k: Key[_]) {
    println(k)
    println(k.id)
    println(k.kind)
    println(k.name)
    println(k.namespace)
    println(k.isComplete)
  }

  def dataJson = """{"boolean":true,"long":123,"double":1.23,"date":"2012-01-01T00:00:00.000Z","text":"text","string":"string","user":{"authDomain":"gmail.com","email":"test@gmail.com","federatedIdentity":null,"userId":null},"keyValue":"agR0ZXN0cgoLEgREYXRhGAIM","shortBlob":"c2hvcnRCbG9i","blob":"YmxvYg==","category":"category","email":"email","geoPt":{"latitude":1.2300000190734863,"longitude":1.2300000190734863},"blobKey":"blobKey","link":"http://www.google.com/","imHandle":{"address":"imHandle","protocol":"sip"},"postalAddress":"postalAddress","rating":1,"phoneNumber":"0","person":"{\"name\":\"John\",\"age\":15}"}"""
}

class Message(
    val name: Property[String],
    val message: Property[String],
    val date: Property[Date])
  extends Mapper[Message] {
  def this() = this(mock, mock, mock)
  def this(name: String, message: String) = this(name, message, new Date)
}

object Message extends Message
