import java.util.Date

import com.google.appengine.api.blobstore.BlobKey
import com.google.appengine.api.datastore._
import com.google.appengine.api.users.User

import com.github.hexx.gaeds._
import com.github.hexx.gaeds.Property._

object Util {
  def stringToByteArray(s: String) = s.toArray.map(_.toByte)
  def createShortBlob(s: String) = new ShortBlob(stringToByteArray(s))
  def createBlob(s: String) = new Blob(stringToByteArray(s))
}

case class Person(name: String, age: Long)

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
    var keyValue: Property[Key],
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
    this(
      false,
      Util.createShortBlob(""),
      Util.createBlob(""),
      new Category(""),
      new Date,
      new Email(""),
      0D,
      new GeoPt(0F, 0F),
      new User("", ""),
      0L,
      new BlobKey(""),
      KeyFactory.createKey("default", 1L),
      new Link(""),
      new IMHandle(IMHandle.Scheme.unknown, ""),
      new PostalAddress(""),
      new Rating(0),
      new PhoneNumber(""),
      "",
      new Text(""),
      Person("", 0))
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
    var keyValue: UnindexedProperty[Key],
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
    this(
      false,
      Util.createShortBlob(""),
      Util.createBlob(""),
      new Category(""),
      new Date,
      new Email(""),
      0D,
      new GeoPt(0F, 0F),
      new User("", ""),
      0L,
      new BlobKey(""),
      KeyFactory.createKey("default", 1L),
      new Link(""),
      new IMHandle(IMHandle.Scheme.unknown, ""),
      new PostalAddress(""),
      new Rating(0),
      new PhoneNumber(""),
      "",
      new Text(""),
      Person("", 0))
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
    var keyValue: Property[Seq[Key]],
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
    this(
      Seq(false),
      Seq(Util.createShortBlob("")),
      Seq(Util.createBlob("")),
      Seq(new Category("")),
      Seq(new Date),
      Seq(new Email("")),
      Seq(0D),
      Seq(new GeoPt(0F, 0F)),
      Seq(new User("", "")),
      Seq(0L),
      Seq(new BlobKey("")),
      Seq(KeyFactory.createKey("default", 1L)),
      Seq(new Link("")),
      Seq(new IMHandle(IMHandle.Scheme.unknown, "")),
      Seq(new PostalAddress("")),
      Seq(new Rating(0)),
      Seq(new PhoneNumber("")),
      Seq(""),
      Seq(new Text("")),
      Seq(Person("", 0)))
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

class SetData(
    var boolean: Property[Set[Boolean]],
    var shortBlob: Property[Set[ShortBlob]],
    var blob: Property[Set[Blob]],
    var category: Property[Set[Category]],
    var date: Property[Set[Date]],
    var email: Property[Set[Email]],
    var double: Property[Set[Double]],
    var geoPt: Property[Set[GeoPt]],
    var user: Property[Set[User]],
    var long: Property[Set[Long]],
    var blobKey: Property[Set[BlobKey]],
    var keyValue: Property[Set[Key]],
    var link: Property[Set[Link]],
    var imHandle: Property[Set[IMHandle]],
    var postalAddress: Property[Set[PostalAddress]],
    var rating: Property[Set[Rating]],
    var phoneNumber: Property[Set[PhoneNumber]],
    var string: Property[Set[String]],
    var text: Property[Set[Text]],
    var person: Property[Set[Person]])
  extends Mapper[SetData] {
  def this() =
    this(
      Set(false),
      Set(Util.createShortBlob("")),
      Set(Util.createBlob("")),
      Set(new Category("")),
      Set(new Date),
      Set(new Email("")),
      Set(0D),
      Set(new GeoPt(0F, 0F)),
      Set(new User("", "")),
      Set(0L),
      Set(new BlobKey("")),
      Set(KeyFactory.createKey("default", 1L)),
      Set(new Link("")),
      Set(new IMHandle(IMHandle.Scheme.unknown, "")),
      Set(new PostalAddress("")),
      Set(new Rating(0)),
      Set(new PhoneNumber("")),
      Set(""),
      Set(new Text("")),
      Set(Person("", 0)))
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

object SetData extends SetData

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
    var keyValue: Property[Option[Key]],
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
    this(
      Option(false),
      Option(Util.createShortBlob("")),
      Option(Util.createBlob("")),
      Option(new Category("")),
      Option(new Date),
      Option(new Email("")),
      Option(0D),
      Option(new GeoPt(0F, 0F)),
      Option(new User("", "")),
      Option(0L),
      Option(new BlobKey("")),
      Option(KeyFactory.createKey("default", 1L)),
      Option(new Link("")),
      Option(new IMHandle(IMHandle.Scheme.unknown, "")),
      Option(new PostalAddress("")),
      Option(new Rating(0)),
      Option(new PhoneNumber("")),
      Option(""),
      Option(new Text("")),
      Option(Person("", 0)))
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

object SampleData {
  def data =
    new Data(
      true,
      Util.createShortBlob("shortBlob"),
      Util.createBlob("blob"),
      new Category("category"),
      new Date,
      new Email("email"),
      1.23,
      new GeoPt(1.23F, 1.23F),
      new User("test@gmail.com", "gmail.com"),
      123L,
      new BlobKey("blobKey"),
      KeyFactory.createKey("data", 2L),
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
      new Date,
      new Email("email"),
      1.23,
      new GeoPt(1.23F, 1.23F),
      new User("test@gmail.com", "gmail.com"),
      123L,
      new BlobKey("blobKey"),
      KeyFactory.createKey("data", 2L),
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
      Seq(new Date, new Date),
      Seq(new Email("email1"), new Email("email2")),
      Seq(1.23D, 4.56D),
      Seq(new GeoPt(1.23F, 1.23F), new GeoPt(4.56F, 4.56F)),
      Seq(new User("test@gmail.com", "gmail.com"), new User("test@yahoo.com", "yahoo.com")),
      Seq(123L, 456L),
      Seq(new BlobKey("blobKey1"), new BlobKey("blobKey2")),
      Seq(KeyFactory.createKey("data1", 2L), KeyFactory.createKey("data2", 3L)),
      Seq(new Link("http://www.google.com/"), new Link("http://www.yahoo.com/")),
      Seq(new IMHandle(IMHandle.Scheme.sip, "imHandle1"), new IMHandle(IMHandle.Scheme.sip, "imHandle2")),
      Seq(new PostalAddress("postalAddress1"), new PostalAddress("postalAddress2")),
      Seq(new Rating(1), new Rating(2)),
      Seq(new PhoneNumber("1"), new PhoneNumber("2")),
      Seq("string1", "string2"),
      Seq(new Text("text1"), new Text("text2")),
      Seq(Person("John", 15), Person("Mike", 10)))

  def setData =
    new SetData(
      Set(true, false),
      Set(Util.createShortBlob("shortBlob1"), Util.createShortBlob("shortBlob2")),
      Set(Util.createBlob("blob1"), Util.createBlob("blob2")),
      Set(new Category("category1"), new Category("category2")),
      Set(new Date, new Date),
      Set(new Email("email1"), new Email("email2")),
      Set(1.23D, 4.56D),
      Set(new GeoPt(1.23F, 1.23F), new GeoPt(4.56F, 4.56F)),
      Set(new User("test@gmail.com", "gmail.com"), new User("test@yahoo.com", "yahoo.com")),
      Set(123L, 456L),
      Set(new BlobKey("blobKey1"), new BlobKey("blobKey2")),
      Set(KeyFactory.createKey("data1", 2L), KeyFactory.createKey("data2", 3L)),
      Set(new Link("http://www.google.com/"), new Link("http://www.yahoo.com/")),
      Set(new IMHandle(IMHandle.Scheme.sip, "imHandle1"), new IMHandle(IMHandle.Scheme.sip, "imHandle2")),
      Set(new PostalAddress("postalAddress1"), new PostalAddress("postalAddress2")),
      Set(new Rating(1), new Rating(2)),
      Set(new PhoneNumber("1"), new PhoneNumber("2")),
      Set("string1", "string2"),
      Set(new Text("text1"), new Text("text2")),
      Set(Person("John", 15), Person("Mike", 10)))

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

  def emptySetData =
    new SetData(
      Set(): Set[Boolean],
      Set(): Set[ShortBlob],
      Set(): Set[Blob],
      Set(): Set[Category],
      Set(): Set[Date],
      Set(): Set[Email],
      Set(): Set[Double],
      Set(): Set[GeoPt],
      Set(): Set[User],
      Set(): Set[Long],
      Set(): Set[BlobKey],
      Set(): Set[Key],
      Set(): Set[Link],
      Set(): Set[IMHandle],
      Set(): Set[PostalAddress],
      Set(): Set[Rating],
      Set(): Set[PhoneNumber],
      Set(): Set[String],
      Set(): Set[Text],
      Set(): Set[Person])

  def optionData =
    new OptionData(
      Option(true),
      Option(Util.createShortBlob("shortBlob")),
      Option(Util.createBlob("blob")),
      Option(new Category("category")),
      Option(new Date),
      Option(new Email("email")),
      Option(1.23D),
      Option(new GeoPt(1.23F, 1.23F)),
      Option(new User("test@gmail.com", "gmail.com")),
      Option(123L),
      Option(new BlobKey("blobKey1")),
      Option(KeyFactory.createKey("data1", 2L)),
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

  def checkUnindexedProperty(ds: UnindexedData) = {
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

  def printSetData(ds: SetData) {
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
}
