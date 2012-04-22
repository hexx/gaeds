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

class Data(
    val boolean: Property[Boolean],
    val shortBlob: Property[ShortBlob],
    val blob: Property[Blob],
    val category: Property[Category],
    val date: Property[Date],
    val email: Property[Email],
    val double: Property[Double],
    val geoPt: Property[GeoPt],
    val user: Property[User],
    val long: Property[Long],
    val blobKey: Property[BlobKey],
    val keyValue: Property[Key],
    val link: Property[Link],
    val imHandle: Property[IMHandle],
    val postalAddress: Property[PostalAddress],
    val rating: Property[Rating],
    val phoneNumber: Property[PhoneNumber],
    val string: Property[String],
    val text: Property[Text])
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
      new Text(""))
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
    text.toString
  }
}

object Data extends Data

class UnindexedData(
    val boolean: UnindexedProperty[Boolean],
    val shortBlob: UnindexedProperty[ShortBlob],
    val blob: UnindexedProperty[Blob],
    val category: UnindexedProperty[Category],
    val date: UnindexedProperty[Date],
    val email: UnindexedProperty[Email],
    val double: UnindexedProperty[Double],
    val geoPt: UnindexedProperty[GeoPt],
    val user: UnindexedProperty[User],
    val long: UnindexedProperty[Long],
    val blobKey: UnindexedProperty[BlobKey],
    val keyValue: UnindexedProperty[Key],
    val link: UnindexedProperty[Link],
    val imHandle: UnindexedProperty[IMHandle],
    val postalAddress: UnindexedProperty[PostalAddress],
    val rating: UnindexedProperty[Rating],
    val phoneNumber: UnindexedProperty[PhoneNumber],
    val string: UnindexedProperty[String],
    val text: UnindexedProperty[Text])
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
      new Text(""))
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
    text.toString
  }
}

object UnindexedData extends UnindexedData

class SeqData(
    val boolean: Property[Seq[Boolean]],
    val shortBlob: Property[Seq[ShortBlob]],
    val blob: Property[Seq[Blob]],
    val category: Property[Seq[Category]],
    val date: Property[Seq[Date]],
    val email: Property[Seq[Email]],
    val double: Property[Seq[Double]],
    val geoPt: Property[Seq[GeoPt]],
    val user: Property[Seq[User]],
    val long: Property[Seq[Long]],
    val blobKey: Property[Seq[BlobKey]],
    val keyValue: Property[Seq[Key]],
    val link: Property[Seq[Link]],
    val imHandle: Property[Seq[IMHandle]],
    val postalAddress: Property[Seq[PostalAddress]],
    val rating: Property[Seq[Rating]],
    val phoneNumber: Property[Seq[PhoneNumber]],
    val string: Property[Seq[String]],
    val text: Property[Seq[Text]])
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
      Seq(new Text("")))
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
    text.mkString
  }
}

object SeqData extends SeqData

class SetData(
    val boolean: Property[Set[Boolean]],
    val shortBlob: Property[Set[ShortBlob]],
    val blob: Property[Set[Blob]],
    val category: Property[Set[Category]],
    val date: Property[Set[Date]],
    val email: Property[Set[Email]],
    val double: Property[Set[Double]],
    val geoPt: Property[Set[GeoPt]],
    val user: Property[Set[User]],
    val long: Property[Set[Long]],
    val blobKey: Property[Set[BlobKey]],
    val keyValue: Property[Set[Key]],
    val link: Property[Set[Link]],
    val imHandle: Property[Set[IMHandle]],
    val postalAddress: Property[Set[PostalAddress]],
    val rating: Property[Set[Rating]],
    val phoneNumber: Property[Set[PhoneNumber]],
    val string: Property[Set[String]],
    val text: Property[Set[Text]])
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
      Set(new Text("")))
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
    text.mkString
  }
}

object SetData extends SetData

class OptionData(
    val boolean: Property[Option[Boolean]],
    val shortBlob: Property[Option[ShortBlob]],
    val blob: Property[Option[Blob]],
    val category: Property[Option[Category]],
    val date: Property[Option[Date]],
    val email: Property[Option[Email]],
    val double: Property[Option[Double]],
    val geoPt: Property[Option[GeoPt]],
    val user: Property[Option[User]],
    val long: Property[Option[Long]],
    val blobKey: Property[Option[BlobKey]],
    val keyValue: Property[Option[Key]],
    val link: Property[Option[Link]],
    val imHandle: Property[Option[IMHandle]],
    val postalAddress: Property[Option[PostalAddress]],
    val rating: Property[Option[Rating]],
    val phoneNumber: Property[Option[PhoneNumber]],
    val string: Property[Option[String]],
    val text: Property[Option[Text]])
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
      Option(new Text("")))
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
    text.toString
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
      new Text("text"))

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
      new Text("text"))

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
      Seq(new Text("text1"), new Text("text2")))

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
      Set(new Text("text1"), new Text("text2")))

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
      Set(): Set[Text])

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
      Option(new Text("text1")))

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
      None)

  def checkUnindexedProperty(ds: UnindexedData) = {
    val entity = ds.toEntity
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
    entity.isUnindexedProperty("text")
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
  }
}
