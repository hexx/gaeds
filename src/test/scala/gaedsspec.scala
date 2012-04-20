import org.scalatest.{ WordSpec, BeforeAndAfter }
import org.scalatest.matchers.MustMatchers

import java.util.Date

import scala.collection.JavaConverters._

import com.google.appengine.api.blobstore.BlobKey
import com.google.appengine.api.datastore._
import com.google.appengine.api.datastore.FetchOptions.Builder._
import com.google.appengine.api.users.User
import com.google.appengine.tools.development.testing.{ LocalDatastoreServiceTestConfig, LocalServiceTestHelper }

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

// low-level sample
import com.google.appengine.api.datastore.{ DatastoreServiceFactory, Entity }
import com.google.appengine.api.datastore.Query
import com.google.appengine.api.datastore.Query.FilterOperator._
import com.google.appengine.api.datastore.Query.SortDirection._

case class Person(name: String, age: Long)

// gaeds sample
import com.github.hexx.gaeds._
import com.github.hexx.gaeds.Property._

class Person2(val name: Property[String], val age: Property[Long]) extends Mapper[Person2] {
  def this() = this("", 0)
  override def toString() = "Person(" + name + "," + age + ")"
}
object Person2 extends Person2

class GAEDSSpec extends WordSpec with BeforeAndAfter with MustMatchers {
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

  val helper = new LocalServiceTestHelper(new LocalDatastoreServiceTestConfig())
  before {
    helper.setUp()
  }
  after {
    helper.tearDown()
  }

  def putTest(k: Key, d: Data) = {
    k.getId must not be 0
    d.key.get must be === k
  }
  def putAndGetTest(k: Key, d1: Data, d2: Data) = {
    putTest(k, d1)
    d1 must be === d2
    d1.key.get must be === d2.key.get
    d1.toString must be === d2.toString
  }

  def seqPutTest(k: Key, d: SeqData) = {
    k.getId must not be 0
    d.key.get must be === k
  }
  def seqPutAndGetTest(k: Key, d1: SeqData, d2: SeqData) = {
    seqPutTest(k, d1)
    d1 must be === d2
    d1.key.get must be === d2.key.get
    d1.toString must be === d2.toString
  }

  def printKey(k: Key) {
    println(k)
    println(k.getId)
    println(k.getKind)
    println(k.getName)
    println(k.getNamespace)
    println(k.isComplete)
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

  "Entity" should {
    "put and get" in {
      val d1 = data
      val k = d1.put
      val d2 = Data.get(k)
      putAndGetTest(k, d1, d2)
      println(d2)
      Datastore.delete(k)
    }
    "multi-put and multi-get" in {
      val ds1 = Seq(data, data, data)
      val ks = Datastore.put(ds1:_*)
      val ds2 = Data.get(ks:_*)
      for (((k, d1), d2) <- ks zip ds1 zip ds2) {
        putAndGetTest(k, d1, d2)
        println(d2)
      }
      Datastore.delete(ks:_*)
    }
    "seq put and get" in {
      val d1 = seqData
      val k = d1.put
      val d2 = SeqData.get(k)
      seqPutAndGetTest(k, d1, d2)
      printSeqData(d2)
      Datastore.delete(k)
    }
    "low-level api sample" in {
      val ds = DatastoreServiceFactory.getDatastoreService
      val p = Person("John", 13)
      val e = new Entity("Person")
      e.setProperty("name", p.name)
      e.setProperty("age", p.age)
      val key = ds.put(e)

      val e2 = ds.get(key)
      val p2 = Person(e2.getProperty("name").asInstanceOf[String], e2.getProperty("age").asInstanceOf[Long])
    }
    "gaeds sample" in {
      val p = new Person2("John", 13)
      val key = p.put()

      val p2 = Person2.get(key)
    }
  }
  "Query" should {
    "basic" in {
      val d1 = data
      val k = d1.put
      val d2 = Data.query.asIterator.next
      Data.query.count must be === 1
      Datastore.delete(k)
      d1 must be === d2
    }
    "QueryResult" in {
      val ds = Seq(data, data, data)
      Datastore.put(ds:_*)
      val ite1 = Data.query.asQueryResultIterator(false)
      val ite2 = Data.query.asIteratorWithCursorAndIndex
      for ((e, (d, c, i)) <- ite1.asScala zip ite2) {
        ite1.getCursor must be === c()
        Data.fromEntity(e) must be === d
      }
    }
    "low-level api sample" in {
      val ds = DatastoreServiceFactory.getDatastoreService
      val q = new Query("Person")
      q.addFilter("age", GREATER_THAN_OR_EQUAL, 10)
      q.addFilter("age", LESS_THAN_OR_EQUAL, 20)
      q.addSort("age", ASCENDING)
      q.addSort("name", ASCENDING)
      val ps = for (e <- ds.prepare(q).asIterator.asScala) yield {
        Person(e.getProperty("name").asInstanceOf[String], e.getProperty("age").asInstanceOf[Long])
      }
    }
    "gaeds sample" in {
      val ps = Person2.query.filter(_.age #>= 10).filter(_.age #<= 20).sort(_.age asc).sort(_.name asc).asIterator
    }
  }
}
