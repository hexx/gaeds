import org.scalatest.{ WordSpec, BeforeAndAfter }
import org.scalatest.matchers.MustMatchers

import java.util.Date

import com.google.appengine.api.blobstore.BlobKey
import com.google.appengine.api.datastore._
import com.google.appengine.api.users.User
import com.google.appengine.tools.development.testing.{ LocalDatastoreServiceTestConfig, LocalServiceTestHelper }

import com.github.hexx.gaeds._
import com.github.hexx.gaeds.Datastore._

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
    val float: Property[Float],
    val double: Property[Double],
    val geoPt: Property[GeoPt],
    val user: Property[User],
    val short: Property[Short],
    val int: Property[Int],
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
      0,
      0,
      new GeoPt(0F, 0F),
      new User("", ""),
      0.toShort,
      0,
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
}

object Data extends Data

class GAEDSSpec extends WordSpec with BeforeAndAfter with MustMatchers {

  def data =
    new Data(
      true,
      Util.createShortBlob("shortBlob"),
      Util.createBlob("blob"),
      new Category("category"),
      new Date,
      new Email("email"),
      1.23F,
      1.23,
      new GeoPt(1.23F, 1.23F),
      new User("test@gmail.com", "gmail.com"),
      123.toShort,
      123,
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
    d1 must be === d2
  }

  def printKey(k: Key) {
    println(k)
    println(k.getId)
    println(k.getKind)
    println(k.getName)
    println(k.getNamespace)
    println(k.isComplete)
  }

  "Entity" should {
    "put and get" in {
      val d1 = data
      val k = d1.put
      val d2 = Data.get(k)
      putAndGetTest(k, d1, d2)
      Datastore.delete(k)
    }
    "multi-put and multi-get" in {
      val ds1 = Seq(data, data, data)
      val ks = Datastore.put(ds1:_*)
      val ds2 = Data.get(ks:_*)
      for (((k, d1), d2) <- ks zip ds1 zip ds2) {
        putAndGetTest(k, d1, d2)
      }
      Datastore.delete(ks:_*)
    }
  }
  "Query" should {
    "basic" in {
      val d1 = data
      val k = d1.put
      val d2 = Data.query.prepare.next
      Data.query.prepare.size must be === 1
      Datastore.delete(k)
      d1 must be === d2
    }
  }
}
