import org.scalatest.{ WordSpec, BeforeAndAfter }
import org.scalatest.matchers.MustMatchers

import java.util.Date

import scala.collection.JavaConverters._

import com.google.appengine.api.blobstore.BlobKey
import com.google.appengine.api.datastore.{ DatastoreServiceFactory, Entity, Query, Key => LLKey }
import com.google.appengine.api.datastore.Query.{ CompositeFilterOperator, FilterPredicate }
import com.google.appengine.api.datastore.Query.FilterOperator._
import com.google.appengine.api.datastore.Query.SortDirection._
import com.google.appengine.api.users.User
import com.google.appengine.tools.development.testing.{ LocalDatastoreServiceTestConfig, LocalServiceTestHelper }

import net.liftweb.json._

import com.github.hexx.gaeds.{ Datastore, Mapper, Key }
import com.github.hexx.gaeds.Property._

import SampleData._

class GaedsSpec extends WordSpec with BeforeAndAfter with MustMatchers {
  val helper = new LocalServiceTestHelper(new LocalDatastoreServiceTestConfig())
  before {
    helper.setUp()
  }
  after {
    helper.tearDown()
  }

  def putCheck[T <: Mapper[T]](k: Key[T], d: T) {
    k.id must not be 0
    d.key.get must be === k
  }
  def putAndGetCheck[T <: Mapper[T]](k: Key[T], d1: T, d2: T) {
    putCheck(k, d1)
    d1 must be === d2
    d1.key.get must be === d2.key.get
    d1.toString must be === d2.toString
  }
  def putAndGetTest[T <: Mapper[T]: ClassManifest](d1: T, key: Option[Key[T]] = None) = {
    val k = key match {
      case Some(k) => {
        d1.key = key
        d1.put
        k
      }
      case None => d1.put
    }
    val d2 = Datastore.get(k)
    putAndGetCheck(k, d1, d2)
    checkUnindexedProperty(d1) must be === false
    checkUnindexedProperty(d2) must be === false
    Datastore.delete(k)
  }
  def putAndGetTwiceTest[T <: Mapper[T]: ClassManifest](d1: T) = {
    val k1 = d1.put
    val d2 = Datastore.get(k1)
    val k2 = d2.put
    val d3 = Datastore.get(k2)
    putAndGetCheck(k2, d1, d3)
    Datastore.delete(k1)
  }
  def putAndGetUnindexedTest[T <: Mapper[T]: ClassManifest](d1: T) = {
    val k = d1.put
    val d2 = Datastore.get(k)
    putAndGetCheck(k, d1, d2)
    checkUnindexedProperty(d1) must be === true
    checkUnindexedProperty(d2) must be === true
    Datastore.delete(k)
  }
  def multiPutAndGetTest[T <: Mapper[T]: ClassManifest](ds1: T*) = {
    val ks = Datastore.put(ds1:_*)
    val ds2 = Datastore.get(ks:_*).values.toSeq
    for (((k, d1), d2) <- ks zip ds1.sortBy(_.key.get) zip ds2.sortBy(_.key.get)) {
      putAndGetCheck(k, d1, d2)
    }
    Datastore.delete(ks:_*)
  }
  def updateTest[T <: Mapper[T]: ClassManifest](d1: T, update: T => T) = {
    val k = update(d1).put
    val d2 = Datastore.get(k)
    putAndGetCheck(k, d1, d2)
    Datastore.delete(k)
  }
  def updateTwiceTest[T <: Mapper[T]: ClassManifest](d1: T, update1: T => T, update2: T => T) = {
    val k1 = update1(d1).put
    val d2 = Datastore.get(k1)
    val k2 = update2(d2).put
    val d3 = Datastore.get(k2)
    val k3 = update1(d3).put
    val d4 = Datastore.get(k3)
    putAndGetCheck(k3, d1, d4)
    Datastore.delete(k1)
  }
  def putAndGetAsyncTest[T <: Mapper[T]: ClassManifest](d1: T) = {
    val k = d1.putAsync.get
    val d2 = Datastore.getAsync(k).get
    putAndGetCheck(k, d1, d2)
    Datastore.deleteAsync(k)
  }
  def multiPutAndGetAsyncTest[T <: Mapper[T]: ClassManifest](ds1: T*) = {
    val ks = Datastore.putAsync(ds1:_*).get
    val ds2 = Datastore.getAsync(ks:_*).get.values.toSeq
    for (((k, d1), d2) <- ks zip ds1.sortBy(_.key.get) zip ds2.sortBy(_.key.get)) {
      putAndGetCheck(k, d1, d2)
    }
    Datastore.deleteAsync(ks:_*)
  }

  "Mappers" can {
    "put and get" in {
      putAndGetTest(data)
    }
    "multi-put and multi-get" in {
      multiPutAndGetTest(data, data, data)
    }
    "put and get twice" in {
      putAndGetTwiceTest(data)
    }
    "update" in {
      updateTest(data, { d: Data => d.string = "newstring"; d })
    }
    "update twice" in {
      updateTwiceTest(data, { d: Data => d.string = "newstring"; d }, { d: Data => d.string = "string"; d })
    }
    "put and get using transactions" in {
      val d1 = data
      val k = d1.put
      Datastore.transaction {
        val d2 = Datastore.get(k)
        putAndGetCheck(k, d1, d2)
        Datastore.delete(k)
      }
    }
    "put and get asynchronously" in {
      putAndGetAsyncTest(data)
    }
    "multi-put and multi-get asynchronously" in {
      multiPutAndGetAsyncTest(data)
    }
    "put and get using allocated ids" in {
      putAndGetTest(data, Some(Data.allocateId))
    }
    "serialize to JSON" in {
      val d = data
      val k = d.put
      putAndGetCheck(k, d, Data.fromJObject(d.toJObject))
    }
  }
  "Mappers have Unindexed Properties" can {
    "put and get" in {
      putAndGetUnindexedTest(unindexedData)
    }
  }
  "Mappers have Seq Properties" can {
    "put and get" in {
      putAndGetTest(seqData)
    }
    "put and get twice" in {
      putAndGetTwiceTest(seqData)
    }
  }
  "Mappers have empty Seq Properties" can {
    "put and get" in {
      putAndGetTest(emptySeqData)
    }
  }
  "Mappers have Option Properties" can {
    "put and get" in {
      putAndGetTest(optionData)
    }
  }
  "Mappers have None Properties" can {
    "put and get" in {
      putAndGetTest(noneOptionData)
    }
  }
  "Key Properties" should {
    "be saved as Low-Level Key" in {
      val d1 = data
      val k1 = d1.put
      val d2 = new KeyTestData(k1, Seq(k1), Option(k1))
      val e = d2.toEntity
      e.getProperty("dataKey").asInstanceOf[LLKey] must be ===  k1.key
      e.getProperty("dataKeys").asInstanceOf[java.util.List[LLKey]].get(0) must be ===  k1.key
      e.getProperty("dataKeyOption").asInstanceOf[LLKey] must be ===  k1.key
      val k2 = d2.put
      val d3 = k2.get
      val d4 = d3.dataKey.get
      putAndGetCheck(k1, d1, d4)
      val d5 = d3.dataKeys(0).get
      putAndGetCheck(k1, d1, d5)
      val d6 = d3.dataKeyOption.get.get
      putAndGetCheck(k1, d1, d6)
    }
  }
  "Queries" when {
    "there is only one Mapper" should {
      "be single" in {
        val d1 = data
        val k = d1.put
        val d2 = Data.query.asIterator.next
        val d3 = Data.query.asSingle
        Data.query.count must be === 1
        putAndGetCheck(k, d1, d2)
        putAndGetCheck(k, d1, d3)
        Datastore.delete(k)
      }
    }
  }
  "Queries" can {
    "filter and sort" in {
      Datastore.put(new Person2("John", 18), new Person2("Mike", 12), new Person2("Mary", 15))
      Person2.query.filter(_.age #> 13).sort(_.age asc).count must be === 2
      val ite = Person2.query.filter(_.age #> 13).sort(_.age asc).asIterator
      ite.next.age.__valueOfProperty must be === 15
      ite.next.age.__valueOfProperty must be === 18
      ite.hasNext must be === false
    }
    "use 'and' filter operator" in {
      Datastore.put(new Person2("John", 8), new Person2("Mike", 20), new Person2("Mary", 30), new Person2("Paul", 40))
      val ite = Person2.query.filter(p => p.age #> 10 and p.age #< 40).sort(_.age asc).asIterator
      ite.next.age.__valueOfProperty must be === 20
      ite.next.age.__valueOfProperty must be === 30
      ite.hasNext must be === false
    }
    "use 'or' filter operator" in {
      Datastore.put(new Person2("John", 8), new Person2("Mike", 20), new Person2("Mary", 30), new Person2("Paul", 40))
      val ite = Person2.query.filter(p => p.age #< 10 or p.age #> 30).sort(_.age asc).asIterator
      ite.next.age.__valueOfProperty must be === 8
      ite.next.age.__valueOfProperty must be === 40
      ite.hasNext must be === false
    }
  }
  "Queries" should {
    "be the same result with Low-Level API" in {
      val ds = Seq(data, data, data)
      Datastore.put(ds:_*)
      val ite1 = Data.query.asQueryResultIterator(false)
      val ite2 = Data.query.asIteratorWithCursorAndIndex
      for ((e, (d, c, i)) <- ite1.asScala zip ite2) {
        ite1.getCursor must be === c()
        Data.fromEntity(e) must be === d
      }
    }
  }
  "gaeds" should {
    "provide a low-level api put and get sample" in {
      val ds = DatastoreServiceFactory.getDatastoreService
      val p = Person("John", 13)
      val e = new Entity("Person")
      e.setProperty("name", p.name)
      e.setProperty("age", p.age)
      val key = ds.put(e)

      val e2 = ds.get(key)
      val p2 = Person(e2.getProperty("name").asInstanceOf[String], e2.getProperty("age").asInstanceOf[Long])
    }
    "provide a gaeds put and get sample" in {
      val p = new Person2("John", 13)
      val key = p.put()

      val p2 = Datastore.get(key)
    }
    "provide a low-level api query sample" in {
      val ds = DatastoreServiceFactory.getDatastoreService
      val q = new Query("Person")
      val f1 = new FilterPredicate("age", GREATER_THAN_OR_EQUAL, 10)
      val f2 = new FilterPredicate("age", LESS_THAN_OR_EQUAL, 20)
      q.setFilter(CompositeFilterOperator.and(f1, f2))
      q.addSort("age", ASCENDING)
      q.addSort("name", ASCENDING)
      val ps = for (e <- ds.prepare(q).asIterator.asScala) yield {
        Person(e.getProperty("name").asInstanceOf[String], e.getProperty("age").asInstanceOf[Long])
      }
    }
    "provide a gaeds query sample" in {
      val ps = Person2.query.filter(p => (p.age #>= 10) and (p.age #<= 20)).sort(_.age asc).sort(_.name asc).asIterator
    }
  }
}
