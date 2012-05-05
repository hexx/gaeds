import org.scalatest.{ WordSpec, BeforeAndAfter }
import org.scalatest.matchers.MustMatchers

import java.util.Date

import scala.collection.JavaConverters._

import com.google.appengine.api.blobstore.BlobKey
import com.google.appengine.api.datastore.{ Key => GAEKey, _ }
import com.google.appengine.api.users.User
import com.google.appengine.tools.development.testing.{ LocalDatastoreServiceTestConfig, LocalServiceTestHelper }

import com.github.hexx.gaeds._
import com.github.hexx.gaeds.Property._

import SampleData._

// low-level sample
import com.google.appengine.api.datastore.{ DatastoreServiceFactory, Entity }
import com.google.appengine.api.datastore.{ Query => GAEQuery }
import com.google.appengine.api.datastore.Query.FilterOperator._
import com.google.appengine.api.datastore.Query.SortDirection._

// gaeds sample
import com.github.hexx.gaeds._
import com.github.hexx.gaeds.Property._

class Person2(val name: Property[String], val age: Property[Long]) extends Mapper[Person2] {
  def this() = this("", 0)
  override def toString() = "Person(" + name + "," + age + ")"
}
object Person2 extends Person2

class GAEDSSpec extends WordSpec with BeforeAndAfter with MustMatchers {
  val helper = new LocalServiceTestHelper(new LocalDatastoreServiceTestConfig())
  before {
    helper.setUp()
  }
  after {
    helper.tearDown()
  }

  def putTest[T <: Mapper[T]](k: Key[T], d: T) = {
    k.id must not be 0
    d.key.get must be === k
  }
  def putAndGetTest[T <: Mapper[T]](k: Key[T], d1: T, d2: T) = {
    putTest(k, d1)
    d1 must be === d2
    d1.key.get must be === d2.key.get
    d1.toString must be === d2.toString
  }

  def printKey(k: Key[_]) {
    println(k)
    println(k.id)
    println(k.kind)
    println(k.name)
    println(k.namespace)
    println(k.isComplete)
  }

  "Entity" should {
    "put and get" in {
      val d1 = data
      val k = d1.put
      val d2 = Datastore.get(k)
      putAndGetTest(k, d1, d2)
      Datastore.delete(k)
    }
    "multi-put and multi-get" in {
      val ds1 = Seq(data, data, data)
      val ks = Datastore.put(ds1:_*)
      val ds2 = Datastore.get(ks:_*).values.toSeq
      for (((k, d1), d2) <- ks zip ds1.sortBy(_.key.get) zip ds2.sortBy(_.key.get)) {
        putAndGetTest(k, d1, d2)
      }
      Datastore.delete(ks:_*)
    }
    "unindexed put and get" in {
      val d1 = unindexedData
      val k = d1.put
      val d2 = Datastore.get(k)
      putAndGetTest(k, d1, d2)
      checkUnindexedProperty(d1) must be === true
      checkUnindexedProperty(d2) must be === true
      Datastore.delete(k)
    }
    "seq put and get" in {
      val d1 = seqData
      val k = d1.put
      val d2 = Datastore.get(k)
      putAndGetTest(k, d1, d2)
      printSeqData(d2)
      Datastore.delete(k)
    }
    "empty seq put and get" in {
      val d1 = emptySeqData
      val k = d1.put
      val d2 = Datastore.get(k)
      putAndGetTest(k, d1, d2)
      printSeqData(d2)
      Datastore.delete(k)
    }
    "option put and get" in {
      val d1 = optionData
      val k = d1.put
      val d2 = Datastore.get(k)
      putAndGetTest(k, d1, d2)
      printOptionData(d2)
      Datastore.delete(k)
    }
    "none put and get" in {
      val d1 = noneOptionData
      val k = d1.put
      val d2 = Datastore.get(k)
      putAndGetTest(k, d1, d2)
      printOptionData(d2)
      Datastore.delete(k)
    }
    "put and get twice" in {
      val d1 = data
      val k1 = d1.put
      val d2 = Datastore.get(k1)
      val k2 = d2.put
      val d3 = Datastore.get(k2)
      putAndGetTest(k2, d1, d3)
      Datastore.delete(k1)
    }
    "seq put and get twice" in {
      val d1 = seqData
      val k1 = d1.put
      val d2 = Datastore.get(k1)
      val k2 = d2.put
      val d3 = Datastore.get(k2)
      putAndGetTest(k2, d1, d3)
      printSeqData(d3)
      Datastore.delete(k1)
    }
    "update" in {
      val d1 = data
      d1.string = "newstring"
      val k = d1.put
      val d2 = Datastore.get(k)
      putAndGetTest(k, d1, d2)
      Datastore.delete(k)
    }
    "update twice" in {
      val d1 = data
      d1.string = "newstring"
      val k1 = d1.put
      val d2 = Datastore.get(k1)
      d2.string = "string"
      val k2 = d2.put
      val d3 = Datastore.get(k2)
      d3.string = "newstring"
      val k3 = d3.put
      val d4 = Datastore.get(k3)
      putAndGetTest(k3, d1, d4)
      Datastore.delete(k1)
    }
    "transaction sample" in {
      val d1 = data
      val k = d1.put
      Datastore.transaction {
        val d2 = Datastore.get(k)
        putAndGetTest(k, d1, d2)
        Datastore.delete(k)
      }
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

      val p2 = Datastore.get(key)
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
      val q = new GAEQuery("Person")
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
