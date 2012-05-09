# gaeds
gaeds is a simple type-safe Scala wrapper for Google App Engine Datastore.

## Sample

### in Google App Engine Low-Level API

```scala
import scala.collection.JavaConverters._
import com.google.appengine.api.datastore.{ DatastoreServiceFactory, Entity }
import com.google.appengine.api.datastore.Query
import com.google.appengine.api.datastore.Query.FilterOperator._
import com.google.appengine.api.datastore.Query.SortDirection._

val ds = DatastoreServiceFactory.getDatastoreService

// data
val kind = "Person"

case class Person(name: String, age: Long)

// put
val p = Person("John", 13)
val e = new Entity(kind)
e.setProperty("name", p.name)
e.setProperty("age", p.age)
val key = ds.put(e)

// get
val e2 = ds.get(key)
val p2 = Person(e2.getProperty("name").asInstanceOf[String], e2.getProperty("age").asInstanceOf[Long])

// query
val q = new Query(kind)
q.addFilter("age", GREATER_THAN_OR_EQUAL, 10)
q.addFilter("age", LESS_THAN_OR_EQUAL, 20)
q.addSort("age", ASCENDING)
q.addSort("name", ASCENDING)
val ps = for (e <- ds.prepare(q).asIterator.asScala) yield {
  Person(e.getProperty("name").asInstanceOf[String], e.getProperty("age").asInstanceOf[Long])
}
```

### in gaeds

```scala
import com.github.hexx.gaeds.{ Mapper, Property }
import com.github.hexx.gaeds.Property._

// data
class Person(
    val name: Property[String],
    val age: Property[Long])
  extends Mapper[Person] {
  def this() = this("", 0)
}

object Person extends Person

// put
val p = new Person("John", 13)
val key = p.put()

// get
val p2 = Person.get(key) // or
val p3 = Datastore.get(key) // or
val p4 = key.get

// query
val ps = Person.query
    .filter(_.age #>= 10)
    .filter(_.age #<= 20)
    .sort(_.age asc)
    .sort(_.name asc)
    .asIterator
```
