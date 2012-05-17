# Gaeds

Gaeds is a simple type-safe Scala wrapper for Google App Engine Datastore.

## Features

- Gaeds provides an Object/Entity mapper with a type-safe Key.
- Gaeds provides a type-safe query DSL.
- Gaeds persists `Serializable` objects (include Case Class in Scala).
- Gaeds deals with `null` properties as `Option` objects.
- Gaeds is very simple, does not depend on other libraries.

## Using Gaeds

Add the following to your sbt configuration.

    libraryDependencies += "com.github.hexx" %% "gaeds" % "0.0.3"

## Sample to compare with a low-level API

### in Google App Engine low-level API

```scala
import scala.collection.JavaConverters._
import com.google.appengine.api.datastore.{ DatastoreServiceFactory, Entity, Query }
import com.google.appengine.api.datastore.Query.FilterOperator._
import com.google.appengine.api.datastore.Query.SortDirection._

val ds = DatastoreServiceFactory.getDatastoreService

// data
val kind = "Person"

case class Person(name: String, age: Long)

val p = Person("John", 13)

// put
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

### in Gaeds

```scala
import com.github.hexx.gaeds.{ Datastore, Mapper, Property }
import com.github.hexx.gaeds.Property._

// data
class Person(
    val name: Property[String],
    val age: Property[Long])
  extends Mapper[Person] {
  def this() = this("", 0)
}

object Person extends Person

val p = new Person("John", 13)

// put
val key = Datastore.put(p) // or
val key2 = p.put()

// get
val p2 = Datastore.get(key) // or
val p3 = key.get

// query
val ps = Person.query
    .filter(_.age #>= 10)
    .filter(_.age #<= 20)
    .sort(_.age asc)
    .sort(_.name asc)
    .asIterator
```
