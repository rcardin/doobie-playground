import cats.data.NonEmptyList
import cats.effect._
import cats.implicits.catsSyntaxApplicativeId
import doobie._
import doobie.implicits._
// Very important to deal with arrays
import doobie.postgres._
import doobie.postgres.implicits._
import doobie.util.transactor.Transactor._

object DoobieApp extends IOApp {

  case class Actor(id: Int, name: String)

  case class Movie(id: String, title: String, year: Int, actors: List[String], director: String)

  // TODO What's Aux?
  val xa: Transactor[IO] = Transactor.fromDriverManager[IO](
    "org.postgresql.Driver",
    "jdbc:postgresql:myimdb",
    "postgres",
    "example"
  )

  def findAllActorsNamesProgram: IO[List[String]] = {
    val findAllActorsQuery: doobie.Query0[String] = sql"select name from actors".query[String]
    val findAllActors: doobie.ConnectionIO[List[String]] = findAllActorsQuery.to[List]
    findAllActors.transact(xa)
  }

  def findAllActorsNamesUsingStreams(): Unit = {
    val actorsNamesStream: fs2.Stream[doobie.ConnectionIO, String] = sql"select name from actors".query[String].stream
  }

  def findAllActorsIdsAndNames: IO[List[(Int, String)]] = {
    val query: doobie.Query0[(Int, String)] = sql"select id, name from actors".query[(Int, String)]
    val findAllActors: doobie.ConnectionIO[List[(Int, String)]] = query.to[List]
    findAllActors.transact(xa)
  }

  def findAllActorNamesUsingLowLevelApiProgram: IO[List[String]] = {
    val query = "select name from actors"
    HC.stream[String](
      query,
      ().pure[PreparedStatementIO], // Input parameters to the sql statement
      512
    ).compile.toList.transact(xa)
  }

  def findAllActorsProgram: IO[List[Actor]] = {
    val findAllActors: fs2.Stream[doobie.ConnectionIO, Actor] =
      sql"select id, name from actors".query[Actor].stream
    findAllActors.compile.toList.transact(xa)
  }

  def findAllDirectorsProgram: IO[List[(String, String)]] = {
    val findAllDirectors: fs2.Stream[doobie.ConnectionIO, (String, String)] =
      sql"select name, last_name from directors ".query[(String, String)].stream
    findAllDirectors.compile.toList.transact(xa)
  }

  def findActorByName(actorName: String): IO[Option[Actor]] = {
    val findActor: doobie.ConnectionIO[Option[Actor]] =
      sql"select id, name from actors where name = $actorName".query[Actor].option
    findActor.transact(xa)
  }

  def findActorsByNameInitialLetterProgram(initialLetter: String): IO[List[Actor]] = {
    val findActors: fs2.Stream[doobie.ConnectionIO, Actor] =
      sql"select id, name from actors where LEFT(name, 1) = $initialLetter".query[Actor].stream
    findActors.compile.toList.transact(xa)
  }

  def findActorByNameUsingLowLevelApi(actorName: String): IO[Option[Actor]] = {
    val query = "select id, name from actors where name = ?"
    HC.stream[Actor](
      query,
      HPS.set(actorName),   // Parameters start from index 1 by default
      512
    ).compile
      .toList
      .map(_.headOption)
      .transact(xa)
  }

  def findActorsByInitialLetterUsingFragments(initialLetter: String): IO[List[Actor]] = {
    val select: Fragment = fr"select id, name"
    val from: Fragment = fr"from actors"
    val where: Fragment = fr"where LEFT(name, 1) = $initialLetter"

    val statement = select ++ from ++ where

    statement.query[Actor].stream.compile.toList.transact(xa)
  }

  def findActorsByInitialLetterUsingFragmentsAndMonoids(initialLetter: String): IO[List[Actor]] = {
    import cats.syntax.monoid._

    val select: Fragment = fr"select id, name"
    val from: Fragment = fr"from actors"
    val where: Fragment = fr"where LEFT(name, 1) = $initialLetter"

    val statement = select |+| from |+| where

    statement.query[Actor].stream.compile.toList.transact(xa)
  }

  def findActorsByNames(actorNames: NonEmptyList[String]): IO[List[Actor]] = {
    val sqlStatement: Fragment =
      fr"select id, name from actors where " ++ Fragments.in(fr"name", actorNames)
    sqlStatement.query[Actor].stream.compile.toList.transact(xa)
  }

  def saveActor(name: String): IO[Int] = {
    // The withUniqueGeneratedKeys says that we expected only one row back, and
    // allows us to get a set of columns from the modified row.
    val saveActor: doobie.ConnectionIO[Int] =
    sql"insert into actors (name) values ($name)"
      .update.withUniqueGeneratedKeys[Int]("id")
    saveActor.transact(xa)
  }

  def saveAndGetActor(name: String): IO[Actor] = {
    // There is also a variant of the withUniqueGeneratedKeys that
    // allows us to retrieve more than a row. It's called withGeneratedKeys.
    val retrievedActor = for {
      id <- sql"insert into actors (name) values ($name)".update.withUniqueGeneratedKeys[Int]("id")
      actor <- sql"select * from actors where id = $id".query[Actor].unique
    } yield actor
    retrievedActor.transact(xa)
  }

  def saveActors(actors: NonEmptyList[String]): IO[List[Int]] = {
    // This is a simple String, not a Fragment.
    val insertStmt: String = "insert into actors (name) values (?)"
    val actorsIds = Update[String](insertStmt).updateManyWithGeneratedKeys[Int]("id")(actors.toList)
    actorsIds.compile.toList.transact(xa)
  }

  class Director(_name: String, _lastName: String) {
    def name: String = _name

    def lastName: String = _lastName

    override def toString: String = s"$name $lastName"
  }

  object Director {
    implicit val directorRead: Read[Director] =
      Read[(String, String)].map { case (name, lastname) => new Director(name, lastname) }

    implicit val directorWrite: Write[Director] =
      Write[(String, String)].contramap(director => (director.name, director.lastName))
  }

  // Cannot find or construct a Read instance for type:
  //
  //   DoobieApp.Director
  //
  // This can happen for a few reasons, but the most common case is that a data
  // member somewhere within this type doesn't have a Get instance in scope. Here are
  // some debugging hints:
  //
  // - For Option types, ensure that a Read instance is in scope for the non-Option
  //   version.
  // - For types you expect to map to a single column ensure that a Get instance is
  //   in scope.
  // - For case classes, HLists, and shapeless records ensure that each element
  //   has a Read instance in scope.
  // - Lather, rinse, repeat, recursively until you find the problematic bit.
  //
  // You can check that an instance exists for Read in the REPL or in your code:
  //
  //   scala> Read[Foo]
  //
  // and similarly with Get:
  //
  //   scala> Get[Foo]
  //
  // And find the missing instance and construct it as needed. Refer to Chapter 12
  // of the book of doobie for more information.
  //
  //       sql"""select "NAME", "LAST_NAME" from "DIRECTORS" """.query[Director].stream
  def findAllDirectors(): IO[List[Director]] = {
    val findAllDirectors: fs2.Stream[doobie.ConnectionIO, Director] =
      sql"select name, last_name from directors".query[Director].stream
    findAllDirectors.compile.toList.transact(xa)
  }

  def findMovieByName(movieName: String): IO[Option[Movie]] = {
    val query = sql"""
         |SELECT m.id,
         |       m.title,
         |       m.year_of_production,
         |       array_agg(a.name) as actors,
         |       d.name
         |FROM movies m
         |JOIN movies_actors ma ON m.id = ma.movie_id
         |JOIN actors a ON ma.actor_id = a.id
         |JOIN directors d ON m.director_id = d.id
         |WHERE m.title = $movieName
         |GROUP BY (m.id,
         |          m.title,
         |          m.year_of_production,
         |          d.name,
         |          d.last_name)
         |""".stripMargin
      .query[Movie]
      .option
    query.transact(xa)
  }

  override def run(args: List[String]): IO[ExitCode] = {
    findActorsByInitialLetterUsingFragmentsAndMonoids("H")
      .map(println)
      .as(ExitCode.Success)
  }
}
