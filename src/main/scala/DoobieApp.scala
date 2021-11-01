import doobie._
import doobie.implicits._
import cats._
import cats.effect._
import cats.implicits._
import doobie.util.transactor.Transactor.Aux

object DoobieApp extends IOApp {

  case class Actor(id: Int, name: String)

  case class Movie(id: String, title: String, year: Int, actors: List[String], director: String)

  // TODO What's Aux?
  val xa: Aux[IO, Unit] = Transactor.fromDriverManager[IO](
    "org.postgresql.Driver",
    "jdbc:postgresql:postgres",
    "postgres",
    "example"
  )

  def findAllActorsNamesProgram: IO[List[String]] = {
    val findAllActors: fs2.Stream[doobie.ConnectionIO, String] =
      sql"""select "NAME" from "ACTORS" """.query[String].stream
    findAllActors.compile.toList.transact(xa)
  }

  def findAllActorsProgram: IO[List[Actor]] = {
    val findAllActors: fs2.Stream[doobie.ConnectionIO, Actor] =
      sql"""select "ID", "NAME" from "ACTORS" """.query[Actor].stream
    findAllActors.compile.toList.transact(xa)
  }

  def findAllDirectorsProgram: IO[List[(String, String)]] = {
    val findAllDirectors: fs2.Stream[doobie.ConnectionIO, (String, String)] =
      sql"""select "NAME", "LAST_NAME" from "DIRECTORS" """.query[(String, String)].stream
    findAllDirectors.compile.toList.transact(xa)
  }

  def findActorByName(actorName: String): IO[Option[Actor]] = {
    val findActor: doobie.ConnectionIO[Option[Actor]] =
      sql"""select "ID", "NAME" from "ACTORS" where "NAME" = $actorName""".query[Actor].option
    findActor.transact(xa)
  }

  override def run(args: List[String]): IO[ExitCode] = {
    findActorByName("Henry Cavill")
      .map(println)
      .as(ExitCode.Success)
  }
}
