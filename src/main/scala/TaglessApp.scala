import cats.effect.kernel.Resource
import cats.effect.{ExitCode, IO, IOApp}
import doobie.hikari.HikariTransactor
import doobie.util.ExecutionContexts

object TaglessApp extends IOApp {
  override def run(args: List[String]): IO[ExitCode] = {
    val postgres: Resource[IO, HikariTransactor[IO]] = for {
      ce <- ExecutionContexts.fixedThreadPool[IO](32) // our connect EC
      xa <- HikariTransactor.newHikariTransactor[IO](
        "org.postgresql.Driver",
        "jdbc:postgresql:myimdb",
        "postgres",
        "example",                              // password
        ce                                   // await connection here
      )
    } yield xa

    val directors: Directors[IO] = Directors.make(postgres)

    val program: IO[Unit] = for {
      id <- directors.create("Steven", "Spielberg")
      spielberg <- directors.findById(id)
      _ <- IO.println(s"The director of Jurassic Park is: $spielberg")
      directorsList <- directors.findAll
      _ <- IO.println(s"All directors: $directorsList")
    } yield ()

    program.as(ExitCode.Success)
  }
}
