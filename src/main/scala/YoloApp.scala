import cats.effect.IO
import doobie.Transactor
import doobie.implicits.toSqlInterpolator

object YoloApp extends App {

  import cats.effect.unsafe.implicits.global

  val xa: Transactor[IO] = Transactor.fromDriverManager[IO](
    "org.postgresql.Driver",
    "jdbc:postgresql:myimdb",
    "postgres",
    "example"
  )

  val y = xa.yolo
  import y._

  val query = sql"select name from actors".query[String].to[List]
  query.quick.unsafeRunSync()
}
