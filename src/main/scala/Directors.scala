import DoobieApp.Director
import cats.effect.{MonadCancelThrow, Resource}
import doobie.implicits._
import doobie.util.transactor.Transactor

// TODO Move the class Director to a better place
trait Directors[F[_]] {
  def findById(id: Int): F[Option[Director]]
  def findAll: F[List[Director]]
  def create(director: Director): F[Int]
}

object Directors {
  def make[F[_]: MonadCancelThrow](postgres: Resource[F, Transactor[F]]): Directors[F] = {
    new Directors[F] {
      import DirectorSQL._

      def findById(id: Int): F[Option[Director]] =
        postgres.use { xa =>
          selectById.option.transact(xa)
        }

      def findAll: F[List[Director]] =
        postgres.use { xa =>
          selectAll.to[List].transact(xa)
        }

      def create(director: Director): F[Int] =
        postgres.use { xa =>
          insert.withUniqueGeneratedKeys[Int]("id").transact(xa)
        }
    }
  }
}

// TODO Resolve the problem with interpolation
private object DirectorSQL {
  val selectAll: doobie.Query0[Director] = sql"SELECT id, name FROM directors".query[Director]
  val selectById: doobie.Query0[Director] = sql"SELECT id, name FROM directors WHERE id = $id".query[Director]
  val insert: doobie.Update0 = sql"INSERT INTO directors (name, last_name) VALUES ($name, $lastName)".update
}
