import cats.effect.{MonadCancelThrow, Resource}
import domain.Director
import doobie.{Read, Write}
import doobie.implicits._
import doobie.util.transactor.Transactor

object domain {
  class Director(_name: String, _lastName: String) {
    def name: String = _name

    def lastName: String = _lastName

    override def toString: String = s"$name $lastName"
  }
}

trait Directors[F[_]] {
  def findById(id: Int): F[Option[Director]]
  def findAll: F[List[Director]]
  def create(name: String, lastName: String): F[Int]
}

object Directors {
  def make[F[_]: MonadCancelThrow](postgres: Resource[F, Transactor[F]]): Directors[F] = {
    new Directors[F] {
      import DirectorSQL._

      def findById(id: Int): F[Option[Director]] =
        postgres.use { xa =>
          sql"SELECT name, last_name FROM directors WHERE id = $id".query[Director].option.transact(xa)
        }

      def findAll: F[List[Director]] =
        postgres.use { xa =>
          sql"SELECT name, last_name FROM directors".query[Director].to[List].transact(xa)
        }

      def create(name: String, lastName: String): F[Int] =
        postgres.use { xa =>
          sql"INSERT INTO directors (name, last_name) VALUES ($name, $lastName)".update.withUniqueGeneratedKeys[Int]("id").transact(xa)
        }
    }
  }
}

private object DirectorSQL {
  implicit val directorRead: Read[Director] =
    Read[(String, String)].map { case (name, lastname) => new Director(name, lastname) }

  implicit val directorWrite: Write[Director] =
    Write[(String, String)].contramap(director => (director.name, director.lastName))
}
