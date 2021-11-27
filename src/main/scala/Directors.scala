import cats.effect.{MonadCancelThrow, Resource}
import domain.{Director, DirectorId, DirectorLastName, DirectorName}
import doobie.{Read, Write}
import doobie.implicits._
import doobie.util.transactor.Transactor
import io.estatico.newtype.macros.newtype

object domain {
  @newtype case class DirectorId(id: Int)
  @newtype case class DirectorName(name: String)
  @newtype case class DirectorLastName(lastName: String)
  case class Director(id: DirectorId, name: DirectorName, lastName: DirectorLastName)
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
    Read[(Int, String, String)].map { case (id, name, lastname) =>
      Director(DirectorId(id), DirectorName(name), DirectorLastName(lastname))
    }

  implicit val directorWrite: Write[Director] =
    Write[(Int, String, String)].contramap { director =>
      (director.id.id, director.name.name, director.lastName.lastName)
    }
}
