name := "doobie-playground"

version := "0.1.0"

scalaVersion := "2.13.6"

val DoobieVersion = "1.0.0-RC1"

libraryDependencies ++= Seq(
  "org.tpolecat" %% "doobie-core"     % DoobieVersion,
  "org.tpolecat" %% "doobie-postgres" % DoobieVersion,
  "org.tpolecat" %% "doobie-hikari"   % DoobieVersion,
  "org.tpolecat" %% "doobie-specs2"   % DoobieVersion % "test",
)