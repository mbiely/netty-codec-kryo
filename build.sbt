name := "netty-codec-kryo"

organization := "ch.epfl.lsr"

version := "0.1"

scalaVersion := "2.10.0-M4"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

resolvers += "Typesafe Snapshots Repository" at "http://repo.typesafe.com/typesafe/snapshots/"

libraryDependencies += "io.netty" % "netty" % "3.5.2.Final"

libraryDependencies += "com.esotericsoftware.kryo" % "kryo" % "2.18-SNAPSHOT"

scalacOptions += "-deprecation"

autoScalaLibrary := false
