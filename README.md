netty-codec-kryo
================

A simple Netty-Decoder/Encoder pair using [Kryo](http://github.github.com/github-flavored-markdown/sample_content.html).  

Features
--------
Registration of classes is done at runtime by establishing per-connection mappings from classes to ids automatically. 
When available the "Scala-Serializers" from [akka-kryo-serialization](https://github.com/romix/akka-kryo-serialization) are used.
