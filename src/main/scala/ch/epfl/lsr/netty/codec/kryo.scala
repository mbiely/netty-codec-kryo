// Copyright 2012 Martin Biely
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
// 
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package ch.epfl.lsr.testing.netty.kryo

import com.esotericsoftware.kryo.{ Kryo, Registration, Serializer }
import com.esotericsoftware.kryo.io.{ Input, Output }
import com.esotericsoftware.kryo.util.{ DefaultClassResolver, MapReferenceResolver }

import org.jboss.netty.handler.codec.frame.{ LengthFieldPrepender, LengthFieldBasedFrameDecoder }
import org.jboss.netty.buffer.{ ChannelBuffer, ChannelBuffers, ChannelBufferInputStream, ChannelBufferOutputStream }

import org.jboss.netty.channel._

import scala.collection.mutable.Queue

case class ClassIdMapping(val clazzName :String, val id :Int) { 
  def registerWith(kryo :Kryo) = { 
    val clazz = Class forName clazzName
    kryo.register(new Registration(clazz, kryo.getDefaultSerializer(clazz), id))
  }
}

class ClassResolver(encoder :KryoEncoder) extends DefaultClassResolver { 
  var nextId = 128

  override def registerImplicit(clazz :Class[_]) = { 
    val id = nextId
    nextId += 1
    
    val mapping = ClassIdMapping(clazz.getName, id)

    if(encoder != null)
      encoder sendMapping mapping

    mapping registerWith kryo
  }
}


object KryoFactory  { 
  val registerDefaultScalaSerializers : (Kryo=>Unit) =
    try { 
      // scala collections 
      val EnumerationSerializerClazz = Class.forName("com.romix.akka.serialization.kryo.EnumerationSerializer").asInstanceOf[Class[Serializer[_]]]
      val ScalaMapSerializerClazz = Class.forName("com.romix.akka.serialization.kryo.ScalaMapSerializer").asInstanceOf[Class[Serializer[_]]]
      val ScalaSetSerializerClazz = Class.forName("com.romix.akka.serialization.kryo.ScalaSetSerializer").asInstanceOf[Class[Serializer[_]]]
      val ScalaCollectionSerializerClazz = Class.forName("com.romix.akka.serialization.kryo.ScalaCollectionSerializer").asInstanceOf[Class[Serializer[_]]]

      { kryo :Kryo =>
	kryo.addDefaultSerializer(classOf[scala.Enumeration#Value], EnumerationSerializerClazz)
        kryo.addDefaultSerializer(classOf[scala.collection.Map[_,_]], ScalaMapSerializerClazz)
        kryo.addDefaultSerializer(classOf[scala.collection.Set[_]], ScalaSetSerializerClazz)
        kryo.addDefaultSerializer(classOf[scala.collection.generic.MapFactory[scala.collection.Map]], ScalaMapSerializerClazz)
        kryo.addDefaultSerializer(classOf[scala.collection.generic.SetFactory[scala.collection.Set]], ScalaSetSerializerClazz)
        kryo.addDefaultSerializer(classOf[scala.collection.Traversable[_]], ScalaCollectionSerializerClazz)
        ()
     }
    } catch { 
      case e :ClassNotFoundException =>
	System.err.println("akka-kryo-serialization not available");
	{ kryo :Kryo => ()}
    }


  def getKryo(encoder :KryoEncoder = null) = { 

    val kryo = new Kryo(new ClassResolver(encoder), new MapReferenceResolver)
    kryo.setInstantiatorStrategy(new org.objenesis.strategy.StdInstantiatorStrategy())
    kryo setRegistrationRequired false
    kryo setReferences false

    kryo.register(classOf[ClassIdMapping],32)
    
    registerDefaultScalaSerializers(kryo)

    kryo
  }
}


class KryoDecoder extends LengthFieldBasedFrameDecoder(1048576,0,4,0,4) { 
  val kryo = KryoFactory.getKryo(null)

  // to avoid copying as in ObjectDecoder
  override def extractFrame(buffer :ChannelBuffer, index :Int, length :Int) = buffer.slice(index,length)

  override def decode(ctx :ChannelHandlerContext, channel :Channel, buffer :ChannelBuffer) = { 
    val frame = super.decode(ctx, channel, buffer).asInstanceOf[ChannelBuffer]
    if(frame == null) { 
      null
    } else { 
      val is = new ChannelBufferInputStream(frame)
      val msg = kryo.readClassAndObject(new Input(is))

      if(msg.isInstanceOf[ClassIdMapping]) { 
	msg.asInstanceOf[ClassIdMapping].registerWith(kryo)
	// discard the read data
	null 
      } else { 
	msg
      }
    }
  }
}

class KryoEncoder extends LengthFieldPrepender(4) { 
  val kryo = KryoFactory.getKryo(this)
  var pendingRegistrations :Queue[ClassIdMapping] = Queue.empty

  def sendMapping(registration :ClassIdMapping) { 
    pendingRegistrations += registration
  }
  
  override def encode(ctx :ChannelHandlerContext, channel :Channel, msg :Object) = { 
    def encode2buffer(msg :Object) = { 
      val os = new ChannelBufferOutputStream(ChannelBuffers.dynamicBuffer)
      val output = new Output(os)
      kryo.writeClassAndObject(output, msg)
      output.flush
      output.close
      super.encode(ctx, channel, os.buffer).asInstanceOf[ChannelBuffer]
    }

    var mappingsBuffer = ChannelBuffers.EMPTY_BUFFER    
    val msgBuffer = encode2buffer(msg)

    if(pendingRegistrations.nonEmpty) { 
      pendingRegistrations.dequeueAll { mapping => // somewhat abusing dequeueAll
	mappingsBuffer = ChannelBuffers.wrappedBuffer(mappingsBuffer, encode2buffer(mapping))
	true
      }
    } 
    ChannelBuffers.wrappedBuffer(mappingsBuffer,msgBuffer)
  }
}
