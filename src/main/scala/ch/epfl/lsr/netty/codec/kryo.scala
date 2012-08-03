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

package ch.epfl.lsr.netty.codec.kryo

import com.esotericsoftware.kryo.{ Kryo, Registration, Serializer }
import com.esotericsoftware.kryo.io.{ Input, Output }
import com.esotericsoftware.kryo.util.{ DefaultClassResolver, MapReferenceResolver }

import org.jboss.netty.channel.{ Channel, ChannelHandlerContext }
import org.jboss.netty.buffer.{ ChannelBuffer, ChannelBuffers, ChannelBufferInputStream, ChannelBufferOutputStream }
import org.jboss.netty.handler.codec.frame.{ LengthFieldPrepender, LengthFieldBasedFrameDecoder }

import scala.collection.immutable.Queue

import java.util.concurrent.atomic.AtomicInteger

case class ClassIdMapping(val clazzName :String, val id :Int) { 
  def this(reg :Registration) = this(reg.getType.getName, reg.getId)
  def registerWith(kryo :Kryo) = { 
    val clazz = Class forName clazzName
    val registration = new Registration(clazz, kryo.getDefaultSerializer(clazz), id)
    kryo.register(registration) == registration
  }
}

class ClassResolver(encoder :KryoEncoder) extends DefaultClassResolver { 
  var ids = new AtomicInteger(128)
  val lock :AnyRef = new Object

  override def getRegistration(clazz :Class[_]) = { 
    lock.synchronized { 
      super.getRegistration(clazz)
    }
  }

  override def register(reg :Registration) = { 
    lock.synchronized { 
      val rv = getRegistration(reg.getClass)
      if(rv == null)
	super.register(reg)
      else
	rv
    }
  }

  override def registerImplicit(clazz :Class[_]) = { 
    val id = ids.getAndIncrement

    val reg = new Registration(clazz, kryo.getDefaultSerializer(clazz), id)
    val rv = this.register(reg)

    if(rv == reg) { 
      encoder enqueueRegistration reg
    }
    
    rv
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
    val resolver = if(encoder == null) new DefaultClassResolver() else new ClassResolver(encoder)

    val kryo = new Kryo(resolver, new MapReferenceResolver)
    kryo.setInstantiatorStrategy(new org.objenesis.strategy.StdInstantiatorStrategy())
    kryo setRegistrationRequired false

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
  private val lock :AnyRef = new Object
  private var theQ :Queue[ClassIdMapping] = Queue.empty
  
  private def resetQ = { 
    lock.synchronized { 
      if(theQ.nonEmpty) { 
	val rv = theQ
	theQ = Queue.empty
	rv
      } else 
	theQ
    }
  }

  private def enQ(mapping :ClassIdMapping) = { 
    lock.synchronized { 
      theQ = theQ enqueue mapping
    }
  }

  // clears the queue !
  def getMappingsBuffer(ctx :ChannelHandlerContext, channel :Channel) = { 
    val q = resetQ

    q.foldLeft(ChannelBuffers.EMPTY_BUFFER) { 
      (buffer,mapping) =>
	ChannelBuffers.wrappedBuffer(buffer, encode2buffer(ctx, channel, mapping))
    }
  } 

  private def encode2buffer(ctx :ChannelHandlerContext, channel :Channel, msg :Object) = { 
    val os = new ChannelBufferOutputStream(ChannelBuffers.dynamicBuffer)
    val output = new Output(os)
    kryo.writeClassAndObject(output, msg)
    output.flush
    output.close
    super.encode(ctx, channel, os.buffer).asInstanceOf[ChannelBuffer]
  }


  val kryo = KryoFactory.getKryo(this)

  def enqueueRegistration(registration :Registration) { 
    enQ(new ClassIdMapping(registration))
  }
  
  def prependWithMappings(ctx :ChannelHandlerContext, channel :Channel, cb :ChannelBuffer) =  
    ChannelBuffers.wrappedBuffer(getMappingsBuffer(ctx, channel), cb)

  override def encode(ctx :ChannelHandlerContext, channel :Channel, msg :Object) = { 
    prependWithMappings(ctx, channel, encode2buffer(ctx, channel, msg))
  }
  
}
