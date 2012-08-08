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

import java.util.LinkedList 

import java.util.concurrent.atomic.AtomicInteger

class ClassIdMapping(val clazzName :String, val id :Int) { 
  def this(reg :Registration) = this(reg.getType.getName, reg.getId)
  def registerWith(kryo :Kryo) = { 
    val clazz = Class forName clazzName
    val registration = new Registration(clazz, kryo.getDefaultSerializer(clazz), id)
    kryo.register(registration) == registration
  }
}

class ClassResolver(listener :ImplicitRegistrationListener) extends DefaultClassResolver { 
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
      listener onImplicitRegistration reg
    }
    
    rv
  }
}


object KryoFactory  { 
  // avoid registerDefault being a scala.Function1
  trait KryoConfigurer { 
    def configure(kryo :Kryo)
  }

  def getConfigurer = { 
    try { 
      // scala collections 
      val EnumerationSerializerClazz = Class.forName("com.romix.akka.serialization.kryo.EnumerationSerializer").asInstanceOf[Class[Serializer[_]]]
      val ScalaMapSerializerClazz = Class.forName("com.romix.akka.serialization.kryo.ScalaMapSerializer").asInstanceOf[Class[Serializer[_]]]
      val ScalaSetSerializerClazz = Class.forName("com.romix.akka.serialization.kryo.ScalaSetSerializer").asInstanceOf[Class[Serializer[_]]]
      val ScalaCollectionSerializerClazz = Class.forName("com.romix.akka.serialization.kryo.ScalaCollectionSerializer").asInstanceOf[Class[Serializer[_]]]

      new KryoConfigurer{ def configure(kryo :Kryo) { 
	kryo.addDefaultSerializer(Class.forName("scala.Enumeration$Value"), EnumerationSerializerClazz)
        kryo.addDefaultSerializer(Class.forName("scala.collection.Map"), ScalaMapSerializerClazz)
        kryo.addDefaultSerializer(Class.forName("scala.collection.Set"), ScalaSetSerializerClazz)
        kryo.addDefaultSerializer(Class.forName("scala.collection.generic.MapFactory"), ScalaMapSerializerClazz)
        kryo.addDefaultSerializer(Class.forName("scala.collection.generic.SetFactory"), ScalaSetSerializerClazz)
        kryo.addDefaultSerializer(Class.forName("scala.collection.Traversable"), ScalaCollectionSerializerClazz)
        }
     }
    } catch { 
      case e :ClassNotFoundException =>
	System.err.println("scala or akka-kryo-serialization not available ("+e+")");
      new KryoConfigurer{ def configure(kryo :Kryo) { }}
    }
  }

  val configureDefaultScalaSerializers : KryoConfigurer = getConfigurer


  def getKryo(listener :ImplicitRegistrationListener) = { 
    val resolver = new ClassResolver(listener)

    val kryo = new Kryo(resolver, new MapReferenceResolver)
    kryo.setInstantiatorStrategy(new org.objenesis.strategy.StdInstantiatorStrategy())
    kryo setRegistrationRequired false

    kryo.register(classOf[ClassIdMapping],32)
    
    configureDefaultScalaSerializers.configure(kryo)

    kryo
  }
}

trait ImplicitRegistrationListener { 
  def onImplicitRegistration(registration :Registration) 

}

trait KryoFromContext extends ImplicitRegistrationListener { 
  def kryo(ctx :ChannelHandlerContext) : Kryo = { 
    var rv = ctx.getAttachment
    if(rv==null) { 
      rv = KryoFactory.getKryo(this)
      ctx.setAttachment(rv)
    }

    rv.asInstanceOf[Kryo]
  }

}

class KryoDecoder extends LengthFieldBasedFrameDecoder(1048576,0,4,0,4) with KryoFromContext { 
  def onImplicitRegistration(registration :Registration) { }

  // to avoid copying as in ObjectDecoder
  override def extractFrame(buffer :ChannelBuffer, index :Int, length :Int) = buffer.slice(index,length)

  override def decode(ctx :ChannelHandlerContext, channel :Channel, buffer :ChannelBuffer) = { 
    val frame = super.decode(ctx, channel, buffer).asInstanceOf[ChannelBuffer]
    if(frame == null) { 
      null
    } else { 
      val is = new ChannelBufferInputStream(frame)
      val msg = kryo(ctx).readClassAndObject(new Input(is))

      if(msg.isInstanceOf[ClassIdMapping]) { 
	msg.asInstanceOf[ClassIdMapping].registerWith(kryo(ctx))
	// discard the read data
	null 
      } else { 
	msg
      }
    }
  }
}


class KryoEncoder extends LengthFieldPrepender(4) with KryoFromContext { 
  private val lock :AnyRef = new Object
  private var theQ :LinkedList[ClassIdMapping] = new LinkedList

  def onImplicitRegistration(registration :Registration) { 
    enQ(new ClassIdMapping(registration))
  }
  
  private def resetQ = { 
    lock.synchronized { 
      if(!theQ.isEmpty) { 
	val rv = theQ
	theQ = new LinkedList
	rv
      } else 
	theQ
    }
  }

  private def enQ(mapping :ClassIdMapping) = { 
    lock.synchronized { 
      theQ addLast mapping
    }
  }

  // clears the queue !
  def getMappingsBuffer(ctx :ChannelHandlerContext, channel :Channel) = { 
    val q = resetQ
    var buffer = ChannelBuffers.EMPTY_BUFFER

    while(!q.isEmpty) { 
      buffer = ChannelBuffers.wrappedBuffer(buffer, encode2buffer(ctx, channel, q.remove)) 
    }

    buffer
  } 

  private def encode2buffer(ctx :ChannelHandlerContext, channel :Channel, msg :Object) = { 
    val os = new ChannelBufferOutputStream(ChannelBuffers.dynamicBuffer)
    val output = new Output(os)
    kryo(ctx).writeClassAndObject(output, msg)
    output.flush
    output.close
    super.encode(ctx, channel, os.buffer).asInstanceOf[ChannelBuffer]
  }
  
  def prependWithMappings(ctx :ChannelHandlerContext, channel :Channel, cb :ChannelBuffer) =  
    ChannelBuffers.wrappedBuffer(getMappingsBuffer(ctx, channel), cb)

  override def encode(ctx :ChannelHandlerContext, channel :Channel, msg :Object) = { 
    prependWithMappings(ctx, channel, encode2buffer(ctx, channel, msg))
  }
  
}
