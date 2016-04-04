import org.apache.spark.{AccumulatorParam, SparkConf}
import org.apache.spark.serializer.JavaSerializer
import scala.collection.mutable.{ HashMap => MutableHashMap }
import org.apache.spark.graphx._
import com.metreta.spark.orientdb.connector._
import org.apache.spark._
import org.apache.spark.sql.hive._
import org.apache.spark.graphx._
import com.metreta.spark.orientdb.connector._
import com.metreta.spark.orientdb.connector._
import com.orientechnologies.orient.core.intent._
import com.tinkerpop.blueprints.impls.orient._
import scala.collection.JavaConversions._
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.hive.ql.plan.TezWork.VertexType
 
object test {

def  hash(userName : String) = { var h = 1125899906842597L;val len = userName.length();;for ( i <- 0 to len-1) { h = 31*h + userName.charAt(i);}; h}

case class graph_edge(from_user: String, to_user : String, no_h2h : Long, no_likes : Long, no_cbg:Long)

case class graph_user(from_user: String, os: String, dev_type : String, reg_time : Long,   no_h2h : Long, no_tl :Long)
val defaultUser = graph_user("dummy", "dummy", "dummy", 123l, 123l, 123l)
def main(args: Array[String]) {
val sc = new SparkContext(new SparkConf())
val sqlContext = new HiveContext(sc)


implicit object HashMapParam extends AccumulatorParam[MutableHashMap[Long, OrientVertex]] {

  def addAccumulator(acc: MutableHashMap[Long, OrientVertex], elem: (Long, OrientVertex)): MutableHashMap[Long, OrientVertex] = {
    acc += elem
    acc
  }

  /*
   * This method is allowed to modify and return the first value for efficiency.
   *
   * @see org.apache.spark.GrowableAccumulableParam.addInPlace(r1: R, r2: R): R
   */
  def addInPlace(acc1: MutableHashMap[Long, OrientVertex], acc2: MutableHashMap[Long, OrientVertex]): MutableHashMap[Long, OrientVertex] = {
    acc2.foreach(elem => addAccumulator(acc1, elem))
    acc1
  }

  /*
   * @see org.apache.spark.GrowableAccumulableParam.zero(initialValue: R): R
   */
  def zero(initialValue: MutableHashMap[Long, OrientVertex]): MutableHashMap[Long, OrientVertex] = {
    val ser = new JavaSerializer(new SparkConf(false)).newInstance()
    val copy = ser.deserialize[MutableHashMap[Long, OrientVertex]](ser.serialize(initialValue))
    copy.clear()
    copy
  }
}


val accum = sc.accumulator(MutableHashMap[Long, OrientVertex]())
accum.name

sqlContext.sql("use userdata").collect
val userRdd = sqlContext.sql("select *, reg_time as reg_time1 from (select from_user,  case when os is null then '' else os end, case when dev_type is null then '' else dev_type end, case when reg_time is null then 0 else unix_timestamp( reg_time) end as reg_time, case when no_h2h is null then 0 else no_h2h end, case when no_tl is null then 0 else no_tl end  from ps_graph_users1 where from_user is not NULL distribute by from_user)T1").map(x=>(graph_user(x.getString(0), x.getString(1), x.getString(2), x.getLong(6), x.getLong(4), x.getLong(5))))
val defaultUser = graph_user("dummy", "dummy", "dummy", 123l, 123l, 123l)
val rdd = sc.parallelize(Array(defaultUser))
userRdd.mapPartitionsWithIndex((i,x)=>{
  
  val urls = Array("remote:10.0.1.212/graph_poc")
val props = new java.util.HashMap[String,Any]();
val vertexToMap = new MutableHashMap[Long, OrientVertex]
// Open the OrientDB database instance
val factory = new OrientGraphFactory(urls(0), "root", "abc");
factory.declareIntent(new OIntentMassiveInsert());
val graph1 = factory.getNoTx();
props.+=(("from_user", "dummy"))
props.+=(("os", "dummy"))
props.+=(("dev_type", "dummy"))
props.+=(("reg_time", 123l))
props.+=(("no_h2h", 123l))
props.+=(("no_tl", 123l))
val srcVertex1 = graph1.addVertex("class:graph_user", props)
srcVertex1.detach()
vertexToMap +=  hash("dummy") -> srcVertex1

x.foreach ( vert=> {
  val srcKey = hash(vert.from_user)
props.+=(("from_user", vert.from_user))
props.+=(("os", vert.os))
props.+=(("dev_type", vert.dev_type))
props.+=(("reg_time", vert.reg_time))
props.+=(("no_h2h", vert.no_h2h))
props.+=(("no_tl", vert.no_tl))
val srcVertex1 = graph1.addVertex("class:graph_user", props);
srcVertex1.detach()
vertexToMap += srcKey -> srcVertex1
1
})
 accum+=vertexToMap
 x
}).count

val accumValue = sc.broadcast(accum.value)
//val factory = new OrientGraphFactory(urls(0), "root", "abc");
//   factory.declareIntent(new OIntentMassiveInsert());
//   val graph1 = factory.getNoTx();
//   val newMap = MutableHashMap[Long, OrientVertex]()
//for( x<- accum.value) yield {
//  val z = x._2.attach(graph1)
//  graph1.detach()
//  newMap += (x._1,
//}



 val edgeRdd = sqlContext.sql("select from_user, to_user, case when no_h2h is null then 0 else no_h2h end, case when no_likes is null then 0 else no_likes end, case when no_cbg is null then 0 else no_cbg end from ps_graph_edge3 where from_user is not NULL and to_user is not NULL cluster by from_user").map(x=> graph_edge(x.getString(0), x.getString(1), x.getLong(2), x.getLong(3), x.getLong(4)))
 
 edgeRdd.mapPartitionsWithIndex((i,x) => {
   println("starting")
   val default = accumValue.value.get(hash(defaultUser.from_user)).get
   val urls = Array("remote:10.0.1.212/graph_poc")
	 val factory = new OrientGraphFactory(urls(0), "root", "abc");
   factory.declareIntent(new OIntentMassiveInsert());
   val graph1 = factory.getNoTx();
   default.attach(graph1)
//   accumValue.value.values.foreach { _.attach(graph1) }
  x.foreach(  edge =>{  
    val from = accumValue.value.getOrElse(hash(edge.from_user), default)
    val to = accumValue.value.getOrElse(hash(edge.to_user), default)
    if(from == default || to == default) {
      println("skipping")
      0
    } else {
      var added = false
      while(!added) {
      var j = 0
      try {
      from.addEdge("graph_edge", to, "class:graph_edge", null, "from_user", edge.from_user,"to_user", edge.to_user, "no_h2h", java.lang.Long.valueOf(edge.no_h2h),"no_likes", java.lang.Long.valueOf(edge.no_likes),"no_cbg", java.lang.Long.valueOf(edge.no_cbg))
      added  =true
      println("passed time " + i)
      } catch {
        case e: Exception => added = false ; j+=1
      }
      }
      1
    } 
    
  })
  x
 }).count
 
}
}
//val graph = Graph(userRdd, edgeRdd, defaultUser)      
//val out = graph.triplets.repartition(1)
//
//case class graph_user_string(from_user : String)
//
//out.mapPartitions(x => {
//val props = new java.util.HashMap[String,Any]();
//val vertexToMap = new java.util.HashMap[graph_user_string, OrientVertex]
//// Open the OrientDB database instance
//val factory = new OrientGraphFactory("remote:10.0.1.212/graph_poc", "root", "abc");
//factory.declareIntent(new OIntentMassiveInsert());
//val graph1 = factory.getNoTx();
//val y = x.map ( vert=> {
//  val srcKey = graph_user_string(vert.srcAttr.from_user)
//  var srcVertex = vertexToMap.get(srcKey)
//if(srcVertex == null) {
//props.+=(("from_user", vert.srcAttr.from_user))
//props.+=(("os", vert.srcAttr.os))
//props.+=(("dev_type", vert.srcAttr.dev_type))
//props.+=(("reg_time", vert.srcAttr.reg_time))
//props.+=(("no_h2h", vert.srcAttr.no_h2h))
//props.+=(("no_tl", vert.srcAttr.no_tl))
//srcVertex = graph1.addVertex("class:graph_user", props); 
//vertexToMap.+=((srcKey, srcVertex))
//}
//
//  val dstKey = graph_user_string(vert.dstAttr.from_user)
//var dstVertex = vertexToMap.get(dstKey) 
//if(dstVertex == null) {
//props.+=(("from_user", vert.dstAttr.from_user))
//props.+=(("os", vert.dstAttr.os))
//props.+=(("dev_type", vert.dstAttr.dev_type))
//props.+=(("reg_time", vert.dstAttr.reg_time))
//props.+=(("no_h2h", vert.dstAttr.no_h2h))
//props.+=(("no_tl", vert.dstAttr.no_tl))
//dstVertex = graph1.addVertex("class:graph_user", props);
//vertexToMap.+=((dstKey, dstVertex))
//}
//val e1 = srcVertex.addEdge("graph_edge", dstVertex, "class:graph_edge", null, "from_user", vert.attr.from_user,"to_user", vert.attr.to_user, "no_h2h", java.lang.Long.valueOf(vert.attr.no_h2h),"no_likes", java.lang.Long.valueOf(vert.attr.no_likes),"no_cbg", java.lang.Long.valueOf(vert.attr.no_cbg))
//1
// })
// y
// }).collect
//
//}
// 
//} 


/*
 * Allows a mutable HashMap[String, Int] to be used as an accumulator in Spark.
 * Whenever we try to put (k, v2) into an accumulator that already contains (k, v1), the result
 * will be a HashMap containing (k, v1 + v2).
 *
 * Would have been nice to extend GrowableAccumulableParam instead of redefining everything, but it's
 * private to the spark package.
 */
 
 
/**
 * Run the following commands on orient before executing this:
 CREATE CLASS graph_user EXTENDS V
 CREATE PROPERTY graph_user.from_user string
 CREATE PROPERTY graph_user.os string
 CREATE PROPERTY graph_user.dev_type string
 CREATE PROPERTY graph_user.reg_time long
 CREATE PROPERTY graph_user.no_h2h long
 CREATE PROPERTY graph_user.no_tl long

 CREATE CLASS graph_edge EXTENDS E
 CREATE PROPERTY graph_edge.from_user String
 CREATE PROPERTY graph_edge.to_user string
 CREATE PROPERTY graph_edge.no_h2h long
 CREATE PROPERTY graph_edge.no_likes long
 CREATE PROPERTY graph_edge.no_cbg long
 * 
 */


 

 

