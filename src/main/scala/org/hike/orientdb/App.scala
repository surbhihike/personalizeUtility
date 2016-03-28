
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
 
object test {

def  hash(userName : String) = { var h = 1125899906842597L;val len = userName.length();;for ( i <- 0 to len-1) { h = 31*h + userName.charAt(i);}; h}

case class graph_edge(from_user: String, to_user : String, no_h2h : Long, no_likes : Long, no_cbg:Long)

case class graph_user(from_user: String, os: String, dev_type : String, reg_time : Long,   no_h2h : Long, no_tl :Long)
val defaultUser = graph_user("dummy", "dummy", "dummy", 123l, 123l, 123l)
def main(args: Array[String]) {
val sc = new SparkContext(new SparkConf())
val sqlContext = new HiveContext(sc)

sqlContext.sql("use userdata").collect
val userRdd = sqlContext.sql("select *, unix_timestamp(reg_time) as reg_time1 from ps_graph_users1 where from_user is not NULL and os is not NULL and dev_type is not NULL and reg_time is not NULL and no_h2h is not NULL and no_tl is not NULL distribute by from_user").map(x=>(hash(x.getString(0) ), graph_user(x.getString(0), x.getString(1), x.getString(2), x.getLong(6), x.getLong(4), x.getLong(5))))
val defaultUser = graph_user("dummy", "dummy", "dummy", 123l, 123l, 123l)
//val userRdd = sc.parallelize(user.map(x=> (hash(x.from_user), x)))
val edge = Array(graph_edge("pankaj", "surbhi", 4l, 5l, 2l), graph_edge("surbhi", "jyoti", 4l, 5l, 2l))


val edgeRdd = sqlContext.sql("select * from ps_graph_edge3 where from_user is not NULL and to_user is not NULL and no_h2h is not NULL and no_likes is not NULL and no_cbg is not NULL").map(x=> Edge(hash(x.getString(0)),hash(x.getString(1)), graph_edge(x.getString(0), x.getString(1), x.getLong(2), x.getLong(3), x.getLong(4))))
//val edgeRdd = sc.parallelize(edge.map(x=> Edge(hash(x.from_user), hash(x.to_user), x)))
val graph = Graph(userRdd, edgeRdd, defaultUser)

//graph.saveGraphToOrient
 
 
  
  
graph.triplets.repartition(1).mapPartitions(x => {
val props = new java.util.HashMap[String,Any]();
val edgeprops = new java.util.HashMap[String,Any]();
val vertexToMap = new java.util.HashMap[graph_user, Vertex]
// Open the OrientDB database instance
val factory = new OrientGraphFactory("remote:10.0.1.212/graph_poc", "root", "abc");
factory.declareIntent(new OIntentMassiveInsert());
val graph1 = factory.getNoTx();
val y = x.map { vert=>
if(vertexToMap.get(vert.srcAttr) == null) {
props.+=(("from_user", vert.srcAttr.from_user))
props.+=(("os", vert.srcAttr.os))
props.+=(("dev_type", vert.srcAttr.dev_type))
props.+=(("reg_time", vert.srcAttr.reg_time))
props.+=(("no_h2h", vert.srcAttr.no_h2h))
props.+=(("no_tl", vert.srcAttr.no_tl))
val v = graph1.addVertex("class:graph_user", props); 
println(v.getId)
vertexToMap.+=((vert.srcAttr, v))
}

if(vertexToMap.get(vert.dstAttr) == null) {
props.+=(("from_user", vert.dstAttr.from_user))
props.+=(("os", vert.dstAttr.os))
props.+=(("dev_type", vert.dstAttr.dev_type))
props.+=(("reg_time", vert.dstAttr.reg_time))
props.+=(("no_h2h", vert.dstAttr.no_h2h))
props.+=(("no_tl", vert.dstAttr.no_tl))
val v = graph1.addVertex("class:graph_user", props); 
vertexToMap.+=((vert.dstAttr, v))
}

val e = graph1.addEdge("class:graph_edge", vertexToMap.get(vert.srcAttr), vertexToMap.get(vert.dstAttr), "graph_edge")
e.setProperty("from_user", vert.attr.from_user)
e.setProperty("to_user", vert.attr.to_user)
e.setProperty("no_h2h", vert.attr.no_h2h)
e.setProperty("no_likes", vert.attr.no_likes)
e.setProperty("no_cbg", vert.attr.no_cbg)
1
 }
 y
 }).collect


//sqlContext.sql("use userdata").collect
//var userRdd = sqlContext.sql("select *, unix_timestamp(reg_time) as reg_time1 from ps_graph_users1 where from_user is not NULL and os is not NULL and dev_type is not NULL and reg_time is not NULL and no_h2h is not NULL and no_tl is not NULL").map(x=>(hash(x.getString(0)), graph_user(x.getString(0), x.getString(1), x.getString(2), x.getLong(6), x.getLong(4), x.getLong(5))))
//val user = Array(graph_user("pankaj", "android", "google", 121212121l, 3l, 2l), graph_user("surbhi", "android", "google", 121212121l, 3l, 2l), graph_user( "jyoti", "android", "google", 121212121l, 3l, 2l))
//userRdd = userRdd.repartition(500)
////val userRdd = sc.parallelize(user.map(x=> (hash(x.from_user), x)))
//val edge = Array(graph_edge("pankaj", "surbhi", 4l, 5l, 2l), graph_edge("surbhi", "jyoti", 4l, 5l, 2l))
//var edgeRdd = sqlContext.sql("select * from ps_graph_edge3 where from_user is not NULL and to_user is not NULL and no_h2h is not NULL and no_likes is not NULL and no_cbg is not NULL").map(x=> Edge(hash(x.getString(0)),hash(x.getString(1)), graph_edge(x.getString(0), x.getString(1), x.getLong(2), x.getLong(3), x.getLong(4))))
////val edgeRdd = sc.parallelize(edge.map(x=> Edge(hash(x.from_user), hash(x.to_user), x)))
//val graph = Graph(userRdd, edgeRdd, defaultUser)
//edgeRdd = edgeRdd.repartition(500)
// graph.saveGraphToOrient
}
 
} 
 
 
