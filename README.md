d-sparq
=======

A distributed, scalable and efficient RDF query engine.

## Dependencies

Following software are required in order to run d-sparq.

1. MongoDB (http://www.mongodb.org)
2. Java 1.6 or later (http://www.oracle.com/technetwork/java/index.html)
3. Hadoop 1.0.3 (http://hadoop.apache.org)
4. ant (http://ant.apache.org)
5. Metis (http://glaros.dtc.umn.edu/gkhome/metis/metis/download). If a distributed graph partitioner can
be used then it is better. But right now, did not find any freely available distributed graph partitioner.

Please download and install all of them. MongoDB needs to be installed on all the machines in the cluster that you plan to make use of. 
Add the executables to PATH environment variable.


## Instructions 

1. Download the source code and compile using ```ant jar```.

##### Start MongoDB Sharded Cluster

1. Instructions are given at http://docs.mongodb.org/manual/tutorial/deploy-shard-cluster.  
Note that if the underlying machine architecture is **NUMA**, then follow the instructions at 
http://docs.mongodb.org/manual/administration/production-notes/#mongodb-and-numa-hardware for starting
MongoDB. Instructions are given here for convenience.
  1. Check whether the file ```zone_reclaim_mode``` at ```/proc/sys/vm/zone_reclaim_mode``` has content
	as 0. If not use the following ```echo 0 > /proc/sys/vm/zone_reclaim_mode```.
  2. Use ```numactl``` while starting MongoDB. ```numactl --interleave=all bin/mongod <mongo_params>```.
  
The steps to set up sharded cluster are given here for convenience. For the following commands, create appropriate folder structure for dbpath.
  1. Start the config server database instances. One instance of this is minimally sufficient. Use ```numactl --interleave=all bin/mongod --configsvr --dbpath db/rdfdb/configdb --port 20000 > logs/configdb.log &```. 
  2. Start the mongos instances. One instance of this is minimally sufficient. Use ```numactl --interleave=all bin/mongos --configdb <config_server_host>:20000 > logs/mongos.log &```.
  3. Start the shards in the cluster. Do this on all the nodes in the cluster. Use ```numactl --interleave=all bin/mongod --shardsvr --dbpath db/rdfdb --port 10000 > logs/shard.log &```.
  4. Add shards to the cluster. Using mongo shell and connect to mongos instance. At the prompt run the
  following commands: a) ```use admin``` b) ```db.runCommand( { addshard : "<shard_host>:<shard_port>" } );``` Run
  this for all shards i.e., put in the information for all the shards. c) Enable sharding for the 
  database (rdfdb) as well as the collection (idvals) to be sharded. Use the commands, 
  ```db.runCommand( { enablesharding : "rdfdb" } );``` and 
  ```db.runCommand( { shardcollection : "rdfdb.idvals", key : { _id : 1 }, unique : true });```
  
##### Configure d-sparq and format input  
  
1. In ShardInfo.properties, make the necessary changes i.e., put the information regarding MongoDB cluster.
2. The input triples should be in N-Triples format. If not, RDF2RDF (http://www.l3s.de/~minack/rdf2rdf) 
can be used to convert the triples into N-Triples format.

##### Encoding Triples

1. Create hash digest message for each term (subject/predicate/object). This is required because some 
terms are very long (eg., blog comments) and it is not convenient to index on long texts. 
Use ```hadoop jar dist/d-sparq.jar dsparq.load.HashGeneratorMR <input_dir> <output_dir>```. Input is 
the directory containing the triples.
2. Load the digest messages along with its string equivalent values into MongoDB. Copy the output of 
previous step from HDFS to local file system or to the one hosting Mongo router. Use 
```java -Xms12g -Xmx12g -cp dist/d-sparq.jar dsparq.load.HashDigestLoader <input_dir>```. Input 
directory is the one containing the output of previous step.
3. A count of total documents on each shard is required for the next step. 
Use ```java -Xms12g -Xmx12g -cp dist/d-sparq.jar dsparq.load.NumericIDPreprocessor```.
4. Generate numerical IDs for the digest messages generated in the previous step. The numerical IDs 
are required for Metis (vertex IDs). Use ```java -Xms12g -Xmx12g -cp dist/d-sparq.jar dsparq.load.IDGenerator```.
Run this on each shard of the cluster. Running it simultaneously on all the shards would save time.
5. Generate triple file(s) with numerical IDs. After this step, triples file in the form of 
SubjectID|PredicateID|ObjectID would be generated. Use ```hadoop jar dist/d-sparq.jar dsparq.load.KVFileCreatorMR <input_dir> <output_dir>```.
Input directory is the one containing original triple files.

##### In case of only 1 node

1. Use ```java -Xms12g -Xmx12g -cp dist/d-sparq.jar dsparq.load.SplitTripleLoader <dir_with_KV_files>```.
The input is the directory containing the triple file(s) in Key-Value format generated as the output 
of previous step. Note that this input directory should not contain any other files or sub-directories. 
Triples are loaded into the local DB. Indexes are also created in this step. 
2. Generate predicate selectivity. Use ```java -Xms12g -Xmx12g -cp dist/d-sparq.jar dsparq.load.PredicateSelectivity```.
3. Go to the section on running queries as a next step.

##### Separate rdf:type triples

1. Use ```hadoop jar dist/d-sparq.jar dsparq.partitioning.GetTypeTriples <input_dir> <output_dir>```. 
Ignore this step if there is only 1 node in the cluster.

##### Generate input file for Metis

1. Metis needs the graph to be in the form of an adjacency list. So generate it using ```hadoop jar dist/d-sparq.jar dsparq.partitioning.UndirectedGraphPartitioner <input_dir> <output_dir>```.
Copy the output to local file system.
2. Total number of vertices and edges should also be specified. Get them using ```java -Xms12g -Xmx12g -cp dist/d-sparq.jar dsparq.partitioning.format.GraphStats adjvertex-r-00000-new```.
3. Add the total number of vertices and edges to the METIS input file (adjvertex-...) as its first line.
4. Run Metis using ```gpmetis <adjvertex_file> <num_partitions>```. Number of partitions is same as
number of nodes in the cluster.

##### Load the triples into respective partitions

1. The output of Metis specifies the vertices which belong to a particular partition. From that, we 
need to get the triples associated with those vertices. Following steps are required to get all the
triples which should go into a particular partition.
2. Combine VertexID file and partitionID file. Use ```ant partition-format -Dvid=vertex-r-00000 -Dpid=adjvertex-r-00000.part.<num>```.
3. Based on the vertexID - partitionID pairs, create separate files for each partition which contain 
all vertices belonging to that partition. Use ```hadoop jar dist/d-sparq.jar dsparq.partitioning.TripleSplitter <input_dir> <output_dir>```.
Input directory contains the combined vertexID - partitionID pairs.
4. For the triples on vertex partitions, use n-hop duplication.
5. Based on the output of above step, get the triples associated with each partition. Use ```hadoop jar dist/d-sparq.jar dsparq.partitioning.PartitionedTripleSeparator <input_dir> <output_dir>```.
6. Copy the triple files which belong to a particular partition on to that partition. 
7. Load the triples into local MongoDB and as well as into RDF-3X on each partition. Use ```java -Xms12g -Xmx12g -cp dist/d-sparq.jar dsparq.load.PartitionedTripleLoader <input_triple_file>```.
Indexes are also created in this step (PartitionedTripleLoader does that). 
8. Generate predicate selectivity. Use ```java -Xms12g -Xmx12g -cp dist/d-sparq.jar dsparq.load.PredicateSelectivity```.
9. For loading to RDF-3X, use ```time bin/rdf3xload rdfdb <file>```.

##### Run the queries

1. Run this on each node of the cluster. Use ```java -Xms12g -Xmx12g -cp dist/d-sparq.jar dsparq.query.opt.ThreadedQueryExecutor2 <query_file> <number_of_times_to_run>```.
The input query file should contain only one query. Query should be written without using any line breaks.
SP2 benchmark tool is used to generate data and the modified queries are also provided. Since only 
basic graph patterns are supported, some of the queries from SP2 are modified. Note that, only the 
total results are printed, not the individual results. This is sufficient for our purpose.
2. For running queries on RDF-3X, 
  1. put rdf3x-0.3.7/bin in the PATH variable. 
  2. Use ```time java -Xms12g -Xms12g -cp dist/d-sparq.jar:src dsparq.sample.RDF3XTest <path_to_rdfdb> <query_file>```.




