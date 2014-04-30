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

1. Download the source code and compile using the command, ```ant jar```.
2. Start a MongoDB sharded cluster (http://docs.mongodb.org/manual/tutorial/deploy-shard-cluster).
3. Make rdfdb as a sharded database and idvals and a sharded collection. Instructions on how to do this
are sharded cluster docs of MongoDB. 
4. In ShardInfo.properties, make the necessary changes i.e., put the information regarding cluster and MongoDB.
5. The input triples should be in N-Triples format. If not, RDF2RDF (http://www.l3s.de/~minack/rdf2rdf) 
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
3. Generate numerical IDs for the digest mesages generated in the previous step. The numerical IDs 
are required for Metis (vertex IDs). Use ```java -Xms12g -Xmx12g -cp dist/d-sparq.jar dsparq.load.IDGenerator```.
Run this on each shard of the cluster. Running it simultaneously on all the shards would save time.
4. Generate triple file(s) with numerical IDs. After this step, triples file in the form of 
SubjectID|PredicateID|ObjectID would be generated. Use ```hadoop jar dist/d-sparq.jar dsparq.load.KVFileCreatorMR <input_dir> <output_dir>```.
Input directory is the one containing original triple files.

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
Indexes are also created in this step (PartitionedTripleLoader does that). For loading to RDF-3X, use
```time bin/rdf3xload rdfdb <file>```.

##### Run the queries

1. Run this on each node of the cluster. Use ```java -Xms12g -Xmx12g -cp dist/d-sparq.jar dsparq.query.opt.ThreadedQueryExecutor2 <query_file> <number_of_times_to_run>```.
The input query file should contain only one query. Query should be written without using any line breaks.
SP2 benchmark tool is used to generate data and the modified queries are also provided. Since only 
basic graph patterns are supported, some of the queries from SP2 are modified.
2. For running queries in RDF-3X, use ```time bin/rdf3xquery rdfdb <query_file>```.



