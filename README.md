d-sparq
=======

A distributed, scalable and efficient RDF query engine.

## Dependencies

Following software are required in order to run d-sparq.

1. MongoDB (http://www.mongodb.org)
2. Java 1.6 or later (http://www.oracle.com/technetwork/java/index.html)
3. Hadoop 1.0.3 (http://hadoop.apache.org)
4. ant (http://ant.apache.org)

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

##### Generate input files for Metis

1. 



