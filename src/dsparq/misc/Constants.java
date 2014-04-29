package dsparq.misc;

public interface Constants {

	public static final String EDGE_LIST = "edge-list.dat";
	public static final String ADJ_LIST = "adj-list-metis";
	public static final String SER_BI_MAP = "id-bi-map.dat";
	public static final String KV_FORMAT_FILE = "kv-format-file";
	public static final String MONGO_LOCAL_HOST = "localhost";
	public static final int MONGO_PORT = 27017;
	public static final String MONGO_DB = "dbpedia";
	public static final String MONGO_TRIPLE_COLLECTION = "triples";
	public static final String MONGO_IDVAL_COLLECTION = "idvals";
	public static final String MONGO_EIDVAL_COLLECTION = "eidvals";
	public static final String MONGO_GRAPHINFO_COLLECTION = "graphinfo";
	public static final String MONGO_VERTEX_PARTITIONID_COLLECTION = "vpid";
	public static final String MONGO_PARTITIONED_VERTEX_COLLECTION = "ptriples";
	public static final String MONGO_STAR_SCHEMA = "starschema";
	public static final String MONGO_PREDICATE_SELECTIVITY = "pselectivity";
	public static final String MONGO_SEQID_COLLECTION = "seqcol";
	public static final String MONGO_WRAPPED_ID_COLLECTION = "wrapid";
	
	public static final String MONGO_CONFIG_ADD_SHARD = "addshard";
	public static final String MONGO_ENABLE_SHARDING = "enablesharding";
	public static final String MONGO_SHARD_COLLECTION = "shardcollection";
	public static final String MONGO_RDF_DB = "rdfdb";
	public static final String MONGO_ADMIN_DB = "admin";
	
	public static final String TOTAL_VERTICES = "total_vertices";
	public static final String TOTAL_DEGREE = "total_degree";
	public static final String STD_DEV_DEGREE = "std_dev_degree";
	public static final String DEGREE_THRESHOLD = "degree_threshold";
	public static final String HIGH_DEGREE_TAG = "highDegree";
	public static final String LOW_DEGREE_TAG = "lowDegree";
	public static final String HIGH_DEGREE_TYPE = "highDegreeType";
	
	// used for generating integer IDs for subject/predicate/object
	public static final long START_ID = 1;
	// for predicates, a 'p' prefix is used along with integer ID.
	// 1-9 are left for special types like "rdf:type"
	public static final long START_PREDICATE_ID = 10;
	public static final long START_TYPE_ID = 0;
	
	public static final int POSITION_SUBJECT = 0;
	public static final int POSITION_PREDICATE = 1;
	public static final int POSITION_OBJECT = 2;
	public static final int POSITION_ISLITERAL = 3;
	public static final int POSITION_NEIGHBOUR = 3;	
	public static final String PREDICATE_TYPE = "2";
	public static final long PREDICATE_TYPE_LONG = 2;
	
	//subject is unique to a document in star schema, so using it as _id
	public static final String FIELD_TRIPLE_SUBJECT = "_id";
	public static final String FIELD_TRIPLE_PREDICATE = "predicate";
	public static final String FIELD_TRIPLE_OBJECT = "object";
	public static final String FIELD_TRIPLE_PRED_OBJ = "predobj";
	
	//msg digest is unique, so using it as _id
	public static final String FIELD_HASH_VALUE = "_id";
	public static final String FIELD_ID = "id";
	public static final String FIELD_STR_VALUE = "sval";
	public static final String FIELD_WID = "wid";
	public static final String FIELD_TYPEID = "tid";	
	public static final String FIELD_VERTEX_ID = "vertexID";
	public static final String FIELD_PARTITION_ID = "partitionID";
	
	public static final String FIELD_SEQID = "seqid";
	public static final String FIELD_PRED_SEQID = "pseqid";
	public static final String FIELD_TYPE_SEQID = "tseqid";
	
	public static final String FIELD_PRED_SELECTIVITY = "count";
	
	public static final String TRIPLE_TERM_DELIMITER = "|";	
	public static final String REGEX_DELIMITER = "\\|";
	
	public static final String RDF_TYPE_URI = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
	public static final String BASE_URI = "http://dummy.base.org";
	
	public static final String NT_FILE_TYPE_TAG = "nt";
	public static final String MONGO_FILE_TYPE_TAG = "mongo";
	
	public static final String MONGO_UNIQUE_INDEX = "unique";
	
	public final int CONTAINER_CAPACITY = 12000;
	
	// used while creating a Jedis instance
	public final int INFINITE_TIMEOUT = 60*60*60*60*60*1000;
}
