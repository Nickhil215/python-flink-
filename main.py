import argparse
import logging
import sys
from pyflink.common import WatermarkStrategy, Encoder, Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors import (FileSource, StreamFormat, FileSink, OutputFileConfig, RollingPolicy)
from langchain_text_splitters import NLTKTextSplitter
from chromadb.utils import embedding_functions
import chromadb
import uuid
import requests
import tempfile
import json

from pymongo import MongoClient

from mysql.connector.cursor import MySQLCursor
from pyflink.datastream import FilterFunction

import socket
    
                
from mysql.connector.pooling import MySQLConnectionPool
import logging

import os

import nltk
nltk.download('punkt_tab')

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
from mysql.connector import pooling, MySQLConnection

    
class RemoveNoneFilter(FilterFunction):
    def filter(self, value):
        return value is not None
    

text_splitter = NLTKTextSplitter(chunk_size=1000, chunk_overlap=100)
    

def bulk_create_player(cursor: MySQLCursor, entity: list[tuple]) -> None:
    print(f"list of  tuples :: {entity}")
    cursor.executemany("insert into group_metadata(time, groupId, entityId, schemaId, tenantId) VALUES (NOW(),%s,%s,%s,%s)", entity)
              


def tidb_insertion(value):
    # Define the connection pool parameters
    pool_name = 'mypool'
    pool_size = 5
    autocommit = True
    
    # Create a connection pool
    db_conf = {
        "host": 'localhost',
        "port": '4000',
        "user": 'root',
        # "password": 'GaianMobius',
        "database": 'targettingframework',
        "autocommit": autocommit,
        "use_pure": True,
        "pool_name": pool_name,
        "pool_size": pool_size
    }
    
    try:
        # Initialize the connection pool
        connection_pool = MySQLConnectionPool(**db_conf)
        logging.info("Connection pool created successfully.")
    except Exception as e:
        logging.error("Error creating connection pool: %s", e)
        raise

    # Extract the required values from the input
    entity_ids = value['entity_ids']
    schema_id = value['schema_id']
    tenant_id = value['tenant_id']
    group_id = value['group_id']

    connection = None
    try:
        # Get a connection from the pool
        connection = connection_pool.get_connection()
        if not connection.is_connected():
            connection.reconnect(attempts=3, delay=2)
        parameters_list = []
        # Perform database operations
        with connection.cursor() as cur:
            for entity_id in entity_ids:
                parameters = (group_id, entity_id, schema_id, tenant_id)
                parameters_list.append(parameters)

            # Assuming bulk_create_player is defined elsewhere
            bulk_create_player(cur, parameters_list)
            
            
            connection.commit()
            logging.info("Data inserted successfully.")
            
        yield parameters_list

    except Exception as e:
        logging.error("Error in TiDB insertion: %s", e)
        if connection is not None:
            connection.rollback()
        raise
    finally:
        if connection is not None:
            connection.close()

                
def group_query_fetch(group_model):
    """
    Extracts raw query and metadata from a group model.

    Args:
    group_model (dict): The group model to extract data from.

    Returns:
    dict or None: A dictionary containing the raw query and metadata, or None if the group model is invalid.
    """
    # try:
    # Define the required keys in the group model
    required_keys = ['definitionRequest', 'rawQueryMap', '_id', 'schemaId', 'tenantID']

        # Check if all required keys exist in the group model
    if not all(key in group_model for key in required_keys):
        logging.error(f"Invalid groupModel: Missing required keys - {group_model}")
        yield None

        # Extract raw query for the primary DB (e.g., CHROMA)
    raw_query_map = group_model['definitionRequest'].get('rawQueryMap', {})
    raw_query = raw_query_map.get('CHROMA')

        # Check if raw query exists
    if not raw_query:
        logging.error(f"Missing raw query for CHROMA in groupModel: {group_model}")
        yield None

        # Extract additional metadata
    group_id = str(group_model['_id'])  # Convert ObjectId to string
    schema_id = group_model['schemaId']
    tenant_id = group_model['tenantID']

        # Return the extracted data
    result = {
            "raw_query": raw_query,
            "group_id": group_id,
            "schema_id": schema_id,
            "tenant_id": tenant_id
        }
    logging.info(f"data ***** {result}")
    yield result
    # except Exception as e:
    #     logging.error(f"Error in getting group model: {str(e)}")
    #     return None    
    
def get_groupModel(schemaId):
    try:
        mongo_user = 'gaian'
        mongo_password = 'GaianMobius'
        # mongo_host = 'percona-mongodb-db-ps-rs0.percona.svc.cluster.local'
        mongo_host = '10.42.180.194'
        mongo_db_name = 'TargettingFramework'
        # connection_string = f"mongodb://{mongo_user}:{mongo_password}@{mongo_host}"
        connection_string = 'mongodb://localhost:27017'
        client = MongoClient(connection_string)
        db = client[mongo_db_name]
        collection = db["groupModel"]
        
        query = {
            "active": True,
            "draft": False,
            "offline": False,
            "schemaId": schemaId  # Assuming collection_name corresponds to SCHEMA_ID
        }        
        data = collection.find(query)
        result = list(data)
        print(f"groups Count :: {len(result)}")  # Using f-string for better readability
        return result
    except Exception as e:
        logging.error(f"Error in getting group model: {str(e)}")
        return None


def generate_embedding(value):
        try: 
            openai_ef =embedding_functions.OpenAIEmbeddingFunction(
            model_name='text-embedding-ada-002',
            api_key = os.getenv('OPENAI_API_KEY')

            )

            embedding = openai_ef([value])[0]
            metadata = {
                "doc_id": "John Doe", 
                "version": "1.0"
            }
            result = {
                    "value": value,
                    "embedding": embedding,
                    "id": str(uuid.uuid4()),
                    "metadata": metadata
                }
            logging.info(f"Generated embedding: {result}")
            yield result
        except Exception as e:
            logging.error(f"Error generating embedding: {str(e)}")
            return

def generate_query_embedding(value):
        try: 
            openai_ef =embedding_functions.OpenAIEmbeddingFunction(
            model_name='text-embedding-ada-002',
            api_key = os.getenv('OPENAI_API_KEY')
            )

            embedding = openai_ef([value.get("raw_query")])[0]
            # metadata = {
            #     "doc_id": "John Doe", 
            #     "version": "1.0"
            # }
            metadata = os.getenv('METADATA_JSON')
            result = {
                    "embedding": embedding,
                    "value": value.get("raw_query"),
                    "metadata": metadata ,
                    "schema_id":value.get("schema_id"),
                    "tenant_id":value.get("tenant_id"),
                    "group_id":value.get("group_id")
                    }
            logging.info(f"Generated query embedding: {result}")
            yield result
        except Exception as e:
            logging.error(f"Error generating embedding: {str(e)}")
            return    


def download_file(url):
    """Download file from URL and save it to a temporary location."""
    response = requests.get(url)
    response.raise_for_status()
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".txt")
    with open(temp_file.name, 'wb') as file:
        file.write(response.content)
    return temp_file.name



def getDataStream(url):
    documents = []
    response = requests.get(url)
    print("------------------------ ----")
    # print(response.text)
    print("----------lines------------------")
    lines = response.text
    # print(lines)
    documents.append(lines)
    count = len(documents)
    # print("Count of records in documents:", count)
    print("%%%%%%%%%% print data stream %%%%%%%%%%%%")
    return documents

CHROMA_HOST = "localhost"
CHROMA_PORT = 33651
CHROMA_AUTH = "f8y9cfFtBFGGYYIjDAfdeGZdI0ptSN1U"
Auth_token = f"Bearer {CHROMA_AUTH}"
chroma_client = chromadb.HttpClient(
            host=CHROMA_HOST,
            port=CHROMA_PORT,
            headers={"Authorization": Auth_token}
        )

def insert_chromaDd(value):
    """Insert a value into the ChromaBD database."""
   
    collection = chroma_client.get_or_create_collection(name=os.getenv('COLLECTION_NAME'))
    collection.add(
            embeddings=[value.get('embedding')],
            documents=[value.get('value')],
            metadatas=[value.get('metadata')],
            ids=[value.get('id')]
        )
    yield value


def query_op(value):
    query_embeddings = value.get('embedding', None)
    print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
    metadata_where_clause = {"doc_id" :value.get('metadata', None)["doc_id"]}
    print(metadata_where_clause)
    collection = chroma_client.get_or_create_collection(name=os.getenv('COLLECTION_NAME'))

    results = collection.query(
    query_embeddings=query_embeddings,
    n_results=10,
    where=metadata_where_clause
    )
    # print(results)
    ids = results.get('ids', [[]])[0]
    results_data = {
        "entity_ids":ids,
        "schema_id": value.get("schema_id"),
        "tenant_id": value.get("tenant_id"),
        "group_id": value.get("group_id")
        }
    yield results_data
    

def split(text):
    try:
        return text_splitter.split_text(text)
    except Exception as e:
        logging.error(f"Error in text splitting: {str(e)}")
        # return []
        
        



def chromadb_pipeline(input_path, metadata, collection_name, openapi_key, model):

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.BATCH)
    env.set_parallelism(1)


    documents = getDataStream(input_path)
    ds = env.from_collection(documents)

    chunk_ds = ds.flat_map(split)
    chunk_ds = chunk_ds.flat_map(generate_embedding)
    chunk_ds.flat_map(insert_chromaDd)
    
    groupModels = get_groupModel(collection_name)
    print(f"groups count :: {len(groupModels)}")
    

    G_Stream = env.from_collection(groupModels)

    G_query_stream = G_Stream.flat_map(group_query_fetch)\
        .filter(lambda x: x is not None)\

    G_query_embedding_stream = G_query_stream.flat_map(generate_query_embedding)

    Chroma_data_stream = G_query_embedding_stream.flat_map(query_op)
    

    kafka_sink_data = Chroma_data_stream.flat_map(tidb_insertion)
    
    
    
    # G_query_stream.print()
    
    
    
    env.execute("ChromaDB Pipeline")


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    parser = argparse.ArgumentParser()
    parser.add_argument('--input', dest='input', required=True, help='Input file to process.')
    parser.add_argument('--collection', dest='collection', required=True, help='ChromaDB collection name.')
    parser.add_argument('--key', dest='key', required=True, help='OpenAI API key.')
    parser.add_argument('--model', dest='model', required=True, help='OpenAI model name.')
    parser.add_argument('--metadata', dest='metadata', required=True, help='Metadata for processing (as JSON string).')

    args = parser.parse_args()

    try:
        metadata = json.loads(args.metadata)
    except json.JSONDecodeError as e:
        logging.error(f"Invalid metadata format: {e}")
        sys.exit(1)
    # Set environment variables
    os.environ['INPUT_FILE'] = args.input
    os.environ['COLLECTION_NAME'] = args.collection
    os.environ['OPENAI_API_KEY'] = args.key
    os.environ['OPENAI_MODEL_NAME'] = args.model
    os.environ['METADATA_JSON'] = args.metadata

    # Confirm environment variables are set
    logging.info("Environment variables set successfully:")
    logging.info(f"INPUT_FILE={os.getenv('INPUT_FILE')}")
    logging.info(f"COLLECTION_NAME={os.getenv('COLLECTION_NAME')}")
    logging.info(f"OPENAI_API_KEY={os.getenv('OPENAI_API_KEY')}")
    logging.info(f"OPENAI_MODEL_NAME={os.getenv('OPENAI_MODEL_NAME')}")
    logging.info(f"METADATA_JSON={os.getenv('METADATA_JSON')}")

    # chromadb_pipeline(
    #     input_path=args.input,
    #     metadata=metadata,
    #     collection_name=args.collection,
    #     openapi_key=args.key,
    #     model=args.model
    # )





















