const { MongoClient, ObjectID } = require('mongodb');
const { BigQuery } = require('@google-cloud/bigquery');

// Function to map MongoDB data types to BigQuery data types
function mapMongoToBigQueryType(mongoValue) {
  if (Array.isArray(mongoValue)) {
    // Handle arrays by converting them to JSON strings
    return 'STRING';
  } else if (mongoValue instanceof Date) {
    // Handle timestamps (Date objects) as TIMESTAMP type in BigQuery
    return 'TIMESTAMP';
  } else if (typeof mongoValue === 'number') {
    // Map JavaScript number to BigQuery FLOAT
    return 'FLOAT';
  } else if (typeof mongoValue === 'boolean') {
    // Map JavaScript boolean to BigQuery BOOLEAN
    return 'BOOLEAN';
  } else if (typeof mongoValue === 'string') {
    // Map JavaScript string to BigQuery STRING
    return 'STRING';
  } else if (typeof mongoValue === 'object' && mongoValue !== null) {
    // Handle nested objects (including _id) as JSON strings
    return 'STRING';
  } else {
    // For unknown types, map to BigQuery STRING (as a catch-all)
    return 'STRING';
  }
}

function createBatches(arr, batchSize) {
  const batches = [];
  for (let i = 0; i < arr.length; i += batchSize) {
    batches.push(arr.slice(i, i + batchSize));
  }
  return batches;
}

exports.migrateMongoToBigQuery = async (req, res) => {
  try {
    // Get MongoDB collection name and BigQuery table name from the request body
    const { mongoCollectionName, bigQueryTableName } = req.body;

    // Get MongoDB URI, database name, and BigQuery dataset ID from environment variables
    const mongoURI = process.env.MONGODB_URI;
    const dbName = process.env.MONGODB_DATABASE_NAME;
    const bigQueryDatasetId = process.env.BIGQUERY_DATASET_ID;
    const batchInsertSize = process.env.BATCH_INSERT_SIZE || 100;

    // Connect to MongoDB
    const mongoClient = await MongoClient.connect(mongoURI, {
      useNewUrlParser: true,
      useUnifiedTopology: true,
    });

    const db = mongoClient.db(dbName);
    const collection = db.collection(mongoCollectionName);

    // Fetch all documents from MongoDB collection
    const allDocuments = await collection.find({}).toArray();

    // Disconnect from MongoDB
    await mongoClient.close();

    // Collect all unique field names from all documents
    const uniqueFieldNames = new Set();
    allDocuments.forEach((document) => {
      Object.keys(document).forEach((fieldName) => uniqueFieldNames.add(fieldName));
    });

    // Generate BigQuery schema based on unique field names
    const bigQuerySchema = Array.from(uniqueFieldNames).map((fieldName) => {
      const sampleValue = allDocuments.find((document) => document.hasOwnProperty(fieldName))[fieldName];
      return {
        name: fieldName,
        type: mapMongoToBigQueryType(sampleValue),
      };
    });

    // Connect to BigQuery
    const bigquery = new BigQuery();

    // Delete the existing table if it exists
    const [tableExists] = await bigquery.dataset(bigQueryDatasetId).table(bigQueryTableName).exists();
    if (tableExists) {
      await bigquery.dataset(bigQueryDatasetId).table(bigQueryTableName).delete();
      console.log(`Deleted existing BigQuery table: ${bigQueryTableName}`);
    }

    // Prepare the data to be inserted into BigQuery
    const rows = allDocuments.map((document) => {
      // Convert MongoDB ObjectID to a string representation
      document._id = document._id.toString();

      // Convert nested objects to JSON strings
      Object.keys(document).forEach((fieldName) => {
        if (document[fieldName] instanceof Date) {
          document[fieldName] = Math.floor(document[fieldName].getTime() / 1000);
        }else if (typeof document[fieldName] === 'object' && document[fieldName] !== null) {
          document[fieldName] = JSON.stringify(document[fieldName]);
        }
      });

      return {
        insertId: document._id, // Use the original ObjectID as the insertId
        json: document,
      };
    });
    
    let batches = createBatches(rows, batchInsertSize);
    for await (let batch of batches){
      // Insert data into BigQuery in batches
      try{
        data = await bigquery.dataset(bigQueryDatasetId).table(bigQueryTableName).insert(batch, { raw: true, schema: bigQuerySchema })
        console.log('Data inserted into BigQuery:', data);
      }catch(err){
        // An API error or partial failure occurred.
        console.error("FAILED", err.message);
        if (err.name === 'PartialFailureError') {
          console.error("PARTIAL FAILURE ERROR");
          // Some rows failed to insert, while others may have succeeded.
          err.errors.forEach(e => {
            console.log(e.row);
            console.error("ERROR", e);
          });
        }
        throw err;
      }
    }
    res.status(200).send('Migration completed successfully.');
  } catch (error) {
    console.error('Error during migration:', error.message);
    res.status(500).send('An error occurred during migration.');
  }
};
