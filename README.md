# About

This contains code for a cloud-function to migrate data from mongodb to bigquery, can also be used locally too. 

This script auto-creates the bigquery table by creating schema after going through all your documents in the collection, the schema includes fields that are optional and aren't present in all the documents. (Which is why I created this)


# Config

There are three environment variables needed for this. 

`MONGODB_URI` - the uri needed by the function to connect to your mongodb 
`MONGODB_DATABASE_NAME` - the name of the database in which your collection resides 
`BIGQUERY_DATASET_ID` - the dataset in which the bigquery table should be created

The name of the collection and the bigquery table name are taken in POST request body. 

`{  "mongoCollectionName": "Company",  "bigQueryTableName": "Company" }'`

Here `mongoCollectionName` is the name of the collection to migrate in mongodb
and `bigQueryTableName` is the name of the BQ table to be created. 


# CAVEATS 
ARRAY and OBJECTS are right now migrated as `STRINGS`, feel free to open a PR to support migration for them. 
