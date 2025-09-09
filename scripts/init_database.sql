/*
This script creates a new database and schemas. Existing DB won't be dropped.
 */

-- Create the "data_warehouse" database
CREATE DATABASE IF NOT EXISTS data_warehouse;

-- Create Schemas for each layer
CREATE SCHEMA bronze;

CREATE SCHEMA silver;

CREATE SCHEMA gold;
