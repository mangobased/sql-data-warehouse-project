/*
This stored procedure loads data from CSV files into the 'bronze' schema.
It will first truncate the tables and then batch load data.
Replace file locations with your own.

Usage Example:
CALL bronze.load_bronze();
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql AS $$
DECLARE
start_time TIMESTAMP;
end_time TIMESTAMP;
operation_duration REAL;
layer_start_time TIMESTAMP;
layer_end_time TIMESTAMP;
layer_operation_duration REAL;

BEGIN
	
	-- Set the start time for whole layer operations
	layer_start_time := clock_timestamp();

	RAISE NOTICE '=======================================';
	RAISE NOTICE 'Truncating table contents if exists';
	RAISE NOTICE '=======================================';

	-- Set the start time for the truncate operations
	start_time := clock_timestamp();

	-- Delete table contents before importing data
	RAISE NOTICE '>> Truncating table: bronze.crm_cust_info';
	TRUNCATE TABLE bronze.crm_cust_info;

	RAISE NOTICE '>> Truncating table: bronze.crm_prd_info';
	TRUNCATE TABLE bronze.crm_prd_info;

	RAISE NOTICE '>> Truncating table: bronze.crm_sales_details';
	TRUNCATE TABLE bronze.crm_sales_details;

	RAISE NOTICE '>> Truncating table: bronze.erp_cust_az12';
	TRUNCATE TABLE bronze.erp_cust_az12;

	RAISE NOTICE '>> Truncating table: bronze.erp_loc_a101';
	TRUNCATE TABLE bronze.erp_loc_a101;

	RAISE NOTICE '>> Truncating table: bronze.erp_px_cat_g1v2';
	TRUNCATE TABLE bronze.erp_px_cat_g1v2;

	-- Calculate the truncation duration
	end_time := clock_timestamp();
	operation_duration := EXTRACT(EPOCH FROM (end_time - start_time));
	
	-- Print the duration
	RAISE NOTICE '>> Truncating duration % seconds', operation_duration;
	RAISE NOTICE '>> ---------------------------------';

	RAISE NOTICE '=======================================';
	RAISE NOTICE 'Loading Bronze Layer';
	RAISE NOTICE '=======================================';

	RAISE NOTICE '---------------------------------------';
	RAISE NOTICE 'Loading CRM Tables';
	RAISE NOTICE '---------------------------------------';
	-- Bulk importing data

	RAISE NOTICE '>> Loading table: bronze.crm_cust_info';

	start_time := clock_timestamp();

	COPY bronze.crm_cust_info 
	FROM '/tmp/cust_info.csv'
	WITH (
	    FORMAT CSV, 
	    HEADER,
	    DELIMITER ','
	);

	end_time := clock_timestamp();
	operation_duration := EXTRACT(EPOCH FROM (end_time - start_time));

	RAISE NOTICE '>> Loading duration % seconds', operation_duration;
	RAISE NOTICE '>> ---------------------------------';

	RAISE NOTICE '>> Loading table: bronze.crm_prd_info';

	start_time := clock_timestamp();

	COPY bronze.crm_prd_info 
	FROM '/tmp/prd_info.csv'
	WITH (
	    FORMAT CSV, 
	    HEADER,
	    DELIMITER ','
	);
	
	end_time := clock_timestamp();
	operation_duration := EXTRACT(EPOCH FROM (end_time - start_time));

	RAISE NOTICE '>> Loading duration % seconds', operation_duration;
	RAISE NOTICE '>> ---------------------------------';


	RAISE NOTICE '>> Loading table: bronze.crm_sales_details';

	start_time := clock_timestamp();

	COPY bronze.crm_sales_details 
	FROM '/tmp/sales_details.csv'
	WITH (
	    FORMAT CSV, 
	    HEADER,
	    DELIMITER ','
	);

	end_time := clock_timestamp();
	operation_duration := EXTRACT(EPOCH FROM (end_time - start_time));

	RAISE NOTICE '>> Loading duration % seconds', operation_duration;
	RAISE NOTICE '>> ---------------------------------';
	
	RAISE NOTICE '---------------------------------------';
	RAISE NOTICE 'Loading ERP Tables';
	RAISE NOTICE '---------------------------------------';

	RAISE NOTICE '>> Loading table: bronze.erp_loc_a101';

	start_time := clock_timestamp();

	COPY bronze.erp_loc_a101 
	FROM '/tmp/LOC_A101.csv'
	WITH (
	    FORMAT CSV, 
	    HEADER,
	    DELIMITER ','
	);

	end_time := clock_timestamp();
	operation_duration := EXTRACT(EPOCH FROM (end_time - start_time));

	RAISE NOTICE '>> Loading duration % seconds', operation_duration;
	RAISE NOTICE '>> ---------------------------------';
	
	RAISE NOTICE '>> Loading table: bronze.erp_cust_az12';

	start_time := clock_timestamp();

	COPY bronze.erp_cust_az12 
	FROM '/tmp/CUST_AZ12.csv'
	WITH (
	    FORMAT CSV, 
	    HEADER,
	    DELIMITER ','
	);
	
	end_time := clock_timestamp();
	operation_duration := EXTRACT(EPOCH FROM (end_time - start_time));

	RAISE NOTICE '>> Loading duration % seconds', operation_duration;
	RAISE NOTICE '>> ---------------------------------';

	RAISE NOTICE '>> Loading table: bronze.erp_px_cat_g1v2';

	start_time := clock_timestamp();

	COPY bronze.erp_px_cat_g1v2 
	FROM '/tmp/PX_CAT_G1V2.csv'
	WITH (
	    FORMAT CSV, 
	    HEADER,
	    DELIMITER ','
	);

	end_time := clock_timestamp();
	operation_duration := EXTRACT(EPOCH FROM (end_time - start_time));

	RAISE NOTICE '>> Loading duration % seconds', operation_duration;
	RAISE NOTICE '>> ---------------------------------';

	layer_end_time := clock_timestamp();
	layer_operation_duration := EXTRACT(EPOCH FROM (layer_end_time - layer_start_time));
	
	-- Print the duration
	RAISE NOTICE '>> Bronze layer loading duration % seconds', layer_operation_duration;
	RAISE NOTICE '>> ---------------------------------';

END;
$$;
