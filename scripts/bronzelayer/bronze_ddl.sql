/*
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.
===============================================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME , @end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME;
    BEGIN TRY 
        SET @batch_start_time=GETDATE();
         PRINT 'Loading Bronze Layer';
         PRINT '---------------------';
        IF OBJECT_ID ('bronze.crm_cust_info','U') IS NOT NULL
            DROP TABLE bronze.crm_cust_info

        CREATE TABLE bronze.crm_cust_info(
	        cst_id	INT,
	        cst_key Nvarchar(50),
	        cst_firstname Nvarchar(50),
	        cst_lastname Nvarchar(50),
	        cst_marital_status Nvarchar(50),
	        cst_gndr Nvarchar(25),
	        cst_create_date date
        );

        IF OBJECT_ID ('bronze.crm_prd_info','U') IS NOT NULL
            DROP TABLE bronze.crm_prd_info

        CREATE TABLE bronze.crm_prd_info (
            prd_id       INT,
            prd_key      NVARCHAR(50),
            prd_nm       NVARCHAR(50),
            prd_cost     INT,
            prd_line     NVARCHAR(50),
            prd_start_dt DATETIME,
            prd_end_dt   DATETIME
        );

        IF OBJECT_ID ('bronze.crm_sales_details','U') IS NOT NULL
            DROP TABLE bronze.crm_sales_details


        IF OBJECT_ID ('bronze.crm_sales_details','U') IS NOT NULL
            DROP TABLE bronze.crm_sales_details


        CREATE TABLE bronze.crm_sales_details (
            sls_ord_num  NVARCHAR(50),
            sls_prd_key  NVARCHAR(50),
            sls_cust_id  INT,
            sls_order_dt INT,
            sls_ship_dt  INT,
            sls_due_dt   INT,
            sls_sales    INT,
            sls_quantity INT,
            sls_price    INT
        );
    

        IF OBJECT_ID ('bronze.erp_loc_a101','U') IS NOT NULL
            DROP TABLE bronze.erp_loc_a101


        CREATE TABLE bronze.erp_loc_a101 (
            cid    NVARCHAR(50),
            cntry  NVARCHAR(50)
        );
    

        IF OBJECT_ID ('bronze.erp_cust_az12','U') IS NOT NULL
            DROP TABLE bronze.erp_cust_az12


        CREATE TABLE bronze.erp_cust_az12 (
            cid    NVARCHAR(50),
            bdate  DATE,
            gen    NVARCHAR(50)
        );


        IF OBJECT_ID ('bronze.erp_px_cat_g1v2','U') IS NOT NULL
            DROP TABLE bronze.erp_px_cat_g1v2


        CREATE TABLE bronze.erp_px_cat_g1v2 (
            id           NVARCHAR(50),
            cat          NVARCHAR(50),
            subcat       NVARCHAR(50),
            maintenance  NVARCHAR(50)
        );
