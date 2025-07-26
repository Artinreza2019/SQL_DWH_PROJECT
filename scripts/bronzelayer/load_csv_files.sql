/*
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.
===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME , @end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME;
    BEGIN TRY 
        SET @batch_start_time=GETDATE();

       PRINT 'Loading CRM Tables';
           PRINT'----------------------------------------------------------';
           SET @start_time=GETDATE();
       PRINT'>> Truncating table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info

        --FULL LOAD
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\sajad\Desktop\New folder (2)\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH(
            FIRSTROW=2,
            FIELDTERMINATOR=',',
            TABLOCK
        );
        SET @end_time=GETDATE();
        PRINT '>> LOAD DURATION ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) +' seconds' ;

        PRINT'----------------------------------------------------------';

        SET @start_time=GETDATE();

        PRINT'>> Truncating table: bronze.crm_prd_info';

        TRUNCATE TABLE bronze.crm_prd_info

        --FULL LOAD
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\sajad\Desktop\New folder (2)\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH(
            FIRSTROW=2,
            FIELDTERMINATOR=',',
            TABLOCK
        );
        SET @end_time=GETDATE();
        PRINT '>> LOAD DURATION ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) +' seconds' ;


        PRINT'----------------------------------------------------------';
              SET @start_time=GETDATE();

        PRINT'>> Truncating table: bronze.crm_sales_details';


        TRUNCATE TABLE bronze.crm_sales_details

        --FULL LOAD
        BULK INSERT bronze.crm_sales_details
        FROM'C:\Users\sajad\Desktop\New folder (2)\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH(
            FIRSTROW=2,
            FIELDTERMINATOR=',',
            TABLOCK
        );
                SET @end_time=GETDATE();
        PRINT '>> LOAD DURATION ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) +' seconds' ;


        PRINT'----------------------------------------------------------';
        
        PRINT 'Loading ERP Tables';
        PRINT'----------------------------------------------------------';
        SET @start_time=GETDATE();

         PRINT'>> Truncating table: bronze.erp_loc_a101';

        TRUNCATE TABLE bronze.erp_loc_a101

        --FULL LOAD
        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\sajad\Desktop\New folder (2)\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH(
            FIRSTROW=2,
            FIELDTERMINATOR=',',
            TABLOCK
        );
        SET @end_time=GETDATE();
        PRINT '>> LOAD DURATION ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) +' seconds' ;
        
        PRINT'----------------------------------------------------------';
        SET @start_time=GETDATE();

        PRINT'>> Truncating table: bronze.erp_px_cat_g1v2';

        TRUNCATE TABLE bronze.erp_cust_az12

        --FULL LOAD
        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\sajad\Desktop\New folder (2)\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH(
            FIRSTROW=2,
            FIELDTERMINATOR=',',
            TABLOCK
        );
        SET @end_time=GETDATE();
        PRINT '>> LOAD DURATION ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) +' seconds' ;


        PRINT'----------------------------------------------------------';
        SET @start_time=GETDATE();

        PRINT'>> Truncating table: bronze.erp_px_cat_g1v2';

        TRUNCATE TABLE bronze.erp_px_cat_g1v2

        --FULL LOAD
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\sajad\Desktop\New folder (2)\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH(
            FIRSTROW=2,
            FIELDTERMINATOR=',',
            TABLOCK
        );
        SET @end_time=GETDATE();
        PRINT '>> LOAD DURATION ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) +' seconds' ;
        PRINT'----------------------------------------------------------';
        SET @batch_end_time =GETDATE();
        PRINT 'Whole duration : '+ CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time) AS NVARCHAR)+' seconds';
        
        PRINT '----------------------------------------------------------';

    END TRY
    BEGIN CATCH
        PRINT'----------------------------------------------------------';
        PRINT'Error Occured During Loading Layers';
        PRINT'Error Message'+Error_Message();
        PRINT'Error Message'+Cast(Error_Number() As Nvarchar);
        PRINT'----------------------------------------------------------';

    END CATCH
END
