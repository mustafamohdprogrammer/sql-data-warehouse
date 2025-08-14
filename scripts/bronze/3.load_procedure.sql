create or alter procedure bronze.load_bronze as
begin
	declare @start_time datetime , @end_time datetime , @batch_start datetime , @batch_end datetime;
	begin try
		set @batch_start = getdate();
		print '=====================================================';
		print 'Loading Bronze Layer';
		print '======================================================';


		print '-------------------------------------------------';
		print 'Loading CRM Tables';
		print '-------------------------------------------------';

		set @start_time  = getdate();
		print '>> Truncating Table: bronze.crm_cust_info';
		truncate table bronze.crm_cust_info;
	
		print '>> Inserting Data Info : bronze.crm_cust_info';
		bulk insert bronze.crm_cust_info
		from 'D:\SQL\sql_baara\dataWarehouse\datasets\source_crm\cust_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);

		set @end_time  = getdate();
		print 'Load Duration :' + cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds';


		set @start_time  = getdate();
		print '>> Truncating Table: bronze.crm_prd_info';
		truncate table bronze.crm_prd_info;

		print '>> Inserting Data Info : bronze.crm_prd_info';
		bulk insert bronze.crm_prd_info
		from 'D:\SQL\sql_baara\dataWarehouse\datasets\source_crm\prd_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);

		set @end_time  = getdate();
		print 'Load Duration :' + cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds';


		set @start_time  = getdate();
		print '>> Truncating Table: bronze.crm_sales_details';
		truncate table bronze.crm_sales_details;

		print '>> Inserting Data Info : bronze.crm_sales_details';
		bulk insert bronze.crm_sales_details
		from 'D:\SQL\sql_baara\dataWarehouse\datasets\source_crm\sales_details.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);

	set @end_time  = getdate();
		print 'Load Duration :' + cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds';


		print '-------------------------------------------------';
		print 'Loading ERP Tables';
		print '-------------------------------------------------';


		set @start_time  = getdate();
		print '>> Truncating Table: bronze.erp_cust_az12';
		truncate table bronze.erp_cust_az12;

		print '>> Inserting Data Info : bronze.erp_cust_az12';
		bulk insert bronze.erp_cust_az12
		from 'D:\SQL\sql_baara\dataWarehouse\datasets\source_erp\cust_az12.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);

		set @end_time  = getdate();
		print 'Load Duration :' + cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds';

		set @start_time  = getdate();
		print '>> Truncating Table: bronze.erp_loc_a101';
		truncate table bronze.erp_loc_a101;

		print '>> Inserting Data Info : bronze.erp_loc_a101';
		bulk insert bronze.erp_loc_a101
		from 'D:\SQL\sql_baara\dataWarehouse\datasets\source_erp\loc_a101.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);


		set @end_time  = getdate();
		print 'Load Duration :' + cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds';


		set @start_time  = getdate();
		print '>> Truncating Table: bronze.erp_px_cat_giv2';
		truncate table bronze.erp_px_cat_giv2;

		print '>> Inserting Data Info : bronze.erp_px_cat_giv2';
		bulk insert bronze.erp_px_cat_giv2
		from 'D:\SQL\sql_baara\dataWarehouse\datasets\source_erp\px_cat_g1v2.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);

		set @end_time  = getdate();
		print 'Load Duration :' + cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds';

		set @batch_end = getdate();
		print '================================='
		print 'Loading Bronze layer is completed'
		print 'Total Load Duration ' + cast (datediff(second,@batch_start,@batch_end) as navchar) + 'seconds';
	end try
	begin catch
		print '============================'
		print 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		print 'Error Message' + ERROR_MESSAGE();
		print 'Error Message' + cast (ERROR_NUMBER() as nvarchar);
		print 'Error Message' + cast (ERROR_STATE() as nvarchar);
		print '============================'
	end catch
end




exec bronze.load_bronze;