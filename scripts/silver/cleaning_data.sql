create or alter procedure silver.load_silver as
begin
	insert into silver.crm_cust_info(
	cst_id,
	cst_key,
	cst_lastname,
	cst_firstname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
)

select
	cst_id,
	cst_key,
	trim(cst_firstname) as cst_firstname,
	trim(cst_lastname) as cst_lastname,
	case
		when upper(trim(cst_material_status)) = 'S' then 'Single'
		when upper(trim(cst_material_status)) = 'M' then 'Married'
		else 'n/a'
	end cst_marital_status,
	case
		when upper(trim(cst_gndr)) = 'F' then 'Female'
		when upper(trim(cst_gndr)) = 'M' then 'Male'
		else 'n/a'
	end cst_gndr,
	cst_create_data as cst_create_date
from
(
	select
		*,
		row_number() over(partition by cst_id order by cst_create_data desc) as flag_last 
	from bronze.crm_cust_info
	where cst_id is not null
)t where flag_last = 1



-- cleaning bronze.crm_prd_info
INSERT INTO silver.crm_prd_info (
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
SELECT
    prd_id,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
    prd_nm,
    ISNULL(prd_cost, 0) AS prd_cost,
    CASE 
        WHEN UPPER(LTRIM(RTRIM(prd_line))) = 'M' THEN 'Mountain'
        WHEN UPPER(LTRIM(RTRIM(prd_line))) = 'R' THEN 'Road'
        WHEN UPPER(LTRIM(RTRIM(prd_line))) = 'S' THEN 'Other Sales'
        WHEN UPPER(LTRIM(RTRIM(prd_line))) = 'T' THEN 'Touring'
        ELSE 'n/a'
    END AS prd_line,
    CAST(prd_start_dt AS DATE) AS prd_start_dt,
    CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info;




-- cleaning crm_sales_details

INSERT INTO silver.crm_sales_details (
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
)
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,

    CASE 
        WHEN sls_order_dt IS NULL OR sls_order_dt = 0 OR LEN(sls_order_dt) != 8 
             OR TRY_CAST(CAST(sls_order_dt AS VARCHAR(8)) AS DATE) IS NULL
        THEN NULL
        ELSE CAST(CAST(sls_order_dt AS VARCHAR(8)) AS DATE)
    END AS sls_order_dt,

    CASE 
        WHEN sls_ship_dt IS NULL OR sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8
             OR TRY_CAST(CAST(sls_ship_dt AS VARCHAR(8)) AS DATE) IS NULL
        THEN NULL
        ELSE CAST(CAST(sls_ship_dt AS VARCHAR(8)) AS DATE)
    END AS sls_ship_dt,

    CASE 
        WHEN sls_due_dt IS NULL OR sls_due_dt = 0 OR LEN(sls_due_dt) != 8
             OR TRY_CAST(CAST(sls_due_dt AS VARCHAR(8)) AS DATE) IS NULL
        THEN NULL
        ELSE CAST(CAST(sls_due_dt AS VARCHAR(8)) AS DATE)
    END AS sls_due_dt,

    CASE 
        WHEN sls_sales IS NULL OR sls_sales <= 0 
             OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales,

    sls_quantity,

    CASE 
        WHEN sls_price IS NULL OR sls_price <= 0
        THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE sls_price
    END AS sls_price

FROM bronze.crm_sales_details;




-- cleaning erp_cust_az12
INSERT INTO silver.erp_cust_az12 (
    cid,
    bdate,
    gen
)
SELECT
    CASE 
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
        ELSE cid
    END AS cid,

    CASE 
        WHEN bdate > GETDATE() THEN NULL
        ELSE bdate
    END AS bdate,

    CASE 
        WHEN UPPER(LTRIM(RTRIM(gen))) IN ('F', 'FEMALE') THEN 'Female'
        WHEN UPPER(LTRIM(RTRIM(gen))) IN ('M', 'MALE') THEN 'Male'
        ELSE 'n/a'
    END AS gen

FROM bronze.erp_cust_az12;



-- cleaning erp_loc_a101

INSERT INTO silver.erp_loc_a101 (
    cid,
    cntry
)
SELECT 
    REPLACE(cid, '-', '_') AS cid,
    CASE 
        WHEN LTRIM(RTRIM(cntry)) = 'DE' THEN 'Germany'
        WHEN LTRIM(RTRIM(cntry)) IN ('US', 'USA') THEN 'United States'
        WHEN cntry IS NULL OR LTRIM(RTRIM(cntry)) = '' THEN 'n/a'
        ELSE LTRIM(RTRIM(cntry))
    END AS cntry
FROM bronze.erp_loc_a101;



-- cleaning erp_px_cat_g1v2

INSERT INTO silver.erp_px_cat_g1v2(
    id,
    cat,
    subcat,
    maintenance
)
SELECT
    CAST(id AS nvarchar(50)) AS id,
    CAST(cat AS NVARCHAR(50)) AS cat,
    CAST(subcat AS NVARCHAR(50)) AS subcat,
    CAST(maintenance AS NVARCHAR(50)) AS maintenance
FROM bronze.erp_px_cat_giv2;
end

exec silver.load_silver