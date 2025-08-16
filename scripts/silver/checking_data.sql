-- check for nulls and duplicates

select
	cst_id,
	count(*)
from bronze.crm_cust_info
group by cst_id
having count (*) >1 or cst_id is null

-- check for unwanted spaces

select
	cst_firstname
from bronze.crm_cust_info
where cst_firstname != trim(cst_firstname)


-- data standardization & consistency
-- here we have gndr as M,F so we will transform it into Male,Female so data could be more understandable
select distinct
	cst_gndr
from bronze.crm_cust_info


-- checking quatity of crm_prd_info table

-- checking unwanted spaces

select prd_nm
from bronze.crm_prd_info
where prd_nm != trim(prd_nm)


-- data standardization

select distinct prd_line
from bronze.crm_prd_info


-- check for invalid data orders

select *
from bronze.crm_prd_info
where prd_end_dt < prd_start_dt



-- checking the quallity of crm_sales_details


select sls_ord_num
from bronze.crm_sales_details
where sls_ord_num != trim(sls_ord_num)


-- check invalid dates

select
	nullif(sls_order_dt,0) sls_order_dt
from bronze.crm_sales_details
where sls_order_dt <=0 
or len(sls_order_dt) != 8
or sls_order_dt > 20500101
or sls_order_dt < 19000101


-- checking data consistency


select distinct
	sls_sales,
	sls_quantity,
	sls_price,

	case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price)
			then sls_quantity *abs(sls_price)
		else sls_sales
	end sls_sales,

	case when sls_price is null or sls_price <= 0
			then sls_sales / nullif(sls_quantity , 0)
		else sls_price
	end as sls_price

from bronze.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <=0 or sls_quantity <= 0 or sls_price <= 0



-- checking the quality of erp_cust_az12


select 
*
from bronze.erp_cust_az12

-- identify out-of-range dates

select distinct bdate
from bronze.erp_cust_az12
where bdate < '1924-01-01' or bdate > getdate()


-- data standardization

select distinct
gen,

case when upper(trim(gen)) in ('F','Female') then 'Female'
	 when upper(trim(gen)) in ('M','Male') then 'Male'
	 else 'n/a'
end gen
from bronze.erp_cust_az12


-- check for unwanted spaces

select * from bronze.erp_px_cat_giv2
where cat != trim(cat)

