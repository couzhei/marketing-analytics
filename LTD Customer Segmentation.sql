with orders_source as (
    select distinct pii.vendor_id  as Vendor_Id,
           pi.amount      as Payed_Amount,
           pii.id         as Invoice_Item_Id,
           COALESCE(ppt.amount, 0) as Item_Wage,
					 -- ppt.amount     as Item_Wage,
           pi.user_id     as User_Id,
           pii.invoice_id as Invoice_Id,
           pii.title      as Item_Title,
           pii.quantity   as Quantity,
					 ((Extract(epoch from age(now(), pi.paid_at)) / 86400) :: int) as interval1,
					 case 
					 when pi.paid_at >= current_date - interval'45 day' then 4
					 when pi.paid_at >= current_date - interval'90 day' 
					 and pi.paid_at < current_date - interval'45 day' then 3
					 when pi.paid_at >= current_date - interval'135 day' 
					 and pi.paid_at < current_date - interval'90 day' then 2
					 when pi.paid_at >= current_date - interval'180 day' 
					 and pi.paid_at < current_date - interval'135 day' then 1
					 else 0 end as recency1
    from laravel.payment_invoices as pi
             join laravel.payment_invoice_items as pii
                  on pii.invoice_id = pi.id and pii.deleted_at is null
									and pii.vendor_id != 266
             left join laravel.payment_pay_transactions ppt
                  on ppt.invoice_item_id = pii.id and ppt.deleted_at is null
                      and ppt.transaction_reason in (3136, 3185, 3186)
    where pi.status = 2999
      and paid_at is not null
			order by Item_Wage
)

,
		 --------------------------------------------------
		 total_user_wage as (
         select User_Id, sum(os.Item_Wage) Total_Wage, max(recency1) as recency, count(distinct Invoice_Id) as count_invoices, min(recency1) first_buy, min(interval1) as interval1
         from orders_source as os
				 group by User_Id
     ),
		  --------------------------------------------------
		 total_customers as (
         select 
				 count(distinct os.User_Id)filter(where recency = 4 and first_buy= 4) Total_new_activeCustomers_recency4
				 ,count(distinct os.User_Id)filter(where recency = 4 and first_buy < 4) Total_activeCustomers_recency4
				 
				 ,count(distinct os.User_Id)filter(where recency = 3 and first_buy = 3) Total_new_warmCustomers_recency3
				 ,count(distinct os.User_Id)filter(where recency = 3 and first_buy <3 ) Total_Customers_recency3
				 
				 ,count(distinct os.User_Id)filter(where recency = 2) Total_Customers_recency2
				 ,count(distinct os.User_Id)filter(where recency = 1) Total_Customers_recency1
				 ,count(distinct os.User_Id)filter(where recency = 0) Total_Customers_recency0
         from orders_source as os join total_user_wage tuw
				 on os.User_Id = tuw.user_id
     ),

     total_wage as (
		 
         select 
				 sum(os.Item_Wage)filter(where recency = 0) Total_Wage_recency0
				 ,sum(os.Item_Wage)filter(where recency = 1) Total_Wage_recency1
				 ,sum(os.Item_Wage)filter(where recency = 2) Total_Wage_recency2
				 
				 ,sum(os.Item_Wage)filter(where recency = 3 and first_buy = 3) Totalwage_new_warmCustomers_recency3
				 ,sum(os.Item_Wage)filter(where recency = 3 and first_buy <3 ) Totalwage_Customers_recency3
				 
				 ,sum(os.Item_Wage)filter(where recency = 4 and first_buy= 4) Totalwage_new_active_recency4
				 ,sum(os.Item_Wage)filter(where recency = 4 and first_buy < 4) Totalwage_active_recency4
         from orders_source as os join total_user_wage tuw
				 on os.User_Id = tuw.user_id
     ),
   -----------------------------------------------------
     orders_per_month as (
         select date_trunc('month', pi.created_at) as               Month,
                count(distinct pi.user_id)                          Users_Count,
                count(pii.id)                                       Items_Count,
                count(pii.id)::decimal / count(distinct pi.user_id) Orders_Per_Customer
         from laravel.payment_invoices pi
                  join laravel.payment_invoice_items pii
                       on pii.invoice_id = pi.id
         where pi.status = 2999
           and pi.deleted_at isnull
           and pi.created_at < date_trunc('month', now())
         group by Month
     ),
     customer_revenue as (
         select (tw.Total_Wage_recency0::dec / bc.Total_Customers_recency0) as Customer_Revenue_recency0
				 ,(tw.Total_Wage_recency1::dec / bc.Total_Customers_recency1) as Customer_Revenue_recency1
				 ,(tw.Total_Wage_recency2::dec / bc.Total_Customers_recency2) as Customer_Revenue_recency2
				 
				 ,(tw.Totalwage_new_warmCustomers_recency3::dec / bc.Total_new_warmCustomers_recency3) as Customer_Revenue_recency3_new_warm
				 ,(tw.Totalwage_Customers_recency3::dec / bc.Total_Customers_recency3) as Customer_Revenue_recency3
				 
				 ,(tw.Totalwage_new_active_recency4::dec / bc.Total_new_activeCustomers_recency4) as Customer_Revenue_recency4_new_active
				 ,(tw.Totalwage_active_recency4::dec / bc.Total_activeCustomers_recency4) as Customer_Revenue_recency4
				 
         from total_customers bc,
              total_wage tw
     )
		,

   beautified as 
		 (
         select 
				 User_Id, Total_Wage as user_revenue, recency as user_last_payment, 
				 count_invoices, interval1,
				 case 
				 when   recency = 0
				 then   trunc(Customer_Revenue_recency0, 5) -- Customer_Revenue_recency0, 
				 when   recency = 1
				 then		trunc(Customer_Revenue_recency1, 5) -- Customer_Revenue_recency1,
         when   recency = 2       
				 then		trunc(Customer_Revenue_recency2, 5) -- Customer_Revenue_recency2,
				 
				 when   recency = 3 and first_buy = 3
				 then		trunc(Customer_Revenue_recency3_new_warm, 5) -- Customer_Revenue_recency3,
				 
				 when   recency = 3 and first_buy < 3
				 then		trunc(Customer_Revenue_recency3, 5) -- Customer_Revenue_recency3,
				 
				 when   recency = 4 and first_buy = 4
				 then		trunc(Customer_Revenue_recency4_new_active, 5) -- Customer_Revenue_recency4,
				 
				 when   recency = 4 and first_buy < 4
				 then		trunc(Customer_Revenue_recency4, 5) -- Customer_Revenue_recency4,
				 end as group_mean_Revenue,
								
								case 
								when recency = 0 and Total_Wage > Customer_Revenue_recency0 then 'lost_high_value'
								when recency = 0 and Total_Wage <= Customer_Revenue_recency0 then 'lost_low_value'
								when recency = 1 and Total_Wage > Customer_Revenue_recency1 then 'inactive_high_value'
								when recency = 1 and Total_Wage <= Customer_Revenue_recency1 then 'inactive_low_value'
								when recency = 2 and Total_Wage <= Customer_Revenue_recency2 then 'cold_low_value'
								when recency = 2 and Total_Wage > Customer_Revenue_recency2 then 'cold_high_value'
								when recency = 3 and first_buy = 3 then 'warm_new'
								when recency = 3 and first_buy < 3 and Total_Wage > Customer_Revenue_recency3 then 'warm_high_value'
								when recency = 3 and first_buy < 3 and Total_Wage <= Customer_Revenue_recency3  then 'warm_low_value'
								when recency = 4 and first_buy = 4 then 'active_new'
								when recency = 4 and first_buy < 4 and Total_Wage > Customer_Revenue_recency4 then 'active_high_value'
								when recency = 4 and first_buy < 4 and Total_Wage <= Customer_Revenue_recency4 then 'active_low_value'
								
								end as group_lable
         from total_user_wage, customer_revenue cr
				 order by recency desc, Total_Wage desc
     )
     select 
		 *
		 from beautified
		  [[where group_lable like {{lable}}]]
