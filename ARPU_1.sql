WITH user_orders as (
select
		
		us.installed_at
        #,us.user_id
		,ord.created_at
		,ord.amount
		,us.count_user
	from  (select  
		user_id
		,installed_at
		,count(*) over (partition by installed_at) as count_user # количество пользователей в когорте
		from sys.users
    where
		installed_at between '2020-03-01' and '2020-03-31')  us
	inner  join sys.orders ord
		on us.user_id=ord.user_id)
SELECT 
		#created_at
		created_at `День операций`
        ,datediff(created_at,installed_at)  `День пользования приложением когортой`
        ,count_user `Количество пользователей в когорте` 
        ,installed_at `День когорты` 
		,SUM(amount)/count_user  as `Дневной ARPU`
       	, #коррелирующий подзапрос
		  (SELECT SUM(amount) /count_user 
		   FROM  user_orders
		   WHERE created_at <= uo.created_at and installed_at=uo.installed_at )  as `Накопительный ARPU`
		
	FROM user_orders uo
    where datediff(created_at,installed_at) in (1,3,7) #  условие задания
	GROUP BY created_at,installed_at
	ORDER BY installed_at,created_at
		
