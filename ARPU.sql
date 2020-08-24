/*Задача №1. SQL: когортный анализ
Имеется следующие таблицы:
users (user_id — id пользователя, installed_at — дата установки)
orders (id - id заказа, user_id, created_at дата оплаты, amount - сумма оплаты)

Необходимо написать SQL-запрос который посчитает накопленный ARPU 1го, 3го и 7го  дня для всех пользователей, 
который установили приложение в марте 2020го года. 
Пользователей необходимо поделить на ежедневные когорты.
*/

#------///СОЗДАНИЕ ДЕМОНСТРАЦИОННЫХ ТАБЛИЦ///------

	drop table  IF EXISTS sys.users;
	drop table  IF EXISTS sys.orders;

	CREATE  TABLE IF NOT EXISTS sys.users ( 
		user_id int #id пользователя
		,installed_at date # дата установки
	);

	insert into sys.users values
	 (1,'2020-03-01')
	,(2,'2020-03-01')
	,(3,'2020-03-01')
	,(4,'2020-03-01')
	,(5,'2020-03-02')
	,(6,'2020-03-02')
	,(7,'2020-03-04')
	,(8,'2020-03-05')
	,(9,'2020-03-06')
	,(10,'2020-03-07')
	,(11,'2020-03-07')
	,(12,'2020-04-01');


	CREATE TABLE IF NOT EXISTS sys.orders ( 
		user_id int
		,created_at date
		,amount decimal(15,2)
	);

	insert into sys.orders values
		 (1,'2020-03-01',500)
		,(2,'2020-03-01',600)
		,(1,'2020-03-02',500)
		,(3,'2020-03-03',4000)
		,(4,'2020-03-03',6000)
		,(5,'2020-03-02',6000)
		,(6,'2020-03-02',7000)
		,(1,'2020-03-01',500)
		,(2,'2020-03-04',600)
		,(7,'2020-03-04',500)
		,(8,'2020-03-06',4000)
		,(9,'2020-03-06',6000)
		,(5,'2020-03-10',6000)
		,(10,'2020-03-10',700);


#------///ПОДГОТОВКА ДАННЫХ ДЛЯ ОТЧЕТА///------

#генерация списка дат за март,исходя из вероятности ,что операции будут не каждый день для когорты,а отчет должен содержать ARPU за этот день
WITH recursive date_range AS ( 

	SELECT '2020-03-01' AS day_reporting # якорь
	UNION ALL
	SELECT DATE_ADD(R.day_reporting, interval 1 day ) # рекурсивный запрос
	FROM date_range R
	WHERE DATE_ADD(R.day_reporting, interval 1 day ) <= '2020-03-31'
	
)
#select * from date_range
,cohort_users as ( # разбиение пользователей на дневные когорты

	select  
		user_id
		,installed_at
		,count(*) over (partition by installed_at) as count_user # количество пользователей в когорте
		,dense_rank() over (order by installed_at) as rnk # порядковый номер дневной когорты
	from sys.users
    where
		installed_at between '2020-03-01' and '2020-03-31'
)
#select * from cohort_users

,users_orders as (  # список сумм потраченных каждым пользователем за каждый день отчетного периода

	select
		 day_reporting
		,us.installed_at
        ,us.user_id
		#,ord.created_at
		,ifnull(ord.amount,0) as amount
		,us.rnk
		,us.count_user
	from  cohort_users  us
	cross join 
		date_range dr
	left join sys.orders ord
		on us.user_id=ord.user_id
		and day_reporting=ord.created_at
	where day_reporting>=us.installed_at
    
)
#select * from users_orders

, cohort_amount as (  # список сумм потраченных каждой когортой за  каждый день отчетного периода и в предыдущие

	SELECT 
		#created_at
		day_reporting 
        ,rnk 
        ,count_user
        ,installed_at
		,SUM(amount)  as day_amount
       	, #коррелирующий подзапрос
		  (SELECT SUM(amount)  
		   FROM  users_orders
		   WHERE day_reporting <= uo.day_reporting and rnk=uo.rnk )  as total_amount
		
	FROM users_orders uo
	GROUP BY day_reporting,rnk
	ORDER BY rnk,day_reporting

)
#select * from cohort_amount
#------///РЕЗУЛЬТИРУЮЩИЙ ОТЧЕТНЫЙ ЗАПРОС///------

select 
	day_reporting `Отчетный день`
    ,rnk `Порядковый номер когорты`
    ,count_user `Количество пользователей в когорте` 
    ,installed_at `День когорты` 
    ,datediff(day_reporting,installed_at)  `День пользования приложением когортой`
	,day_amount `Сумма потраченная когортой за день`
    ,total_amount `Общая сумма потраченная когортой на этот день`
	,cast(day_amount /count_user as decimal(15,2)) `Дневной ARPU`
    ,cast(total_amount /count_user as decimal(15,2)) `Накопительный ARPU`

from cohort_amount
where datediff(day_reporting,installed_at) in (1,3,7) #  условие задания
