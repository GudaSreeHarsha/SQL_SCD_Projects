--Netflix project

drop table if exists netflix;

create table netflix
(
	show_id	varchar(6),
	type 	varchar(10),
	title	varchar(150),
	director varchar(230),	
	casts	varchar(1000),
	country	varchar(150),
	date_added	varchar(50),
	release_year  int,	
	rating	varchar(20),
	duration	varchar(15),
	listed_in	varchar(150),
	description varchar(300)
)
select * from netflix


select 
count(*) as total_contents 
from netflix


select 
	distinct type
from netflix

----Count no of movies and TV shows

select * from netflix

select 
type,
count(*) as total_content
from netflix
group by type

---- Find the most common rating for movies and TV shows

select * from netflix

select 
	type,
	rating
from
(
	select 
		type,
		rating,
		count(*) as rating_count,
		Rank() over(partition by type order by count(*) desc)as ranking
	from
		netflix
	group by type, rating
)where ranking=1

---- List all the movies listed in 2020

select * from netflix

select 
*
from netflix
where 
	release_year=2020
and 
	type='Movie'

----find the top 5 contries with most content from netflix

select * from netflix

with countries
as
(
	select 
		trim(unnest(string_to_array(country,','))) as new_country,
		count(*) as counting,
		dense_rank() over(order by count(*)  desc) as ranking
	from netflix
	group by 1
)
select * 
from countries 
where ranking <=5


----Identify the longest movie

select * 
from netflix
where 
	type ='Movie'
	and
	duration=(select max(duration) from netflix)


----Identify the contents which added in last five years

select *
from netflix 
where to_date(date_added,'Month-dd-YYYY') >=current_date- interval '6 Years'


----Find all the movies and Tv shows directed by 'Rajiv Chilaka'

select * 
from netflix 
where director ilike '%Rajiv Chilaka%'


----list all Tv shows with more than 5 seasons

select *,
SPLIT_PART(duration,' ',1)as seasons 
from netflix
where 
	type='TV Show'
	and
	SPLIT_PART(duration,' ',1)::numeric>5

----Find no of content items in each genre

select * 
from netflix

select 
	BTRIM(unnest(string_to_array(listed_in,','))) as genre,
	count(show_id)as count_of_shows
from netflix
group by genre
order by count_of_shows desc

----Find each year and the average number of content released by India on netflix return top 5 year with highest content release

select 
	extract(year from TO_DATE(date_added,'Month/DD/YYY')) as year,
	count(*),
	round(count(*)::numeric/(select count(*) from netflix where country= 'India')::numeric * 100,2) as avg_content_per_year
from netflix
where country ilike 'India'
group by 1
order by 3 desc

----List all the movies that are documentries

select *
from netflix
where listed_in like '%Documentaries%'

----Find all the contents without director

select * from netflix
where director is null

----Find how many movies actor salman khan appeared in last 10 years

select *
from netflix 
where 
casts ilike '%Salman Khan%'
and
release_year>extract(year from current_date)-11

----Find the top 10 actors who have appeared in highest number of movies produced in india
with cte as
(
	select 
	trim(unnest(string_to_array(casts,','))) as actors,
	count(*) as count_of_movies,
	rank() over(order by count(*) desc) as rank
	from netflix
	where country='India'
	group by actors
	order by count_of_movies desc
)
select * from cte
where rank <11

----categorize the content based on the presence of keywords 'Kill' and 'Voilence' in the description field. Label content containing these
----keywords as 'Bad' and all others as 'Good'. Count how many items fall into each category

with new_table
as
(
	select *, 
	case 
		when 
			description ilike '%kill%'
			or 
			description ilike '%violence%' 
		then 'Bad Content'
		else 'Good content'
		end as category
	from netflix
)
select category,count(*) as total_count
from new_table
group by category

------
select * from netflix
where
description ilike '%kill%'
or
description ilike '%voilence%'






