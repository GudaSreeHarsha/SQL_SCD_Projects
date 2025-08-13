create table stg_pan_numbers_dataset
(
pan_number	text
)

select * from stg_pan_numbers_dataset

----Identify and Handle missing data

select * from stg_pan_numbers_dataset
where pan_number is null

----Check for duplicate data

select pan_number,
count(*)
from stg_pan_numbers_dataset
group by pan_number
having count(*)>1

----Handle the spaces leading/trailing

select *
from stg_pan_numbers_dataset
where pan_number <> trim(pan_number)

----Correct Letter cases

select * 
from stg_pan_numbers_dataset
where pan_number != upper(pan_number)

---- Cleaned Pan Numbers

select distinct upper(trim(pan_number)) as pan_numbers
from stg_pan_numbers_dataset
where pan_number is not null
and trim(pan_number) != ''

---- Function to check adjacent characters are similar
create or replace function fn_check_adjacent_chars(p_str text)
returns boolean
language plpgsql
as $$
begin
	for i in 1 .. (length(p_str)-1)
	loop 
		if substring(p_str,i,1)=substring(p_str,i+1,1)
		then
			return true; -- the adjacent characters are similar
		end if;
	end loop;
	return false;-- non of the adjacent characters are similar
end;
$$

select fn_check_adjacent_chars('12345')

---- Function to check sequencial characters are used
create or replace function fn_check_sequencial_chars(p_str text)--ABCDE
returns boolean
language plpgsql
as $$
begin
	for i in 1 .. (length(p_str)-1)
	loop 
		if ascii(substring(p_str,i+1,1))-ascii(substring(p_str,i,1))<>1
		then
			return false; -- the string is not sequencial
		end if;
	end loop;
	return true;-- the string is sequencial
end;
$$

select fn_check_sequencial_chars('13245')

---- Regular expression to validate the PanNumber-- 'AAAAA1234A'

select * 
from stg_pan_numbers_dataset
where pan_number ~ '^[A-Z]{5}[0-9]{4}[A-Z]$'

----Valid and Invalid Pan Numbers categorisation
create or replace view vw_valid_invalid_pan_numbers
as
with cte_cleaned_pan as
(
	select distinct upper(trim(pan_number)) as pan_number
	from stg_pan_numbers_dataset
	where pan_number is not null
	and trim(pan_number) != ''
),
 cte_valid_pan as
(
	select *
	from cte_cleaned_pan
	where fn_check_adjacent_chars(pan_number)=false
	and fn_check_sequencial_chars(substring(pan_number,1,5))=false
	and fn_check_sequencial_chars(substring(pan_number,6,4))=false
	and pan_number ~'^[A-Z]{5}[0-9]{4}[A-Z]$'
)
select cln.pan_number,
case 
when vld.pan_number is not null
then 'Valid Pan Number'
else 'Invalid Pan Number'
end as status
from cte_cleaned_pan cln
left join cte_valid_pan vld on vld.pan_number=cln.pan_number


select * from vw_valid_invalid_pan_numbers

----summery Report

with cte as
(
	select
		(select count(*) from stg_pan_numbers_dataset) as total_processed_records,
		count(*) filter(where status='Valid Pan Number') as total_Valid_pans,
		count(*) filter(where status='Invalid Pan Number') as total_Invalid_pans
	from vw_valid_invalid_pan_numbers
)
select total_processed_records, total_Valid_pans, total_Invalid_pans,
(total_processed_records-(total_Valid_pans+total_Invalid_pans))  as total_missing_pans
from cte









