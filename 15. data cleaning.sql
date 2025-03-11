-- data cleaning
select * from layoffs;

-- to do in cleaning
-- 1.remove duplicates
-- 2. standardize data 
-- 3. null values or blank
-- 4. remove unnecessary columns

-- keep copy of raw data 
create table layoffs_staging
like layoffs;

insert layoffs_staging
select * from layoffs;

select * from layoffs_staging;

-- 1. cleaning duplicates as there no primary key present use row_number()

select *, 
row_number() 
over(partition by company , location,industry, total_laid_off,percentage_laid_off,`date`,stage, country, funds_raised_millions) row_num
from layoffs_staging;
-- checking for duplicate datas
with duplicate_cte as
(select *, 
row_number() 
over(partition by company,location,industry, total_laid_off,percentage_laid_off,`date`,stage, country, funds_raised_millions) row_num
from layoffs_staging
)
select * from duplicate_cte
where row_num>1;

-- making table for deleting duplicate data with row_num col
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * from layoffs_staging2;

insert into layoffs_staging2
select *, 
row_number() 
over(partition by company , location,industry, total_laid_off,percentage_laid_off,`date`,stage, country, funds_raised_millions) row_num
from layoffs_staging;

select * from layoffs_staging2
where row_num>1;

set sql_safe_updates=0;
delete from layoffs_staging2
where row_num>1;

-- 2. standardizing data
-- removing leading space
select DISTINCT(company) from layoffs_staging2;

update layoffs_staging2
set company=trim(company);

select DISTINCT(industry) from layoffs_staging2 ORDER BY 1;

select * from layoffs_staging2
where industry like 'Crypto%';
-- same naming
update layoffs_staging2
set industry='Crypto'
where industry like 'Crypto%';

select DISTINCT(country) from layoffs_staging2 order by 1;
update layoffs_staging2
set country=trim(trailing '.' from country)
where country like 'United States%';

-- converting date in correct date format
select `date` ,
str_to_date(`date`,'%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set `date`=str_to_date(`date`,'%m/%d/%Y');
-- change it data type from text to date also
alter table layoffs_staging2
modify `date` date;

-- 3. nulls and blanks

select * from layoffs_staging2
where total_laid_off is null and percentage_laid_off is null;

-- null and blank in industry column try to populate it based on similar col data
select *
from layoffs_staging2 
where industry is null or industry='';

select *
from layoffs_staging2 ;

select t1.industry, t2.industry 
from layoffs_staging2 t1 
join layoffs_staging2 t2
on t1.company=t2.company
where (t1.industry is null )
and t2.industry is  not null;

-- change the blank spaces to null to make query work
update layoffs_staging2 
set industry=null
where industry='';

update layoffs_staging2 t1
join layoffs_staging2 t2 
on t1.company=t2.company
set t1.industry=t2.industry 
where (t1.industry is null )
and t2.industry is  not null;

-- delete the unrequired data
select * from layoffs_staging2
where total_laid_off is null and percentage_laid_off is null;

-- delete unnecessary column
alter table layoffs_staging2
drop column row_num;

delete from layoffs_staging2
where total_laid_off is null and percentage_laid_off is null;
