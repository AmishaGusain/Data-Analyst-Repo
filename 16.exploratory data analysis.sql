-- exploratory data analysis
select * from layoffs_staging2;

select max(total_laid_off), max(percentage_laid_off)
 from layoffs_staging2;
 
 -- whole company paid off
 select *  
from layoffs_staging2
where percentage_laid_off=1
order by funds_raised_millions DESC;

-- company wise total laid offs
select company, sum(total_laid_off)  
from layoffs_staging2 
group by company
order by 2 desc;

-- date range of data 
select min(`date`), max(`date`)
from layoffs_staging2;

-- industry wise total laid offs
select industry, sum(total_laid_off)  
from layoffs_staging2 
group by industry
order by 2 desc;

-- country wise total laid offs
select country, sum(total_laid_off)  
from layoffs_staging2 
group by country
order by 2 desc;

-- year wise total laid offs
select year(`date`), sum(total_laid_off)  
from layoffs_staging2 
group by  year(`date`)
order by 1 desc;

-- stage wise total laid offs
select stage, sum(total_laid_off)  
from layoffs_staging2 
group by  stage
order by 2 desc;

-- max layoff on one day wali company 
select * from layoffs_staging2 
where total_laid_off in(select max(total_laid_off) from layoffs_staging2);

-- month wise lay offs
select substring(`date`,1,7) `month`, sum(total_laid_off) total_off
from layoffs_staging2
where `date` is not null
group by `month`
order by `month` asc;

-- rolling total month wise layoffs
with Rolling_total as
(
-- month wise lay offs
select substring(`date`,1,7) `month`, sum(total_laid_off) total_off
from layoffs_staging2
where `date` is not null
group by `month`
order by `month` asc)
select `month`,total_off,sum(total_off) over(order by `month`) as rolling_total
 from Rolling_total;
 
 -- company laying off  year wise
 
 select company , year(`date`),sum(total_laid_off)
 from layoffs_staging2
 group by company, `date`
 order by company ;
 
  select company , year(`date`),sum(total_laid_off)
 from layoffs_staging2
 group by company, year(`date`)
 order by 3 desc ;
 
 -- which company has highest layoff in yearwise category
 with Company_year(company, years,total_layoffs) as
 ( select company , year(`date`),sum(total_laid_off)
 from layoffs_staging2
 group by company, year(`date`)
 )
 select * ,
 DENSE_RANK() over(partition by years order by total_layoffs desc)  as year_wise_ranking
 from Company_year
 where years is not null
  ;
  
   with Company_year(company, years,total_layoffs) as
 ( select company , year(`date`),sum(total_laid_off)
 from layoffs_staging2
 group by company, year(`date`)
 )
 select * ,
 DENSE_RANK() over(partition by years order by total_layoffs desc)  as year_wise_ranking
 from Company_year
 where years is not null
 order by year_wise_ranking ;
 
 -- year wise top 5 company who laid off most
  with Company_year(company, years,total_layoffs) as
 ( select company , year(`date`),sum(total_laid_off)
 from layoffs_staging2
 group by company, year(`date`)
 ), Company_year_ranking as
 (select * ,
 DENSE_RANK() over(partition by years order by total_layoffs desc)  as year_wise_ranking
 from Company_year
 where years is not null)
 select * from Company_year_ranking
 where year_wise_ranking<=5;
 