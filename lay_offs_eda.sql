-- EXPLORATORYY DATA ANALYSIS

SELECT *
FROM layoffs_staging2;

SELECT MAX(total_laid_off),MAX(percentage_laid_off)
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised DESC ;

SELECT  company,SUM(total_laid_off)
FROM layoffs_staging2
group by company
order by 2 DESC ; 

 SELECT MIN(`date`),MAX(`date`)
 FROM layoffs_staging2 ;
 
 
SELECT industry ,SUM(total_laid_off)
FROM layoffs_staging2
group by industry
order by 2 DESC ; 

SELECT country ,SUM(total_laid_off)
FROM layoffs_staging2
group by country
order by 2 DESC ; 
 
SELECT *
FROM layoffs_staging2;

SELECT YEAR(`date`) ,SUM(total_laid_off)
FROM layoffs_staging2
group by year (`date`)
order by 1 DESC ; 

SELECT stage ,SUM(total_laid_off)
FROM layoffs_staging2
group by stage
order by 2 DESC ; 

-- rolling total layoffs
SELECT SUBSTRING(`date`,6,2) AS `MONTH`,SUM(total_laid_off)
FROM layoffs_staging2
WHERE  SUBSTRING(`date`,6,2) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC;

WITH rolling_total AS
(
SELECT SUBSTRING(`date`,1,7) AS `MONTH`,SUM(total_laid_off) as total_off
FROM layoffs_staging2
WHERE  SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC
)
SELECT `MONTH`,total_off,SUM(total_off) OVER(ORDER BY `MONTH`) AS rolling_total
FROM rolling_total;

SELECT country ,SUM(total_laid_off)
FROM layoffs_staging2
group by country
order by 2 DESC ; 

SELECT company ,YEAR(`date`),SUM(total_laid_off)
FROM layoffs_staging2
group by  company ,year(`date`)
order by  3 ASC ;


WITH company_year (company,years,total_laid_off) AS
(
SELECT company ,YEAR(`date`),SUM(total_laid_off)
FROM layoffs_staging2
group by company,year(`date`)
),company_year_rank AS
 (
 SELECT *,
 dense_rank()OVER(PARTITION BY years ORDER BY total_laid_off DESC ) AS ranking
 FROM company_year
 WHERE years IS NOT NULL
 )
 select *
 from company_year_rank
 WHERE ranking <=5 ;