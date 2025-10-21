-- Data cleaning of world layoffs

USE world_layoffs;
SELECT *
FROM layoffs;

-- CREATING STAGING TABLE TO PROTECT RAW DATA
CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT*
FROM layoffs;

-- 1.REMOVING DUPLICATAES
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,industry,total_laid_off,percentage_laid_off,`date`,stage,
country,funds_raised) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,industry,total_laid_off,percentage_laid_off,`date`,stage,
country,funds_raised) AS row_num
FROM layoffs_staging
)
DELETE
FROM duplicate_cte
WHERE row_num >1;

SHOW CREATE TABLE layoffs_staging;
CREATE TABLE `layoffs_staging2` (
   `company` text,
   `location` text,
   `total_laid_off` double DEFAULT NULL,
   `date` text,
   `percentage_laid_off` text,
   `industry` text,
   `source` text,
   `stage` text,
   `funds_raised` text,
   `country` text,
   `date_added` text,
   `row_num` INT
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ;


SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,industry,total_laid_off,percentage_laid_off,`date`,stage,
country,funds_raised) AS row_num
FROM layoffs_staging ;

DELETE
FROM layoffs_staging2
WHERE row_num >1;

SELECT*
FROM layoffs_staging2
WHERE row_num >1;


-- 2. standardizing the data

 SELECT distinct TRIM(company)
 FROM layoffs_staging2;
 
 UPDATE layoffs_staging2
 SET company = TRIM(company);
 
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%'
ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE  industry LIKE 'Crypto%' ;

UPDATE layoffs_staging2
SET industry =  'Crypto' ;

SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY 1; 

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1; # data was good

-- checking for any inconsistancies in date column
SELECT `date`
FROM layoffs_staging2;
-- changing the format of date
SELECT `date`,
STR_TO_DATE(`date`,'%m/%d/%Y')
FROM layoffs_staging2; 
-- updating in the table
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`,'%m/%d/%Y'); # Y for 2023 , y for 23 (year format)
-- recheck for update done 
SELECT `date`
FROM layoffs_staging2;

-- changing the datatype of date column
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- rechecking the modification
SELECT *
FROM layoffs_staging2;


-- 3. DEALING WITH NULL VALUES

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL; ##  no null values

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL OR industry =""; ## no null values

SELECT T1.company, T1.industry, T2.industry
FROM layoffs_staging2 T1
JOIN layoffs_staging2 T2
	ON t1.company = t2.company
    WHERE (T1.industry IS NULL or T1.industry = '' ) 
    AND t2. industry IS NOT NULL;
    
-- cheacking and removing  unnecessary columns and rows
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;  