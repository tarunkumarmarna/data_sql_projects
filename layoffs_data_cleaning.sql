-- Data cleaning project

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

