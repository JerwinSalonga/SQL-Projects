CREATE database world_layoffs;
use world_layoffs;

-- Remove Duplicates

-- create staging (buffer between the source data)
CREATE TABLE layoffs_staging
LIKE layoffs;

-- populating the new table
INSERT layoffs_staging
SELECT *
FROM layoffs;

-- assign row numbers to each unique rows
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_numbers
FROM layoffs_staging;

-- see if there are any duplicates (return 2 if there are duplicates)
WITH duplicate_cte AS (
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_numbers
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_numbers > 1;

-- double-check if the outputs really are duplicates.
SELECT *
FROM layoffs_staging
WHERE company IN ('Casper', 'Cazoo', 'Hibob', 'Wildlife Studios', 'Yahoo');

-- creating 2nd staging table to delete duplicates
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
  `row_numbers` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- populating the new staging table with row numbers
INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_numbers
FROM layoffs_staging;

-- deleting the duplicate rows
DELETE
FROM layoffs_staging2
WHERE row_numbers > 1;


-- Standardize the Data
SELECT * 
FROM layoffs_staging2;

-- removing extra white space
SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

-- checking for inconsistencies in industry column
SELECT DISTINCT(industry)
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

-- standardize the industry that has 'Crypto'
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- checking for inconsistencies in country column
SELECT DISTINCT(country)
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT country
FROM layoffs_staging2
WHERE country LIKE 'United States%';

-- standardize the country that has 'United States'
UPDATE layoffs_staging2
SET country = trim(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- converting date column from text to date datatype
SELECT `date`,
str_to_date(`date`, '%m/%d/%Y') AS new_date
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


-- Null Values or Blank Values

-- check if there are null or blank values in the industry column
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

SELECT *
FROM layoffs_staging2
WHERE company LIKE "Bally's%";

-- set the blank values first into NULL before populating
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Airbnb';

-- created a self join, to see if there are available data in NULL or blank values
SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- impute the available data into NULL values
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry LIKE '')
AND t2.industry IS NOT NULL;

-- remove the rows that has no values (both total laid off and percentage laid off that is null)
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

ALTER TABLE layoffs_staging2
DROP COLUMN row_numbers;
