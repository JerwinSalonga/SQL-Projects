-- Exploratory Data Analysis

SELECT *
FROM layoffs_staging2;

-- check the start and end of the dataset
SELECT MAX(`date`) AS end_date, MIN(`date`) AS starting_date
FROM layoffs_staging2;

-- companies that has 100% laid off their employees (by funds raised)
SELECT * 
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

-- check how many were laid off by eBay
SELECT *
FROM layoffs_staging2
WHERE company LIKE 'eBay';

-- total laid off by company
SELECT company, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

-- total laid off by industry
SELECT industry, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

-- total laid off by consumer industry, and by year
SELECT industry, YEAR(`date`) AS year, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
WHERE industry = 'Consumer'
GROUP BY industry, YEAR(`date`)
ORDER BY year DESC;

-- total laid off by country
SELECT country, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

-- total laid off by date
SELECT YEAR(`date`), SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

-- date that has the most laid offs
SELECT `date`, MAX(total_laid_off) as max_laid_off
FROM layoffs_staging2
GROUP BY `date`
ORDER BY 2 DESC
LIMIT 1;

-- total laid off by stage
SELECT stage, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

-- rolling total by year and month
WITH rolling_total AS (
SELECT SUBSTRING(`date`,1,7) AS `month`, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `month`
ORDER BY 1
)
SELECT `month`, 
total_laid_off,
SUM(total_laid_off) OVER(ORDER BY `month`) AS rolling_total
FROM rolling_total;

-- ranking by total laid off, partitioned by year
WITH company_year AS (
SELECT company, YEAR(`date`) AS year, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
), company_year_rank AS (
SELECT *,
DENSE_RANK() OVER(PARTITION BY year ORDER BY total_laid_off DESC) AS ranking
FROM company_year
WHERE year IS NOT NULL
)
SELECT *
FROM company_year_rank
WHERE ranking <= 5;












