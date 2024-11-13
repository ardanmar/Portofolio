-- World Company Layoffs Data Cleaning

SELECT *
FROM layoffs;

CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT INTO layoffs_staging
SELECT *
FROM layoffs;

-- 1. Remove Duplicate

-- 1.1. Check Duplicate
WITH cte1 AS
(SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry,
total_laid_off, percentage_laid_off, `date`, stage, country,
funds_raised_millions) AS row_num
FROM layoffs_staging)

SELECT *
FROM cte1
WHERE row_num > 1;

-- 1.2. Delete Duplicate
CREATE TABLE `layoffs_staging_2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoffs_staging_2
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry,
total_laid_off, percentage_laid_off, `date`, stage, country,
funds_raised_millions) AS row_num
FROM layoffs_staging;

DELETE
FROM layoffs_staging_2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging_2;

-- 2. Data Standarization

-- 2.1. Trimming Company Naming
SELECT DISTINCT company, TRIM(company)
FROM layoffs_staging_2;

UPDATE layoffs_staging_2
SET company = TRIM(company);

-- 2.2. Edit Location Naming
SELECT DISTINCT location, country
FROM layoffs_staging_2
ORDER BY 1;

UPDATE layoffs_staging_2
SET location = 'Dusseldorf'
WHERE location LIKE '%sseldorf';

UPDATE layoffs_staging_2
SET location = 'Florianopolis'
WHERE location LIKE 'Florian%';

UPDATE layoffs_staging_2
SET location = 'Malmo'
WHERE location LIKE 'Malm%';

-- 2.3. Trimming Country Naming
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging_2
ORDER BY 1;

UPDATE layoffs_staging_2
SET country = TRIM(TRAILING '.' FROM country);

-- 2.4. Edit Industry Naming
SELECT DISTINCT industry
FROM layoffs_staging_2
ORDER BY 1;

SELECT *
FROM layoffs_staging_2
WHERE industry  LIKE 'Crypto%';

UPDATE layoffs_staging_2
SET industry = 'Crypto'
WHERE industry  LIKE 'Crypto%';

-- 2.5. Edit Date Format
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging_2;

UPDATE layoffs_staging_2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging_2
MODIFY `date` DATE;

-- 3. Populate Null or Blank values

SELECT DISTINCT industry
FROM layoffs_staging_2
ORDER BY 1;

SELECT *
FROM layoffs_staging_2
WHERE industry IS NULL OR industry = '';

UPDATE layoffs_staging_2
SET industry = NULL
WHERE industry = '';

SELECT t1.industry, t2.industry
FROM layoffs_staging_2 t1
JOIN layoffs_staging_2 t2
	ON t1.company = t2.company
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging_2 t1
JOIN layoffs_staging_2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- 4. Remove Columns or Rows

-- 4.1. Delete Row with Null laid off data
SELECT *
FROM layoffs_staging_2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging_2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- 4.2. Delete Unused Column
ALTER TABLE layoffs_staging_2
DROP COLUMN row_num;

SELECT *
FROM layoffs_staging_2;