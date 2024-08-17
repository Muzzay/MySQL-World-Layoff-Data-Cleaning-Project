-- DATA CLEANING PROJECT

SELECT *
FROM layoffs;

-- PROCESS IN CLEANING DATA
-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null Values or Blank Values
-- 4. Remove Any Columns

CREATE TABLE layoff_staging
LIKE layoffs;

SELECT * 
FROM layoff_staging;

INSERT layoff_staging
SELECT * 
FROM layoffs;

-- It's always a best practise not to work directly on the raw data. Create a duplicate table and work with that

# ----------------------------------- CLEANING UP DUPLICATES -----------------------------------------------------------------


WITH duplicate_CTE AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, 
total_laid_off, percentage_laid_off, 
'date', stage, country, 
funds_raised_millions) AS row_num
FROM layoff_staging
)
SELECT * 
FROM duplicate_cte
WHERE row_num > 1;

SELECT * 
FROM layoff_staging
WHERE company = 'Casper';

-- You cannot update (Insert/delete/etc) a CTE as such we need to device new way of deleting these duplicates
-- One way of doing so it to create a staging 2 of the table, filter and delete it directly from the table. 

CREATE TABLE `layoff_staging2` (
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

SELECT *
FROM layoff_staging2;


INSERT INTO layoff_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, 
total_laid_off, percentage_laid_off, 
'date', stage, country, 
funds_raised_millions) AS row_num
FROM layoff_staging;

DELETE 
FROM layoff_staging2
WHERE row_num > 1;

SELECT *
FROM layoff_staging2;

# ----------------------------------- STANDARDIZING DATA ---------------------------------------------------------------------------------------------
-- 1. TRIM (Taking away any white spaces before and after the texts)
SELECT company, TRIM(company)
FROM layoff_staging2;

#Used to update row values
UPDATE layoff_staging2
SET company = TRIM(company);

SELECT *
FROM layoff_staging2;

-- 2. Standardizing Industry names
#Checking for duplicated column (industry) names
SELECT industry
FROM layoff_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoff_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT country
FROM layoff_staging2
order by 1;

UPDATE layoff_staging2
SET country = 'United States'
WHERE country LIKE 'United States%';

#Advanced way of trim a specific character 
-- UPDATE layoff_staging2
-- SET country =  TRIM(trailing '.' FROM country)
-- WHERE country LIKE 'United State%' 
 
-- 3. Changing data type for dates first from a string to date format
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoff_staging2;

UPDATE layoff_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT `date`
FROM layoff_staging2;

-- After converting to a date format, then you change the data type for the date
ALTER TABLE layoff_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoff_staging2;

# ----------------------------------------------- NULL & BLANK VALUES -------------------------------------------------------------------------
UPDATE layoff_staging2
SET industry = NULL
WHERE industry = '';


SELECT * 
FROM layoff_staging2
WHERE industry IS NULL OR industry = '';

SELECT company, industry
FROM layoff_staging2
WHERE company LIKE '%Bally%';

#First we joined the table on itself to identify how many rows are missing under the industry in each company. Once we did that, we updated the table such that
#for each industry column which is not null under such company, should be updated with whatever value is assigned to it. 
SELECT st1.company, st1.location, st1.industry,
	   st2.company, st2.location, st2.industry
FROM layoff_staging2 st1
JOIN layoff_staging2 st2
ON st1.company = st2.company
WHERE st1.industry IS NULL OR st1.industry = ''
AND st2.industry IS NOT NULL;

UPDATE layoff_staging2 st1
JOIN layoff_staging2 st2
ON st1.company = st2.company
SET st1.industry = st2.industry
WHERE st1.industry IS NULL 
AND st2.industry IS NOT NULL;


#------------------------------------- DELETING ANY UNECESSARY COLUMNS -----------------------------------------------------------------------

SELECT *
FROM layoff_staging2
WHERE percentage_laid_off IS NULL
AND total_laid_off IS NULL;

DELETE
FROM layoff_staging2
WHERE percentage_laid_off IS NULL
AND total_laid_off IS NULL;

# DROPPING A COLUMN
ALTER TABLE layoff_staging2
DROP row_num;


# -------------------------------- EXPLORATORY ANALYSIS -----------------------------------------------------
-- Exploratory analysis is more about playing through the data to have a feel the story the data is telling without necessarily having any motive behind the analysis

SELECT DISTINCT industry, sum(total_laid_off)
FROM layoff_staging2
GROUP BY industry
ORDER BY sum(total_laid_off) DESC;

SELECT *
FROM layoff_staging2
WHERE industry = 'Consumer'
ORDER BY `date` ASC,  total_laid_off DESC;

SELECT MIN(`date`), MAX(`date`)
FROM layoff_staging2;

SELECT country, sum(total_laid_off)
FROM layoff_staging2
GROUP BY country
ORDER BY 2 DESC;

SELECT company, industry, sum(total_laid_off), country, `date`
FROM layoff_staging2
WHERE YEAR(`date`) = 2023 AND industry = 'Other'
GROUP BY company, industry, country, `date`
ORDER BY sum(total_laid_off) DESC;

SELECT industry, sum(total_laid_off)
FROM layoff_staging2
WHERE YEAR(`date`) = 2023 AND industry = 'Other'
GROUP BY industry;

SELECT YEAR(`date`), sum(total_laid_off)
FROM layoff_staging2
WHERE industry = 'Other'
GROUP BY YEAR(`date`), industry;

SELECT *
FROM layoff_staging2
WHERE company = 'Blackbaud';

SELECT month(`date`) AS `Date_Laid_Off`, sum(total_laid_off)
FROM layoff_staging2
GROUP BY `Date_Laid_Off`;


#Created a CTE called rolling_totoal to make the expression more readable. 
-- Selected only the year and month from the Date Column, and sum of people laid off, ordered by the Y&M in ASC order
-- Called out the columns (1,2,3) in the rolling order by month

WITH rolling_total AS (
		SELECT substring(`date`,1,7) AS 'Year&Month', sum(total_laid_off) AS laid_off
		FROM layoff_staging2
		WHERE substring(`date`,1,7) IS NOT NULL
		GROUP BY `Year&Month`
		ORDER BY 1 ASC
)

SELECT `Year&Month`, laid_off, sum(laid_off) over(order by `Year&Month`) AS Rolling_Total
FROM rolling_total;

WITH rolling_year_total AS (
		SELECT company, substring(`date`,1,7) AS 'Year&Month', sum(total_laid_off) AS laid_off
		FROM layoff_staging2
		WHERE substring(`date`,1,7) IS NOT NULL
		GROUP BY company, `Year&Month`
		ORDER BY 1 ASC
)

SELECT company, `Year&Month`, laid_off, sum(laid_off) over(order by `Year&Month`) AS Rolling_Total
FROM rolling_year_total;


SELECT company, YEAR(`date`), sum(total_laid_off)
FROM layoff_staging2
GROUP BY company, YEAR(`date`)
ORDER BY company ASC;


WITH 
Company_Year (Company, Years, Total_Laid_Off) AS 
	(SELECT company, YEAR(`date`), sum(total_laid_off) 
	FROM layoff_staging2
	GROUP BY company, YEAR(`date`)
	), 
Company_Ranking AS
	( SELECT *, DENSE_RANK() OVER (PARTITION BY Years ORDER BY total_laid_off DESC) AS Ranking
	FROM Company_Year
	WHERE years is not null
	)
SELECT *
FROM Company_Ranking
WHERE ranking <= 5;

WITH funds_ranking AS 
	(
	SELECT company, 
    industry, YEAR(`date`), 
    funds_raised_millions, sum(funds_raised_millions),
    dense_rank() OVER (partition by company order by funds_raised_millions) AS funds_ranking
	FROM layoff_staging2
    GROUP BY company
	ORDER BY funds_raised_millions DESC
	)
    
SELECT *
FROM funds_ranking;


SELECT *
FROM layoff_staging2
WHERE company = 'Netflix' or company = 'Google' or company = 'Microsoft';

SELECT company, industry, sum(percentage_laid_off), sum(total_laid_off), sum(funds_raised_millions)
FROM layoff_staging2
GROUP BY company, industry
ORDER BY sum(funds_raised_millions) DESC
LIMIT 10;

SELECT industry, avg(percentage_laid_off), sum(total_laid_off), sum(funds_raised_millions)
FROM layoff_staging2
GROUP BY industry
ORDER BY sum(funds_raised_millions) DESC
LIMIT 10;






