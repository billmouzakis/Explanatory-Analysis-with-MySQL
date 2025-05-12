-- We will use the data from the file  layoffs.csv

-- Let's see our data

SELECT *
FROM layoffs;

-- The first step is to remove the duplicates

-- The second step is to standardize the data (correct spelling errors etc.)

-- The third step is to handle null or blank values

-- The fourth step is to remove unnecessary columns

-- First of all, it is very helpful to copy the data into another table so that if we do a mistake we can have a back up of the original data

CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT layoffs_staging
SELECT *
FROM layoffs;

-- Let's see the duplicate values

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, `date`
) AS row_num
FROM layoffs_staging;

-- Duplicate values have row_num greater than 1

-- We will create a CTE in order to remove duplicate values

WITH duplicate_CTE AS(

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, `date` ) AS row_num
FROM layoffs_staging

)

SELECT *
FROM duplicate_CTE
WHERE row_num > 1;

-- Let's see for example the records which correspond to the company Oda

SELECT *
FROM layoffs_staging
WHERE company = "Oda";

-- These observations are not really duplicates. So in order to get real duplicates we have toinclude all the columns in the partition

WITH duplicate_CTE AS(

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions ) AS row_num
FROM layoffs_staging

)

SELECT *
FROM duplicate_CTE
WHERE row_num > 1;

-- Let's see what happens in Casper

SELECT *
FROM layoffs_staging
WHERE company = "Casper";

-- From what we can see only the the first and the third row are real duplicates

-- Attention! We want to remove only one of them

-- Let's remove the remove the duplicates

WITH duplicate_CTE AS(

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions ) AS row_num
FROM layoffs_staging

)

DELETE
FROM duplicate_CTE
WHERE row_num > 1;

-- We got an error because we can't update a CTE and deleting is an update. So in order to get what we want we have to create a new table without the duplicates

-- Let's create the new table

CREATE TABLE layoffs_staging2 (
company text,
location text,
industry text,
total_laid_off INT,
percentage_laid_off text,
`date` text,
stage text,
country text,
funds_raised_millions int,
row_num INT
);

INSERT INTO layoffs_staging2
SELECT company,
location,
industry,
total_laid_off,
percentage_laid_off,
`date`,
stage,
country,
funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions )
            AS row_num
	FROM
		layoffs_staging;
        
SELECT *
FROM layoffs_staging2;

-- Let's select the data we want to remove

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

-- Let's delete them

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

-- Let's check for duplicates

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

-- Let's standardize the data

SELECT *
FROM layoffs_staging2;

-- For example we can see that the first two observations in the first column (company) have a space in the beginning

-- Let's see all the values the column company takes

SELECT DISTINCT(company)
FROM layoffs_staging2;

-- From what we can see there are two companies in which we should remove the space

SELECT company, TRIM(company)
FROM layoffs_staging2;

-- Let's do the appropriate changes

UPDATE layoffs_staging2
SET company = TRIM(company);

-- Let's have a look of the dataset

SELECT *
FROM layoffs_staging2;

-- Let's see what values does the industry column have

SELECT DISTINCT(industry)
FROM layoffs_staging2
ORDER BY 1;

-- 1) We can see that there are null and blank values.

-- 2) The values Crypto, Crypto Currency, CryptoCurrency are basically the same. We should fix this because we may have problems in the future visualizations

UPDATE layoffs_staging2
SET industry = "Crypto"
WHERE industry LIKE "Crypto%";

-- Let's see if we are ok now

SELECT DISTINCT(industry)
FROM layoffs_staging2;

-- Let's see the column location

SELECT DISTINCT(location)
FROM layoffs_staging2;

-- We don't see any serious problem

-- Let's do the same for the country column

SELECT DISTINCT(country)
FROM layoffs_staging2;

-- We can see the values "United States" and "United States." and we should fix this.

UPDATE layoffs_staging2
SET country = "United States"
WHERE country LIKE "United States%";

-- Because we have dates it is very possible that we may want to use Time Series. In order to do this we have to make sure that the column date is in date format and not text format

SHOW FIELDS
FROM layoffs_staging2;

-- Let's cahnge the fromat of the column date

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- Let's check the format

SHOW FIELDS
FROM layoffs_staging2;

-- Let's see the NULL values. These values mainly occur in the columns total_laid_off and percentage_laid_off so we will focus on these columns.

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL;

-- We can see that in the column total_laid_off has null values but we can see that both columns total_aid_off and percentage_laid_off have null values.

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

-- Basically these observations are not very usefull so we will remove them

-- Let's see what values does the column industry have

SELECT DISTINCT(industry)
FROM layoffs_staging2;

-- We can see that there are both blank and null values. Let's see them in the whole dataset

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL OR industry = "";

-- It would be beneficial to fill these values which are blank. For example, we can look if the industry of a specific company has been recorded in another row and
-- fill the corresponding blank values.

-- Let's take for example the company Airbnb

SELECT *
FROM layoffs_staging2
WHERE company = "Airbnb";

-- As we can see the industry which Airbnb belongs is recorded in another observation.

UPDATE layoffs_staging2
SET industry = "Travel"
WHERE company = "Airbnb" AND industry = "";

-- Let's do the same but for the other companies

SELECT *
FROM layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = "") AND (t2.industry IS NOT NULL AND t2.industry != "");

-- Let's select the columns we want to see

SELECT t1.industry, t2.industry
FROM layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = "") AND (t2.industry IS NOT NULL AND t2.industry != "");

-- Let's go and put some values

UPDATE layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = "") AND (t2.industry IS NOT NULL AND t2.industry != "");

-- Let's see what we did

SELECT DISTINCT(industry)
FROM layoffs_staging2;

-- We can see that we still have some null and blank values

SELECT *
FROM layoffs_staging2;

-- In order to solve this problem we have to turn blank values into null values

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = "";

UPDATE layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL AND t2.industry IS NOT NULL;

-- Let's see what we did

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL;

-- Let's see if we have other observations from this company

SELECT *
FROM layoffs_staging2
WHERE company = "Bally's Interactive";

-- We want to occupy ourselves with the lay-offs. For this reason we don't want to have observations which have null values in both total_laid_off and percentage_laid_off
-- columns. So we will remove them

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

-- We will delete the column row_num because we don't need it

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- Explanatory Data Analysis

SELECT *
FROM layoffs_staging2;

-- The percentage_laid_off doesn't help us very much because we don't know how large the company is.

-- If we explore our data we can see that there companies which made layoffs more than one day. So it would be very helpful to see how many people have bee laid-off in total
-- for every company

SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company;

-- We can order them to see which company had the most laid-offs

SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

-- We can do the same but for the industry

SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

-- We can do the same but based on the country

SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

-- Let's find the time period of the data

SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

-- Άρα βλέπουμε ότι το χρονικό περιθώριο είναι 11/3/2020 έως και 6/3/2023

-- We can do the same but based on the date

SELECT `date`, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY `date`
ORDER BY 1 DESC;

-- The truth is that this isn't very helpful. So we will do the same but based on the year

SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

-- We can do the same but based on the specific month of the year

SELECT SUBSTRING(`date`,1,7) AS month_and_year, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY month_and_year;

-- We will get the part of the data that doesn't have any null value in the date column

SELECT SUBSTRING(`date`,1,7) AS month_and_year, SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY month_and_year;

-- We can calculate the cumulative sum of the total lay-offs

WITH total_laid_off_per_time AS
(
SELECT SUBSTRING(`date`,1,7) AS month_and_year,
       SUM(total_laid_off) AS sum_t
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY month_and_year
)
SELECT month_and_year, sum_t,
		SUM(sum_t) OVER (ORDER BY month_and_year ASC) AS Cummulative_sum
FROM total_laid_off_per_time;

-- We can see that in the end of the year 2020 we had around 81k layoffs although in the end of the year 2021 we had around 96k. As we can see the year 2021 was better
-- because the increasement was way lower than the year 2020. Also, we can see that in the end of the year 2022 we had around 260k lay-offs which means that this year
-- was worse than the year 2021.

-- Let's do the same but for the country

WITH SUM2 AS(
SELECT country,
	SUBSTRING(`date`,1,7) AS month_and_year,
    SUM(total_laid_off) AS SUM1
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY country, month_and_year
)
SELECT country, month_and_year, SUM1,
SUM(SUM1) OVER (PARTITION BY country ORDER BY month_and_year ASC) AS Cummulative_sum
FROM SUM2;

-- Let's see which companies had the most lay-offs every year

WITH cte_1 AS(
SELECT company, YEAR(`date`) AS years,
SUM(total_laid_off) AS SUM1
FROM layoffs_staging2
WHERE total_laid_off IS NOT NULL AND YEAR(`date`) IS NOT NULL
GROUP BY company, years
)
SELECT company, years, sum1,
DENSE_RANK() OVER (PARTITION BY years ORDER BY SUM1 DESC) AS Ranking
FROM cte_1
ORDER BY years ASC;

-- Let's rank every country based on the total lay-offs in every year

WITH cte_1 AS(
SELECT company, YEAR(`date`) AS years,
SUM(total_laid_off) AS SUM1
FROM layoffs_staging2
WHERE YEAR(`date`) IS NOT NULL
GROUP BY company, years
)
SELECT company, years, SUM1,
DENSE_RANK() OVER (PARTITION BY years ORDER BY SUM1 DESC) AS Ranking
FROM cte_1
ORDER BY Ranking ASC;

-- Let's see the top five companies based of the total lay-offs for every year

WITH cte_1 AS(
SELECT company, YEAR(`date`) AS years,
SUM(total_laid_off) AS SUM1
FROM layoffs_staging2
WHERE YEAR(`date`) IS NOT NULL
GROUP BY company, years
),
ranking_companies AS(
SELECT company, years, SUM1,
DENSE_RANK() OVER (PARTITION BY years ORDER BY SUM1 DESC) AS Ranking
FROM cte_1
)
SELECT *
FROM ranking_companies
WHERE Ranking <= 5
;




