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
PARTITION BY company, location, industry, total_laid_off, `date` #βάζουμε έτσι το date γιατί υπάρχει συνάρτηση date
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

-- Βλέπουμε ότι υπάρχουν NULL τιμές στην στήλη total_laid_off αλλά υπάρχουν και παρατηρήσεις που έχουν NULL τιμές και στην στήλη total_laid_off αλλά
-- και στην στήλη percentage_laid_off

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

-- Πρακτικά, αυτές οι παρατηρήσεις δεν μας είναι και πάρα πολύ χρήσιμες. Αυτές τις παρατηρήσεις πολύ αργότερα θα τις απομακρύνουμε

-- Ας πάμε να δούμε τις τιμές που έχει η στήλη industry

SELECT DISTINCT(industry)
FROM layoffs_staging2;

-- Βλέπουμε ότι υπάρχουν και blank και null τιμές. Πάμε να τα δούμε στο σύνολο δεδομένων

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL OR industry = ""; #έτσι περιγράφεται το blank

-- Καλό είναι αν μπορούμε να γεμίσουμε αυτές τις τιμές που είναι blank. Μπορούμε π.χ να δούμε αν έχει καταγραφεί κάπου αλλού το industry που ανήκει κάποια εταιρεία και
-- να βάλλουμε την τιμή.

-- Ας πάρουμε για παράδειγμα την Airbnb

SELECT *
FROM layoffs_staging2
WHERE company = "Airbnb";

-- Άρα βλέπουμε ότι έχει καταγραφεί σε ποιο industry ανήκει η Airbnb. Πάμε λοιπόν να γεμίσουμε το κενό

UPDATE layoffs_staging2
SET industry = "Travel"
WHERE company = "Airbnb" AND industry = "";

-- Πάμε να κάνουμε το ίδιο και για τις άλλες εταιρείες

SELECT *
FROM layoffs_staging2
WHERE company = "Bally's Interactive";

-- Εδώ δεν μπορούμε να κάνουμε κάτι

SELECT *
FROM layoffs_staging2
WHERE company = "Carvana";

-- Πάμε να το φτιάξουμε

UPDATE layoffs_staging2
SET industry = "Transportation"
WHERE company = "Carvana" AND industry = "";

SELECT *
FROM layoffs_staging2
WHERE company = "Juul";

-- Πάμε να το φτιάξουμε

UPDATE layoffs_staging2
SET industry = "Consumer"
WHERE company = "Juul" AND industry = "";

-- Αυτό που κάναμε παραπάνω μπορούμε να το κάνουμε και με λιγότερες γραμμές κώδικα. Δεν είναι μόνο οι λιγότερες γραμμές κωδικα είναι και το ότι αν είχαμε 500 εταιρείες
-- για τις οποίες συνέβαινε το ίδιο πράγμα τότε δεν θα βοήθαγε να κάνουμε αυτό που κάναμε παραπάνω.

-- Θα κάνουμε ένα self Join

SELECT *
FROM layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company #εδώ τελειώνει το join και πάμε να μας εμφανίσει αυτό που θέλουμε
WHERE (t1.industry IS NULL OR t1.industry = "") AND t2.industry IS NOT NULL;

-- Πάμε να δούμε τις στήλες που μας ενδιαφέρουν

SELECT t1.industry, t2.industry
FROM layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company #εδώ τελειώνει το join και πάμε να μας εμφανίσει αυτό που θέλουμε
WHERE (t1.industry IS NULL OR t1.industry = "") AND (t2.industry IS NOT NULL OR t2.industry != "");

-- Πάμε να βάλλουμε τώρα τις τιμές

UPDATE layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company #εδώ τελειώνει το join και πάμε να μας εμφανίσει αυτό που θέλουμε
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = "") AND (t2.industry IS NOT NULL OR t2.industry != "");

-- Πάμε τώρα να δούμε τώρα τι κάναμε

SELECT DISTINCT(industry)
FROM layoffs_staging2;

-- Βλέπουμε ότι πάλι έχουμε κάποια NULL και κάποια Blank. Πάμε να τα δούμε όμως και στο σύνολο των δεδομένων

SELECT *
FROM layoffs_staging2;

-- Για να λύσουμε το παραπάνω πρόβλημα θα πρέπει να κάνουμε τις blank τιμές NULL

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = "";

UPDATE layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company #εδώ τελειώνει το join και πάμε να μας εμφανίσει αυτό που θέλουμε
SET t1.industry = t2.industry
WHERE t1.industry IS NULL AND t2.industry IS NOT NULL;

-- Για να δούμε αν μας έχει ξεφύγει κάτι

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL;

-- Πάμε να δούμε αν έχουμε άλλες παρατηρήσεις από αυτή την εταιρεία

SELECT *
FROM layoffs_staging2
WHERE company = "Bally's Interactive";

-- Όχι είναι μόνο μια.

-- Πάμε να διαγράψουμε τις παρατηρήσεις που έχουν NULL τιμή και στην στήλη total_laid_off και percentage_laid_off. Αφού δεν μας δίνουν ουσιαστική πληροφορία

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

-- Πάμε να διαγράψουμε την στήλη row_num αφού δεν την χρειαζόμαστε

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- Explanatory Data Analysis

SELECT *
FROM layoffs_staging2;

-- Το ποσοστό των απολυμένων υπαλλήλων δεν βοηθάει και τόσο μιας και δεν ξέρουμε το μέγεθος της εταιρείας

-- Αν περιηγηθούμε λίγο στα δεδομένα θα δούμε ότι σε κάποιες εταιρείες έχουν γίνει απολύσεςι σε παραπάνω από μια μέρα. Οπότε καλό θα ήταν να δούμε πόσα άτομα έχουν απολυθεί
-- συνολικά από κάθε εταιρεία.

SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company;

-- Μπορούμε αν θέλουμε να τα ταξινομήσουμε με τέτοιο τρόπο ώστε δούμε ποια εταιρεία είχε τις πιο υψηλές απολύσεις

SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

-- Μπορούμε να κάνουμε το ίδιο και για το industry

SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

-- Μπορούμε να κάνουμε το ίδιο και με την χώρα

SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

-- Μπορούμε να βρούμε και το χρονικό περιθώριο των δεδομένων

SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

-- Άρα βλέπουμε ότι το χρονικό περιθώριο είναι 11/3/2020 έως και 6/3/2023

-- Μπορούμε να κάνουμε και αυτό που είδαμε παραπάνω αλλά με την ημερομηνία

SELECT `date`, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY `date`
ORDER BY 1 DESC; # αλλά με βάση την ημερομηνία αυτή την φορά

-- Η αλήθεια είναι ότι αυτό δεν βολεύει, οπότε το κάνουμε με βάση το έτος

SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

-- Μπορούμε να το κάνουμε έτσι ώστε να έχουμε και τον μήνα και το έτος (δεν έχει νόημα να βλέπουμε μόνο τον μήνα)

SELECT SUBSTRING(`date`,1,7) AS month_and_year, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY month_and_year;

-- Βλέπουμε ότι υπάρχει μια γραμμή που περιέχει NULL στην πρώτη στήλη οπότε την βγάζουμε

SELECT SUBSTRING(`date`,1,7) AS month_and_year, SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL #θα θέλαμε πάρα πολύ να μπορούμε να βάλλουμε `month_and_year` αλλά δεν γίνεται επειδή το where clause γίνεται πρώτα και μετά το SELECT
#οπότε δεν το καταλαβαίνει
GROUP BY month_and_year;

-- Μπορούμε να πάμε ένα βήμα παραπάνω τον παραπάνω κώδικα και να φτιάξουμε ένα cummulative sum με τις απολύσεις που έγιναν.

-- Θα φτιάξουμε και μια στήλη που θα δείχνει κάθε φορά το ποσό που προστέθηκε στο τρέχον άθροισμα

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

-- Μπορούμε να δούμε ότι στο τέλος του 2020 είχαμε συνολικά γύρω στις 81.000 απολύσεις ενώ στο τέλος του 2021 είχαμε γύρω στα 96k. Από ότι βλέπουμε το 2021 ήταν καλύτερη χρονιά
-- μιας και η αύξηση είναι πολύ μικρότερη από αυτή που είδαμε στο 2020. Επίσης, μπορούμε να δούμε ότι στο τέλος του 2022 είχαμε γύρω στις 260k απολύσεις, το οποίο σημαίνει
-- ότι η χρονιά αυτή ήταν πολύ χειρότερη σε σύγκριση με το 2021.

-- Αν θέλουμε να το κάνουμε αυτό αλλά για κάθε χώρα

-- Πάμε να δούμε τον κώδικα βήμα-βήμα

-- Θα φτιάξουμε τον κώδικα που θα υπολογίζει τον συνολικό αριθμό απολύσεων ανά μήνα σε κάθε χώρα

WITH SUM2 AS(
SELECT country,
	SUBSTRING(`date`,1,7) AS month_and_year,
    SUM(total_laid_off) AS SUM1
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY country, month_and_year #εδώ ουσιαστικά φτιάχνουμε ομάδες με βάση την χώρα και την ημερομηνία (σκέψου ότι έχουμε πολλές παρατηρήσεις με την ίδια χώρα
#αλλά άλλη ημερομηνία. Οπότε θέλουμε να αθροίσουμε όλες τις παρατηρήσεις που αντιστιχούν στην ίδια χώρα και το μόνο που αλλάζει είναι η ημερομηνία
)
SELECT country, month_and_year, SUM1,
SUM(SUM1) OVER (PARTITION BY country ORDER BY month_and_year ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS Cummulative_sum #στο Partition δεν βάλλαμε την
#ημερομηνία γιατί θέλουμε το άθροισμα να αλλάζει μόνο με βάση την χώρα. Επίσης βάλλαμε το άλλο για ασφάλεια. Αν ταξινομούσαμε τα δεδομένα με βάση αριθμητικά δεδομένα
#ή με βάση μοναδικές τιμές τότε δεν θα χρειαζόταν.
FROM SUM2;

-- Πάμε να δούμε ποιες εταιρείες είχαν τις περισσότερες απολύσεις ανά έτος

WITH cte_1 AS(
SELECT company, YEAR(`date`) AS years,
SUM(total_laid_off) AS SUM1
FROM layoffs_staging2
WHERE total_laid_off IS NOT NULL AND YEAR(`date`) IS NOT NULL
GROUP BY company, years
)# με αυτό μπορούμε και υπολογίζουμε τις συνολικές απολύσεις ανά έτος
SELECT company, years, sum1,
DENSE_RANK() OVER (PARTITION BY years ORDER BY SUM1 DESC) AS Ranking
FROM cte_1
ORDER BY years ASC;


WITH cte_1 AS(
SELECT company, YEAR(`date`) AS years,
SUM(total_laid_off) AS SUM1
FROM layoffs_staging2
WHERE YEAR(`date`) IS NOT NULL
GROUP BY company, years
)# με αυτό μπορούμε και υπολογίζουμε τις συνολικές απολύσεις ανά έτος
SELECT company, years, SUM1,
DENSE_RANK() OVER (PARTITION BY years ORDER BY SUM1 DESC) AS Ranking
#Με την παραπάνω εντολή ουσιαστικά φτιάχνουμε "ομάδες" με βάση το έτος και κάνουμε ranking μέσα στην ομάδα με βάση τις συνολικές απολύσεις
FROM cte_1
ORDER BY Ranking ASC; #αυτό το χρειαζόμαστε γιατί έτσι μπορούμε να δούμε ανά έτος ποια ήταν πρώτη, μετά ανα έτος ποια ήταν δεύτερη κ.ο.κ

-- Θέλουμε σε κάθε έτος να έχουμε τις top 5 σε συνολικές απολύσεις

WITH cte_1 AS(
SELECT company, YEAR(`date`) AS years,
SUM(total_laid_off) AS SUM1
FROM layoffs_staging2
WHERE YEAR(`date`) IS NOT NULL
GROUP BY company, years
),#Εδώ υπολογίζουμε τις συνολικές απολύσεις ανά έτος
ranking_companies AS(
SELECT company, years, SUM1,
DENSE_RANK() OVER (PARTITION BY years ORDER BY SUM1 DESC) AS Ranking
FROM cte_1
) #εδώ βάζουμε ένα ακόμη CTE
SELECT *
FROM ranking_companies
WHERE Ranking <= 5
;

#Εδώ το κάναμε έτσι γιατί δεν μπορούμε να βάλουμε το WHERE ούτε μετά το ORDER BY ούτε πριν από αυτό. Δεν μπορούμε να το βάλλουμε το WHERE πριν από το ORDER BY γιατί
#δεν μπορούμε να χρησιμοποιηθεί το Ranking ενώ δεν έχει φτιαχτεί. Αυτό συμβαίνει γιατί πρώτα εκτελείται το WHERE και μετά η εντολή DENSE_RANK() OVER (PARTITION BY
#years ORDER BY SUM1 DESC) AS Ranking





