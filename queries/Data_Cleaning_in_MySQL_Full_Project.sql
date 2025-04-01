-- DATA CLEANING

select * from layoffs;
create table layoffs_staging like layoffs;
insert layoffs_staging 
select * from 
layoffs;

select * from layoffs_staging;

SELECT *,
ROW_NUMBER() OVER(PARTITION BY
company, location, industry, total_laid_off, percentage_laid_off, date , stage , country, funds_raised_millions) as row_num 
from layoffs_staging;
 
WITH duplicate_cte as
(SELECT *,
ROW_NUMBER() OVER(PARTITION BY
company, location, industry, total_laid_off, percentage_laid_off, date , stage , country, funds_raised_millions) as row_num 
from layoffs_staging)
select * from duplicate_cte where row_num > 1;


select * from layoffs_staging where company='Oyster';


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
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * from layoffs_staging2;

INSERT into layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(PARTITION BY
company, location, industry, total_laid_off, percentage_laid_off, date , stage , country, funds_raised_millions) as row_num 
from layoffs_staging;

select * from layoffs_staging2 where row_num > 1;

-- STANDARDIZING DATA

SELECT DISTINCT company, TRIM(company) from layoffs_staging2;

UPDATE layoffs_staging2 set company= TRIM(company);

SELECT DISTINCT industry from layoffs_staging2 
WHERE industry like 'Crypto%';

SELECT DISTINCT location from layoffs_staging2 order by 1;

SELECT DISTINCT country from layoffs_staging2 where country like 'United States%';

SELECT DISTINCT country,TRIM(TRAILING '.' from country) from layoffs_staging2 where country like 'United States%';

UPDATE layoffs_staging2 set country = TRIM(TRAILING '.' from country) where country like 'United States%';

SELECT date,STR_TO_DATE( date, '%m/%d/%Y') FROM layoffs_staging2;

UPDATE layoffs_staging2 set date=STR_TO_DATE( date, '%m/%d/%Y');

select * from layoffs_staging2;

ALTER TABLE layoffs_staging2 MODIFY COLUMN date DATE;

-- HANDLING NULL AND BLANK VALUES

SELECT * FROM layoffs_staging2 where industry is null or industry='';

select * from layoffs_staging2 where company='Airbnb';

select * from layoffs_staging2 t1 join layoffs_staging2 t2
on t1.company=t2.company 
where (t1.industry is null or t1.industry='') and t2.industry is not null;

update layoffs_staging2 set industry = 'Travel' where company='Airbnb';

select * from layoffs_staging2 where company LIKE 'Bally%';

select * from layoffs_staging2;

select * from layoffs_staging2 where total_laid_off is null and percentage_laid_off is null;

-- DELETING COLUMNS

DELETE from layoffs_staging2 where total_laid_off is null and percentage_laid_off is null;

SELECT * from layoffs_staging2;

ALTER TABLE layoffs_staging2 DROP COLUMN row_num;















 


 