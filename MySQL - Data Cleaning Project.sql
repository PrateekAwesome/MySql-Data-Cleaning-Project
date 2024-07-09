-- Data Cleaning 

select * , row_number() over()
from layoffs ;

-- 1. Remove Duplicates
-- 2. Standardize the Data 
-- 3. Null Values or Blank Values 
-- 4. Remove any Columns or Rows (if necessary)

CREATE TABLE layoffs_staging 
LIKE layoffs ; 

SELECT * 
FROM layoffs_staging ; 

INSERT layoffs_staging 
SELECT * 
FROM layoffs ; 



-- Removing Duplicates 

SELECT * , 
ROW_NUMBER() OVER(
PARTITION BY company , location , industry , total_laid_off , percentage_laid_off , `date` , stage , country , funds_raised 
) AS row_num 
FROM layoffs_staging ; 

-- Creating CTE for Easier Calculation  

WITH duplicate_cte AS 
(
SELECT * , 
ROW_NUMBER() OVER(
PARTITION BY company , location , industry , total_laid_off , percentage_laid_off , `date` , stage , country , funds_raised ) AS row_num 
FROM layoffs_staging 
)
SELECT * 
FROM duplicate_cte 
WHERE row_num > 1 ; 

-- Checking if they are really duplicates 

select *
from layoffs_staging 
where company = 'Beyond Meat';

select *
from layoffs_staging 
where company = 'cazoo';

-- In Posgre SQL , we can delete things in CTE but in SQL , since delete is an update like command , we can't update a CTE 
-- So we would have to create  a new table , and delete it there 

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` text,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised` double DEFAULT NULL , 
  `row_num` int 
  
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * 
FROM layoffs_staging2 ; 

INSERT INTO layoffs_staging2
SELECT * , 
ROW_NUMBER() OVER(
PARTITION BY company , location , industry , total_laid_off , percentage_laid_off , `date` , stage , country , funds_raised ) AS row_num 
FROM layoffs_staging 
-- This was our CTE earlier 
; 

SELECT * 
FROM layoffs_staging2 
WHERE row_num > 1 or row_num < 1
 ; 

select * 
from layoffs_staging2
where company = 'Beyond Meat' or company = 'Cazoo' ; 

select *
from layoffs_staging2
where row_num > 2 ;

-- Standardizing Data 

SELECT company , TRIM(company)
from layoffs_staging2 ;  

select company , trim(company) from layoffs_staging2 
where company != trim(company)
;

UPDATE layoffs_staging2 
SET company = TRIM(company) ; 

select company , trim(company) from layoffs_staging2 
where company != trim(company) ; 
-- Now not existing , earlier they were 

SELECT DISTINCT industry 
FROM layoffs_staging2 
ORDER BY 1 ; 

select * 
from layoffs_staging2 
where industry='Transportation'; -- I thought it was wrong but it wasn't 


-- In tutorial it was unclean , but in our data it's clean alread

select distinct location 
from layoffs_staging2 
Order by 1 ; 

select `date` , Max(`date`) over(partition by substring(`date` , 6, 2))
from layoffs_staging2
order by 1 
 ; -- This one was just for practice , its not cleaning data 
 
-- Suppose if we had to change the date format of our date 
select `date`, 
str_to_date(`date` , '%m/%d/%Y')
from layoffs_staging ; -- This literally worked for them , we can do something else for ourselves -> actually their(tutorial) dataset is different than ours 
-- this worked for them because their date format was different dd/mm/yyyy and our is yyyy-mm-dd
-- lets change to their format then make this statement working , afterall we are practicing data cleaning whatever can come in actual projects

select `date` , concat(substring(`date`,9 , 2) , '/' , substring(`date` , 6 , 2) , '/' , substring(`date`,1,4)) as foramatted_date
from layoffs_staging2 
order by 1 
; 

-- lets update it and then re-update it back 
update layoffs_staging2 
set `date` = concat(substring(`date`,9 , 2) , '/' , substring(`date` , 6 , 2) , '/' , substring(`date`,1,4)) ; 

select * 
from layoffs_staging2 ; 

select `date`, 
str_to_date(`date` , '%m/%d/%Y')
from layoffs_staging ; -- Tutorial's statement is still not working - SAD LYF , Hari Hari , Biphale Janam Gonainu , Manushya Janam Paiyaa , Radha Krishna Naa Bhajiyaa , Janiyaa Suniyaa Vish Khainuuuuuu , Janiyaa Suniya Vish Khainuuu 

-- Rechange in our way 
select * 
from layoffs_staging2 ; 

select `date` , 
concat(substring(`date`,7,4),'-',substring(`date`,4,2),'-',substring(`date`,1,2)) 
from layoffs_staging2 ; 

-- again update table 
update layoffs_staging2
set `date` = concat(substring(`date`,7,4),'-',substring(`date`,4,2),'-',substring(`date`,1,2)) ;

-- Lets alter its data type now 
alter table layoffs_staging2 
modify column `date` date ; 

-- Great done 

-- Most Important Part => Handling Null and Blank Values 

select *
from layoffs_staging2 
where total_laid_off = '' and percentage_laid_off = '' ; -- such information is useless , and can be removed if told by owner of data

select * from layoffs_staging2 ; 

select * from layoffs_staging2 
where  industry = ''  ;

select * 
from layoffs_staging2 t1 
join layoffs_staging2 t2 
	on t1.company = t2.company
    and t1.location = t2.location 
where (t1.industry is null or t1.industry = '')
and t2.industry is not null ;

select * from layoffs_staging2
where total_laid_off = '' and percentage_laid_off = '' and funds_raised=''
; -- at present there is one company - tuft and needle , location = Phoenix , industry = retail , date = 2020-03-19 ,stage = aquired, country = united states , funds_raised = 0 , row_num = 1

-- lets delete as it seems unwanted data - no decesion can be taken out of it ( always delete with  permission )

select * from layoffs_staging2 
where company = 'Appsmith' ; 

delete 
from layoffs_staging2
where total_laid_off = '' and percentage_laid_off = '' and funds_raised ='' ;
-- 1 row(s) affected 

-- That's all for our Data Cleaning Project => Data 





