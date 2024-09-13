-- cleaning the data
   -- 1. Remove duplicates
   -- 2. Standadise the data
   -- 3. Null values or blank values
   -- 4. Remove extra columns

-- changing the data types
ALTER TABLE initial_data 
MODIFY COLUMN CustomerID INT,
MODIFY COLUMN Name TEXT,
MODIFY COLUMN Email TEXT,
MODIFY COLUMN PhoneNumber VARCHAR(30),
MODIFY COLUMN Address TEXT,
MODIFY COLUMN PurchaseAmount DOUBLE,
MODIFY COLUMN PurchaseDate DATE, 
MODIFY COLUMN BookTitle TEXT;

-- creating a temporary table to do changes (staging)
create table initial_data_staging
like initial_data;

select *
from initial_data_staging;

insert initial_data_staging
select *
from initial_data ;                    -- coping data from the original dataset

-- creating row_num with partitions
select *,
row_number() over (
partition by CustomerID, name, Email, PhoneNumber, Address, PurchaseAmount, PurchaseDate, BookTitle) as row_num
from initial_data_staging;

-- creating a CTE for the rows and checking if there is any duplicate
with duplicate_cte as 
(
select *,
row_number() over (
partition by CustomerID, name, Email, PhoneNumber, Address, PurchaseAmount, PurchaseDate, BookTitle) as row_num
from initial_data_staging
)
select *
from duplicate_cte
where row_num >1;
-- there were no duplicates

-- checking for blank or null values ( there are no null values in the dataset only blanks are there)
select *
from initial_data_staging
where PhoneNumber = '' and Email = '' and address = ''; -- we will delete these rows as no cantact info is available for these

-- deleting the unwanted rows
delete 
from initial_data_staging
where PhoneNumber = '' and Email = '' and address = '';

-- Update the PhoneNumber column to the standardized format
UPDATE initial_data_staging
SET PhoneNumber = 
    CONCAT(
        '(',
        SUBSTRING(REGEXP_REPLACE(PhoneNumber, '[^0-9]', ''), 1, 3), -- Area code
        ') ',
        SUBSTRING(REGEXP_REPLACE(PhoneNumber, '[^0-9]', ''), 4, 3), -- Central office code
        '-',
        SUBSTRING(REGEXP_REPLACE(PhoneNumber, '[^0-9]', ''), 7, 4), -- Line number
        IF(
            LENGTH(REGEXP_REPLACE(PhoneNumber, '[^0-9]', '')) > 10 AND
            LOCATE('x', PhoneNumber) > 0,
            CONCAT(' x', SUBSTRING(PhoneNumber, LOCATE('x', PhoneNumber) + 1)), -- Extension
            ''
        )
    )
WHERE PhoneNumber REGEXP '[0-9]';

-- Update the PhoneNumber column to the standardized format and removing the extra values at the end
UPDATE initial_data_staging
SET PhoneNumber = 
    CONCAT(
        '(',
        SUBSTRING(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(PhoneNumber, '(', ''), ')', ''), '-', ''), '.', ''), 'x', ''), 1, 3),
        ') ',
        SUBSTRING(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(PhoneNumber, '(', ''), ')', ''), '-', ''), '.', ''), 'x', ''), 4, 3),
        '-',
        SUBSTRING(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(PhoneNumber, '(', ''), ')', ''), '-', ''), '.', ''), 'x', ''), 7, 4),
        IF(
            LOCATE('x', PhoneNumber) > 0,
            CONCAT(' x', LEFT(
                SUBSTRING(
                    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(PhoneNumber, '(', ''), ')', ''), '-', ''), '.', ''), 'x', ''),
                    LOCATE('x', PhoneNumber) + 1,
                    LENGTH(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(PhoneNumber, '(', ''), ')', ''), '-', ''), '.', ''), 'x', '')) - LOCATE('x', PhoneNumber)
                ),
                4
            )),
            ''
        )
    )
WHERE PhoneNumber REGEXP '[0-9]';



