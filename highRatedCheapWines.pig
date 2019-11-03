/* high rated cheapest wines */

wines = LOAD '/Users/riddhikakadiya/Desktop/BigDataProject/input/winemag-data-130k-v2.txt' USING PigStorage('\t') AS (country, designation, points, price, province, taster, title, variety, winery);

 
B = FOREACH wines GENERATE $6, $2, $3;  
BB = FILTER B by $2 is not null;
C = ORDER BB BY $2 ASC;
D = ORDER C BY $1 DESC;
E = LIMIT D 25;

DUMP E;