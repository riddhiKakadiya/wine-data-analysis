wines = LOAD '/Users/riddhikakadiya/Desktop/BigDataProject/input/winemag-data-130k-v2.txt' USING PigStorage('\t') AS (country, designation, points, price, province, taster, title, variety, winery);

 
B = FOREACH wines GENERATE $6, $3;  
D = ORDER B BY $1 DESC;
C = LIMIT D 30;

DUMP C;