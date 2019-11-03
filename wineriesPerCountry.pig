/*  wineries each counrty has. 


wines = LOAD '/input/winemag-data-130k-v2.txt' USING PigStorage('\t') AS (country, designation, points, price, province, taster, title, variety, winery);
countries = GROUP wines BY country;
wineries = FOREACH countries GENERATE $8;
WCNT = DISTINCT(wineries);
cnt = FOREACH wineries GENERATE $0, count(DISTINCT(wineries));
DUMP cnt;

*/



wines = LOAD '/Users/riddhikakadiya/Desktop/BigDataProject/input/winemag-data-130k-v2.txt' USING PigStorage('\t') AS (country, designation, points, price, province, taster, title, variety, winery);

 
A = GROUP wines BY $0;  
B = FOREACH A GENERATE $0, COUNT(wines.winery);  
DUMP B;
/*STORE B in 'wineryOut';*/