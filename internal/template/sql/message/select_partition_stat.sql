SELECT COALESCE(MAX(`offset`), 0) as max_offset , COALESCE(MIN(`offset`), 0) as min_offset , count(*) as total 
FROM `%s`
