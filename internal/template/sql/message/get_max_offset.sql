SELECT COALESCE(MAX(`offset`), 0) as max_offset
FROM `%s`
