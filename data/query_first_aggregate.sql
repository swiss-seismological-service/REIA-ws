SELECT 
	coalesce(results.sum_injured,0) as loss_value,
	tags.name as name
FROM (
	SELECT
		sum(lr.loss_value * lr.weight) AS sum_injured, 
		assoc.aggregationtag
	FROM 
		(SELECT * FROM loss_riskvalue as lr 
		 WHERE lr._calculation_oid = 23
		 AND lr.losscategory = 'STRUCTURAL' 
		 AND lr._type = 'LOSS'
		)
		AS lr 
	JOIN (
		SELECT * FROM
			loss_assoc_riskvalue_aggregationtag as assoc
		WHERE
			assoc.aggregationtype = 'CantonGemeinde'
	) AS assoc 
	ON 
		lr._oid = assoc.riskvalue 
		AND lr._calculation_oid = assoc._calculation_oid 
		AND lr.losscategory = assoc.losscategory
	GROUP BY assoc.aggregationtag, assoc.aggregationtype 
) AS results
RIGHT JOIN (
	SELECT 
		loss_aggregationtag._oid AS _oid, 
		loss_aggregationtag.type AS type, 
		loss_aggregationtag.name AS name 
	FROM 
		loss_aggregationtag 
	WHERE 
		loss_aggregationtag.type = 'CantonGemeinde' AND
		loss_aggregationtag.name LIKE 'AG%'
) AS tags 
ON 
	tags._oid = results.aggregationtag
ORDER BY
	tags.name
				