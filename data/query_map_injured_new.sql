SELECT 
	coalesce(results.sum_injured,0) as injured,
	tags.name as tag_name,
	municipalities.gid AS gid,
	municipalities.name as municipality_name,
	municipalities.geom AS the_geom
FROM (
	SELECT
		sum(lr.loss_value * lr.weight) AS sum_injured, 
		assoc.aggregationtag
	FROM 
		(SELECT * FROM loss_riskvalue as lr 
		 WHERE lr._calculation_oid = 23
		 AND lr.losscategory = 'NONSTRUCTURAL' 
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
RIGHT JOIN 
	municipalities
ON 
	municipalities.cantongeme = tags.name 
WHERE
	municipalities.gdektg = 'AG'
ORDER BY
	tags.name