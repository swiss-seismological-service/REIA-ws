SELECT
	sum(lr.loss_value * lr.weight) AS sum_injured, 
	tags_of_type.name 
FROM 
	(SELECT * FROM loss_riskvalue as lr 
	 WHERE lr._calculation_oid = 15
	 AND lr.losscategory = 'NONSTRUCTURAL' 
	 AND lr._type = 'LOSS'
	)
	AS lr 
	JOIN loss_assoc_riskvalue_aggregationtag AS assoc 
	ON lr._oid = assoc.riskvalue 
		AND lr._calculation_oid = assoc._calculation_oid 
		AND lr.losscategory = assoc.losscategory
	JOIN (
		SELECT 
			loss_aggregationtag._oid AS _oid, 
			loss_aggregationtag.type AS type, 
			loss_aggregationtag.name AS name 
		FROM 
			loss_aggregationtag 
		WHERE 
			loss_aggregationtag.type = 'CantonGemeinde'
-- 			AND loss_aggregationtag.name LIKE 'AG%'
	) AS tags_of_type 
	ON tags_of_type._oid = assoc.aggregationtag 
WHERE 
	tags_of_type.name LIKE 'AG%'
	AND lr.losscategory = 'NONSTRUCTURAL' 
	AND lr._calculation_oid = 15
	AND lr._type = 'LOSS'
GROUP BY tags_of_type.name
				