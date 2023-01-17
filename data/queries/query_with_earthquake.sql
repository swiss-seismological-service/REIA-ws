WITH cte  AS MATERIALIZED (
	SELECT loss_calculation._oid
	FROM loss_calculation
		 JOIN loss_earthquakeinformation
		 ON loss_calculation._earthquakeinformation_oid=loss_earthquakeinformation._oid
	WHERE
		 loss_earthquakeinformation.originid = 'smi:ch.ethz.sed/scenario/Origin/Sierre_M5_8'
		 AND loss_calculation.status = 'COMPLETE'
		 AND loss_calculation._type = 'LOSS'
	ORDER BY loss_calculation.creationinfo_creationtime DESC
	LIMIT 1
)
SELECT
	sum(lr.loss_value * lr.weight) AS sum_injured, 
	tags_of_type.name 
FROM 
	(SELECT * FROM loss_riskvalue as lr 
	 WHERE lr._calculation_oid = (select cte._oid from cte) 
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
			AND loss_aggregationtag.name LIKE 'AG%'
	) AS tags_of_type 
	ON tags_of_type._oid = assoc.aggregationtag 
WHERE 
	tags_of_type.name LIKE 'AG%'
	AND lr.losscategory = 'NONSTRUCTURAL' 
	AND lr._calculation_oid = (select cte._oid from cte)
	AND lr._type = 'LOSS'
GROUP BY tags_of_type.name
				