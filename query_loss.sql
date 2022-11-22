SELECT 
	sum(values_of_type.loss_value * values_of_type.weight) AS mean, 
	values_of_type.tag_name 
FROM (
	SELECT 
		loss_riskvalue.loss_value AS loss_value, 
		loss_riskvalue.eventid AS eventid, 
		tags_of_type.name AS tag_name, 
		loss_riskvalue.weight AS weight
	FROM 
		loss_riskvalue 
	JOIN 
		loss_assoc_riskvalue_aggregationtag 
	ON 
		loss_riskvalue._oid = loss_assoc_riskvalue_aggregationtag.riskvalue 
	JOIN (
		SELECT 
			loss_aggregationtag._oid AS _oid, 
			loss_aggregationtag.type AS type, 
			loss_aggregationtag.name AS name 
		FROM 
			loss_aggregationtag 
		WHERE 
			loss_aggregationtag.type = 'Canton'
	) AS tags_of_type 
	ON tags_of_type._oid = loss_assoc_riskvalue_aggregationtag.aggregationtag 
		WHERE loss_riskvalue._calculation_oid = 1 
		AND loss_riskvalue._type = 'lossvalue'
-- 		AND loss_riskvalue.losscategory in ('STRUCTURAL', 'NONSTRUCTURAL')
		AND loss_riskvalue.losscategory = 'STRUCTURAL'
		ORDER BY tags_of_type.name, loss_riskvalue.eventid
	) AS values_of_type 
GROUP BY values_of_type.tag_name
ORDER BY values_of_type.tag_name