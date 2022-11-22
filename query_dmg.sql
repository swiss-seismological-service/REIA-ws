SELECT 
	sum(values_of_type.dg1_value * values_of_type.weight) AS dg1, 
	sum(values_of_type.dg2_value * values_of_type.weight) AS dg2, 
	sum(values_of_type.dg3_value * values_of_type.weight) AS dg3, 
	sum(values_of_type.dg4_value * values_of_type.weight) AS dg4, 
	sum(values_of_type.dg5_value * values_of_type.weight) AS dg5, 
	values_of_type.tag_name 
FROM (
	SELECT 
		loss_riskvalue.dg1_value AS dg1_value,
		loss_riskvalue.dg2_value AS dg2_value, 
		loss_riskvalue.dg3_value AS dg3_value, 
		loss_riskvalue.dg4_value AS dg4_value, 
		loss_riskvalue.dg5_value AS dg5_value, 
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
		WHERE loss_riskvalue._calculation_oid = 2 
		AND loss_riskvalue._type = 'damagevalue'
		AND loss_riskvalue.losscategory = 'STRUCTURAL'
		ORDER BY tags_of_type.name, loss_riskvalue.eventid
	) AS values_of_type 
GROUP BY values_of_type.tag_name
ORDER BY values_of_type.tag_name