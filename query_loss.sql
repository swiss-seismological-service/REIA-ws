SELECT 
	sum(loss_riskvalue.loss_value * loss_riskvalue.weight) AS sum_injured, 
	tags_of_type.name 
FROM 
	loss_riskvalue 
	JOIN loss_assoc_riskvalue_aggregationtag ON loss_riskvalue._oid = loss_assoc_riskvalue_aggregationtag.riskvalue 
	JOIN (
		SELECT 
			loss_aggregationtag._oid AS _oid, 
			loss_aggregationtag.type AS type, 
			loss_aggregationtag.name AS name 
		FROM loss_aggregationtag 
		WHERE loss_aggregationtag.type = 'CantonGemeinde'
	) AS tags_of_type 
	ON tags_of_type._oid = loss_assoc_riskvalue_aggregationtag.aggregationtag 
WHERE 
	loss_riskvalue.losscategory = 'NONSTRUCTURAL'
	AND loss_riskvalue._calculation_oid = 5
	AND tags_of_type.name LIKE 'AG%'
	AND loss_riskvalue._type = 'lossvalue'
GROUP BY tags_of_type.name