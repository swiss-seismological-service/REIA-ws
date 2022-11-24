SELECT 
	sum((loss_riskvalue.dg1_value + 
		 loss_riskvalue.dg2_value +
		 loss_riskvalue.dg3_value + 
		 loss_riskvalue.dg4_value + 
		 loss_riskvalue.dg5_value)*loss_riskvalue.weight) AS damaged_buildings,
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
	loss_riskvalue.losscategory = 'STRUCTURAL'
	AND loss_riskvalue._calculation_oid = 2
	AND tags_of_type.name LIKE 'AG%'
	AND loss_riskvalue._type = 'damagevalue'
GROUP BY tags_of_type.name