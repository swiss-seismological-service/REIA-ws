SELECT 
	sum(lr.loss_value) AS loss_value, 
	sum(lr.weight) / count(lr._oid) AS weight  
FROM (
	SELECT *
	FROM
		loss_riskvalue
	WHERE
		loss_riskvalue.losscategory='STRUCTURAL' AND
		loss_riskvalue._calculation_oid = 1 AND
		loss_riskvalue._type = 'LOSS'
) as lr
JOIN 
	loss_assoc_riskvalue_aggregationtag 
ON 
	lr._oid = loss_assoc_riskvalue_aggregationtag.riskvalue AND 
	lr.losscategory = loss_assoc_riskvalue_aggregationtag.losscategory AND 
	lr._calculation_oid = loss_assoc_riskvalue_aggregationtag._calculation_oid 
JOIN (
	SELECT 
		loss_aggregationtag._oid AS _oid, 
		loss_aggregationtag.type AS type, 
		loss_aggregationtag.name AS name 
	FROM 
		loss_aggregationtag 
	WHERE 
		loss_aggregationtag.type = 'Canton'
) AS anon_1 
ON 
	anon_1._oid = loss_assoc_riskvalue_aggregationtag.aggregationtag --AND 
-- 	anon_1.type = loss_assoc_riskvalue_aggregationtag.aggregationtype 
WHERE 
	lr._calculation_oid = 1 AND 
	lr.losscategory = 'STRUCTURAL' AND 
	lr._type IN ('LOSS') 
GROUP BY 
	lr._losscalculationbranch_oid * POWER(10,9) + lr.eventid



