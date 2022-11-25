SELECT 
	sum(loss_riskvalue.loss_value) AS event_sum, 
	sum(loss_riskvalue.weight) / count(loss_riskvalue._oid) AS event_weight, 
	loss_riskvalue._losscalculationbranch_oid * (10^6) + loss_riskvalue.eventid AS unique_event 
FROM 
	loss_riskvalue 
	JOIN loss_assoc_riskvalue_aggregationtag 
	ON loss_riskvalue._oid = loss_assoc_riskvalue_aggregationtag.riskvalue 
	JOIN (
		SELECT 
			loss_aggregationtag._oid AS _oid
		FROM 
			loss_aggregationtag 
		WHERE loss_aggregationtag.type = 'Canton'
	) AS tags_of_type 
	ON tags_of_type._oid = loss_assoc_riskvalue_aggregationtag.aggregationtag 

WHERE 
	loss_riskvalue._calculation_oid = 3
	AND loss_riskvalue.losscategory = 'NONSTRUCTURAL'
	AND loss_riskvalue._type = 'lossvalue'
GROUP BY unique_event