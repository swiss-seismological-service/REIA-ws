SELECT 
	sum(loss_asset.buildingcount) AS sum_1,
	tag_of_type.name 
FROM 
	loss_asset 
	JOIN loss_assoc_asset_aggregationtag ON loss_asset._oid = loss_assoc_asset_aggregationtag.asset 
	JOIN (
		SELECT 
			loss_aggregationtag._oid AS _oid, 
			loss_aggregationtag.type AS type, 
			loss_aggregationtag.name AS name 
		FROM 
			loss_aggregationtag 
		WHERE 
			loss_aggregationtag.type = 'CantonGemeinde'
	) AS tag_of_type
	ON tag_of_type._oid = loss_assoc_asset_aggregationtag.aggregationtag 
	JOIN (
		SELECT loss_exposuremodel._oid AS _oid
		FROM loss_exposuremodel 
		JOIN (
			loss_calculationbranch 
			JOIN loss_damagecalculationbranch 
			ON loss_calculationbranch._oid = loss_damagecalculationbranch._oid
		)	
		ON loss_exposuremodel._oid = loss_calculationbranch._exposuremodel_oid 
		JOIN loss_calculation 
		ON loss_calculation._oid = loss_damagecalculationbranch._calculation_oid 
		WHERE loss_calculation._oid = 2
		LIMIT 1
	) AS exposuremodel 
	ON exposuremodel._oid = loss_asset._exposuremodel_oid 
WHERE tag_of_type.name LIKE 'AG%'
GROUP BY tag_of_type.name