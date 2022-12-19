WITH 
cte2 AS MATERIALIZED (
SELECT 
	sum(loss_asset.buildingcount) AS total_buildings,
	tags_of_type.name
FROM 
	loss_asset 
	JOIN loss_assoc_asset_aggregationtag ON loss_asset._oid = loss_assoc_asset_aggregationtag.asset 
	JOIN loss_aggregationtag AS tags_of_type
	ON tags_of_type._oid = loss_assoc_asset_aggregationtag.aggregationtag 
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
		WHERE loss_calculation._oid = 24
		LIMIT 1
	) AS exposuremodel 
	ON exposuremodel._oid = loss_asset._exposuremodel_oid 
	WHERE tags_of_type.name LIKE 'AG%'
	AND tags_of_type.type = 'CantonGemeinde'
GROUP BY tags_of_type.name
ORDER BY tags_of_type.name
)
SELECT 
	round(coalesce(results.damaged_buildings, 0)/cte2.total_buildings*100) AS damage,
	tags.name AS tag_name,
	municipalities.gid AS gid,
	municipalities.name as municipality_name,
	municipalities.geom AS the_geom
FROM (
	SELECT
		sum(( lr.dg2_value +
			+ lr.dg3_value +
			+ lr.dg4_value +
			+ lr.dg5_value
			) * lr.weight
		) AS damaged_buildings,
		assoc.aggregationtag
	FROM 
		(SELECT * FROM loss_riskvalue as lr 
		 WHERE lr._calculation_oid = 24
		 AND lr.losscategory = 'STRUCTURAL' 
		 AND lr._type = 'DAMAGE'
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
RIGHT JOIN municipalities ON municipalities.cantongeme = tags.name 
JOIN cte2 ON tags.name = cte2.name
WHERE
	municipalities.gdektg = 'AG'
ORDER BY
	tags.name