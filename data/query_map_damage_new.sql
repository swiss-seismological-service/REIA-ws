
WITH cte AS MATERIALIZED (
	SELECT loss_calculation._oid
	FROM loss_calculation
		 JOIN loss_earthquakeinformation
		 ON loss_calculation._earthquakeinformation_oid=loss_earthquakeinformation._oid
	WHERE
		 loss_earthquakeinformation.originid = 'smi:ch.ethz.sed/scenario/Origin/Sierre_M5_8'
		 AND loss_calculation.status = 'COMPLETE'
		 AND loss_calculation._type = 'DAMAGE'
	ORDER BY _earthquakeinformation_oid, loss_calculation.creationinfo_creationtime DESC
	LIMIT 1
),
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
		WHERE loss_calculation._oid = (SELECT cte._oid FROM cte)
		LIMIT 1
	) AS exposuremodel 
	ON exposuremodel._oid = loss_asset._exposuremodel_oid 
	WHERE tags_of_type.name LIKE 'SG%'
	AND tags_of_type.type = 'CantonGemeinde'
GROUP BY tags_of_type.name
ORDER BY tags_of_type.name
)
SELECT 
    "damage"::text,
    ST_AsBinary(ST_Force2D("the_geom"),'NDR') as geom,
    "gid"::text
FROM (
        SELECT 
			lr.ag_name AS tag_name,
			round(lr.damaged_buildings/cte2.total_buildings*100) AS damage,
			cte2.total_buildings as tot,
			municipalities.gid AS gid,
			municipalities.name as municipality_name,
			municipalities.geom AS the_geom
		FROM (
			SELECT 
			  sum( ( lr.dg2_value +
					 + lr.dg3_value +
					 + lr.dg4_value +
					 + lr.dg5_value
				   ) * lr.weight
				) AS damaged_buildings,
				tags_of_type.ag_name AS ag_name
			FROM 
				(SELECT * FROM loss_riskvalue as lr 
				 WHERE lr._calculation_oid = (select cte._oid from cte) 
				 AND lr.losscategory = 'STRUCTURAL' 
				 AND lr._type = 'DAMAGE'
				)
				AS lr 
				JOIN loss_assoc_riskvalue_aggregationtag AS assoc 
				ON (lr._oid = assoc.riskvalue 
					AND lr._calculation_oid = assoc._calculation_oid 
					AND lr.losscategory = assoc.losscategory)
				JOIN (
					SELECT 
						loss_aggregationtag._oid AS _oid, 
						loss_aggregationtag.type AS type, 
						loss_aggregationtag.name AS ag_name 
					FROM 
						loss_aggregationtag 
					WHERE 
						loss_aggregationtag.type = 'CantonGemeinde'
						AND loss_aggregationtag.name LIKE 'SG%'
				) AS tags_of_type 
				ON tags_of_type._oid = assoc.aggregationtag 
			WHERE 
				tags_of_type.ag_name LIKE 'SG%'
				AND lr.losscategory = 'STRUCTURAL' 
				AND lr._calculation_oid = (select cte._oid from cte)
				AND lr._type = 'DAMAGE'
			GROUP BY tags_of_type.ag_name
			) AS lr
			INNER JOIN municipalities
			ON (municipalities.cantongeme = lr.ag_name )
			JOIN cte2 ON municipalities.cantongeme = cte2.name
			WHERE
				municipalities.gdektg = 'SG'
        ) AS subquery
where "the_geom" && ST_GeomFromText('POLYGON((8.22458283409584 46.8479161567939,8.22458283409584 47.5674167150977,10.2701809966295 47.5674167150977,10.2701809966295 46.8479161567939,8.22458283409584 46.8479161567939))',4326)
ORDER BY tag_name