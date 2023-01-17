WITH cte AS (
	SELECT 
	SUM(mat.total_buildings * models.weight) as total_buildings,
	mat.name as name
FROM
loss_buildings_per_municipality as mat
JOIN (
	SELECT loss_exposuremodel._oid, SUM(loss_calculationbranch.weight) as weight
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
	GROUP BY loss_exposuremodel._oid
) as models
ON
	models._oid = mat._oid
GROUP BY
	mat.name
)
SELECT
		round(coalesce(losses.damaged_buildings, 0)/losses.total_buildings*100) AS damage,
		losses.name AS tag_name,
		municipalities.gid AS gid,
		municipalities.name as municipality_name,
		municipalities.geom AS the_geom,
		municipalities.gdektg
FROM (
	SELECT 
	results.damaged_buildings,
	results.aggregationtag,
	tags.name,
	cte.total_buildings
	FROM (
		SELECT
				sum(( lr.dg2_value +
						lr.dg3_value +
						lr.dg4_value +
						lr.dg5_value
						) * lr.weight
				) AS damaged_buildings,
				assoc.aggregationtag
		FROM
				(SELECT * FROM loss_riskvalue as lr
				WHERE lr._calculation_oid = 2
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
				loss_aggregationtag.name LIKE 'VD%'
) AS tags
JOIN cte ON tags.name = cte.name
ON tags._oid = results.aggregationtag ) AS losses

RIGHT JOIN municipalities ON municipalities.cantongeme = losses.name
WHERE municipalities.gdektg = 'VD'
ORDER BY
		losses.name