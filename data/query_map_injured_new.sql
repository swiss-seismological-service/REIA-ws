WITH cte AS MATERIALIZED (
	SELECT loss_calculation._oid
	FROM loss_calculation
		 JOIN loss_earthquakeinformation
		 ON loss_calculation._earthquakeinformation_oid=loss_earthquakeinformation._oid
	WHERE
		 loss_earthquakeinformation.originid = 'smi:ch.ethz.sed/scenario/Origin/Sierre_M5_8'
		 AND loss_calculation.status = 'COMPLETE'
		 AND loss_calculation._type = 'LOSS'
	ORDER BY loss_calculation.creationinfo_creationtime DESC
	LIMIT 1
)
SELECT 
    "injured"::text,
    ST_AsBinary(ST_Force2D("the_geom"),'NDR') as geom,
    "gid"::text FROM (
        SELECT
        lr.ag_name AS tag_name,
        coalesce(lr.injured, 0) AS injured,
        lr.injured_src AS injured_src,
        municipalities.gid AS gid,
        municipalities.name as municipality_name,
        municipalities.geom AS the_geom
            FROM (
                SELECT 
					round(sum(lr.loss_value * lr.weight)) AS injured,
					sum(lr.loss_value * lr.weight) AS injured_src, 
					tags_of_type.ag_name AS ag_name
				FROM 
					(SELECT * FROM loss_riskvalue as lr 
					 WHERE lr._calculation_oid = (select cte._oid from cte) 
					 AND lr.losscategory = 'NONSTRUCTURAL' 
					 AND lr._type = 'LOSS'
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
					AND lr.losscategory = 'NONSTRUCTURAL' 
					AND lr._calculation_oid = (select cte._oid from cte)
					AND lr._type = 'LOSS'
				GROUP BY tags_of_type.ag_name
                ) AS lr
                INNER JOIN municipalities
                ON (municipalities.cantongeme = lr.ag_name )
            WHERE
                municipalities.gdektg = 'SG'
        ) AS subquery
where "the_geom" && ST_GeomFromText('POLYGON((8.22458283409584 46.8479161567939,8.22458283409584 47.5674167150977,10.2701809966295 47.5674167150977,10.2701809966295 46.8479161567939,8.22458283409584 46.8479161567939))',4326)