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
                    round(sum(loss_riskvalue.loss_value * loss_riskvalue.weight)) AS injured,
                    sum(loss_riskvalue.loss_value * loss_riskvalue.weight) AS injured_src,
                    tags_of_type.ag_name as ag_name
                FROM
                    loss_riskvalue
                    JOIN loss_assoc_riskvalue_aggregationtag ON loss_riskvalue._oid = loss_assoc_riskvalue_aggregationtag.riskvalue
                    JOIN (
                            SELECT
                                    loss_aggregationtag._oid AS _oid,
                                    loss_aggregationtag.type AS type,
                                    loss_aggregationtag.name AS ag_name
                            FROM loss_aggregationtag
                            WHERE loss_aggregationtag.type = 'CantonGemeinde'
                        ) AS tags_of_type
                        ON tags_of_type._oid = loss_assoc_riskvalue_aggregationtag.aggregationtag
                        JOIN ( SELECT loss_calculation._oid
                            FROM loss_calculation
                                JOIN loss_earthquakeinformation
                                ON loss_calculation._earthquakeinformation_oid=loss_earthquakeinformation._oid
                            WHERE
                                loss_earthquakeinformation.originid = 'smi:ch.ethz.sed/scenario/aarau5_8'
                                AND loss_calculation.status = 'COMPLETE'
                                AND loss_calculation._type = 'LOSS'
                            ORDER BY loss_calculation.creationinfo_creationtime DESC
                            LIMIT 1
                    ) AS calc
                    ON loss_riskvalue._calculation_oid = calc._oid
                WHERE
                        loss_riskvalue.losscategory = 'NONSTRUCTURAL'
                        AND loss_riskvalue._type = 'LOSS'
                AND LEFT(tags_of_type.ag_name,2) = 'SG'

            GROUP BY tags_of_type.ag_name
                ) AS lr
                INNER JOIN municipalities
                ON (municipalities.cantongeme = lr.ag_name )
            WHERE
                municipalities.gdektg = 'SG'
        ) AS subquery
where "the_geom" && ST_GeomFromText('POLYGON((8.22458283409584 46.8479161567939,8.22458283409584 47.5674167150977,10.2701809966295 47.5674167150977,10.2701809966295 46.8479161567939,8.22458283409584 46.8479161567939))',4326)