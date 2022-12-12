 select "damage"::text,ST_AsBinary(ST_Force2D("the_geom"),'NDR') as geom,"gid"::text from (
        SELECT
           round(100*tbl_values.damage_buildings / num_buildings.total_buildings) as damage,
           100*tbl_values.damage_buildings / num_buildings.total_buildings as damage_src,
           tbl_values.municipality_name,
           tbl_values.gid AS gid,
           num_buildings.total_buildings,
           tbl_values.geom AS the_geom
        FROM
        (SELECT
          loss_aggregationtag.name AS tag_name,
          sum( ( loss_riskvalue.dg2_value +
                 + loss_riskvalue.dg3_value +
                 + loss_riskvalue.dg4_value +
                 + loss_riskvalue.dg5_value
               ) * loss_riskvalue.weight
             ) AS damage_buildings,
                 sum(loss_riskvalue.dg1_value * loss_riskvalue.weight) AS dg1_src,
             sum(loss_riskvalue.dg2_value * loss_riskvalue.weight) AS dg2_src,
             sum(loss_riskvalue.dg3_value * loss_riskvalue.weight) AS dg3_src,
             sum(loss_riskvalue.dg4_value * loss_riskvalue.weight) AS dg4_src,
             sum(loss_riskvalue.dg5_value * loss_riskvalue.weight) AS dg5_src,
             sum(loss_riskvalue.weight) AS weight_src,

          municipalities.gid AS gid,
          municipalities.name as municipality_name,
          municipalities.geom AS geom
         FROM
             loss_riskvalue
             JOIN loss_assoc_riskvalue_aggregationtag  ON loss_riskvalue._oid = loss_assoc_riskvalue_aggregationtag.riskvalue
             JOIN loss_aggregationtag  ON loss_aggregationtag._oid = loss_assoc_riskvalue_aggregationtag.aggregationtag
             JOIN municipalities ON municipalities.cantongeme = loss_aggregationtag.name
             JOIN ( SELECT loss_calculation._oid
                    FROM loss_calculation
                         JOIN loss_earthquakeinformation
                         ON loss_calculation._earthquakeinformation_oid=loss_earthquakeinformation._oid
                    WHERE
                         loss_earthquakeinformation.originid = convert_from( decode ( 'c21pOmNoLmV0aHouc2VkL3NjZW5hcmlvL09yaWdpbi9BYXJhdV9NNl8w' , 'base64'::text),'UTF8')
                         AND loss_calculation.status = 'COMPLETE'
                         AND loss_calculation._type = 'damagecalculation'
                    ORDER BY loss_calculation.creationinfo_creationtime DESC
                    LIMIT 1
                  ) AS calc
             ON calc._oid = loss_riskvalue._calculation_oid
         WHERE
               loss_aggregationtag.type = 'CantonGemeinde'
          AND  municipalities.gdektg = 'SG'
          AND loss_riskvalue._type = 'damagevalue'
          AND loss_riskvalue.losscategory = 'STRUCTURAL'
        GROUP BY
          loss_aggregationtag.name,
          municipalities.gid,
          municipalities.name,
          municipalities.geom
        ORDER BY loss_aggregationtag.name

        ) AS tbl_values

        JOIN
        (SELECT
                sum(loss_asset.buildingcount) AS total_buildings,
                loss_aggregationtag.name  AS tag_name
        FROM
                loss_asset
                JOIN loss_assoc_asset_aggregationtag ON loss_asset._oid = loss_assoc_asset_aggregationtag.asset
                JOIN loss_aggregationtag
                ON (loss_aggregationtag.type = 'CantonGemeinde' AND loss_aggregationtag._oid = loss_assoc_asset_aggregationtag.aggregationtag)
                JOIN (
                        SELECT loss_exposuremodel._oid AS _oid
                        FROM loss_exposuremodel
                             JOIN (
                                     loss_calculationbranch
                                     JOIN loss_damagecalculationbranch
                                     ON loss_calculationbranch._oid = loss_damagecalculationbranch._oid
                             )
                             ON loss_exposuremodel._oid = loss_calculationbranch._exposuremodel_oid
                             JOIN ( SELECT loss_calculation._oid
                                    FROM loss_calculation
                                         JOIN loss_earthquakeinformation
                                         ON loss_calculation._earthquakeinformation_oid=loss_earthquakeinformation._oid
                                    WHERE
                                         loss_earthquakeinformation.originid = convert_from( decode ( 'c21pOmNoLmV0aHouc2VkL3NjZW5hcmlvL09yaWdpbi9BYXJhdV9NNl8w' , 'base64'::text),'UTF8')
                                         AND loss_calculation.status = 'COMPLETE'
                                         AND loss_calculation._type = 'damagecalculation'
                                    ORDER BY loss_calculation.creationinfo_creationtime DESC
                                    LIMIT 1
                                  ) AS calc
                             ON calc._oid = loss_damagecalculationbranch._calculation_oid
                        LIMIT 1
                ) AS exposuremodel
                ON exposuremodel._oid = loss_asset._exposuremodel_oid
                JOIN municipalities ON municipalities.cantongeme = loss_aggregationtag.name
        WHERE
              municipalities.gdektg = 'SG'

        GROUP BY loss_aggregationtag.name
        ORDER BY loss_aggregationtag.name
        ) AS num_buildings

        ON num_buildings.tag_name = tbl_values.tag_name



                  ) AS subquery
         where "the_geom" && ST_GeomFromText('POLYGON((8.22458283409584 46.8479161567939,8.22458283409584 47.5674167150977,10.2701809966295 47.5674167150977,10.2701809966295 46.8479161567939,8.22458283409584 46.8479161567939))',4326)