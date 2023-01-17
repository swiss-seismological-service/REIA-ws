        -- CREATE INDEX idx_loss_aggregationtag__oid ON loss_aggregationtag (_oid);
        -- CREATE INDEX idx_loss_aggregationtag_name ON loss_aggregationtag (lower(name));
        -- CREATE INDEX idx_loss_aggregationtag_type_name ON loss_aggregationtag (type, (lower(name)));
        -- CLUSTER loss_aggregationtag Using idx_loss_aggregationtag_name;

        -- CREATE INDEX idx_loss_riskvalue__type ON loss_riskvalue (_type);
        -- CREATE INDEX idx_loss_riskvalue__oid ON loss_riskvalue (_oid);

        -- SET default_statistics_target = 1000;
        -- ANALYZE loss_riskvalue;
        -- ANALYZE loss_aggregationtag;
        -- ALTER TABLE loss_riskvalue SET (autovacuum_analyze_scale_factor = 0.05); ??????
        -- CREATE INDEX idx_loss_assoc__calculation_oid ON loss_assoc_riskvalue_aggregationtag (_calculation_oid)
        -- CREATE INDEX idx_loss_assoc_losscategory ON loss_assoc_riskvalue_aggregationtag (losscategory)
        -- CREATE INDEX idx_loss_assoc_riskvalue_aggregationtag ON loss_assoc_riskvalue_aggregationtag (aggregationtag)
        -- CREATE INDEX idx_loss_assoc_riskvalue_riskvalue ON loss_assoc_riskvalue_aggregationtag (riskvalue)
        -- DROP INDEX idx_loss_assoc_riskvalue_aggregationtag;