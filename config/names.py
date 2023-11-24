from config import Settings

# MAPPINGS for CSV filenames
# filename {type}_{oid}_{aggregation}-{filter?}_{category}.csv

# category names
csv_names_categories = {
    'damage': {
        Settings.RiskCategory.BUSINESS_INTERRUPTION:
        Settings.RiskCategory.BUSINESS_INTERRUPTION,
        Settings.RiskCategory.CONTENTS:
        Settings.RiskCategory.CONTENTS,
        Settings.RiskCategory.NONSTRUCTURAL:
        Settings.RiskCategory.NONSTRUCTURAL,
        Settings.RiskCategory.OCCUPANTS:
        Settings.RiskCategory.OCCUPANTS,
        Settings.RiskCategory.STRUCTURAL:
        Settings.RiskCategory.STRUCTURAL
    },
    'loss': {
        Settings.RiskCategory.BUSINESS_INTERRUPTION:
        Settings.RiskCategory.BUSINESS_INTERRUPTION,
        Settings.RiskCategory.CONTENTS:
        Settings.RiskCategory.CONTENTS,
        Settings.RiskCategory.NONSTRUCTURAL:
        Settings.RiskCategory.NONSTRUCTURAL,
        Settings.RiskCategory.OCCUPANTS:
        Settings.RiskCategory.OCCUPANTS,
        Settings.RiskCategory.STRUCTURAL:
        Settings.RiskCategory.STRUCTURAL
    }
}

# aggregation names
csv_names_aggregations = {}

# aggregation names if query param sum=true
csv_names_sum = {}
