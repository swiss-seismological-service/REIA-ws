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

csv_column_names = {
    'aggregation': {
        'example_aggregation': {
            'tag': 'tag'
        }
    },
    'damage': {
        'structural': {
            'dg1_mean': 'damage_grade_1_mean',
            'dg1_pc10': 'damage_grade_1_pc10',
            'dg1_pc90': 'damage_grade_1_pc90',
            'dg2_mean': 'damage_grade_2_mean',
            'dg2_pc10': 'damage_grade_2_pc10',
            'dg2_pc90': 'damage_grade_2_pc90',
            'dg3_mean': 'damage_grade_3_mean',
            'dg3_pc10': 'damage_grade_3_pc10',
            'dg3_pc90': 'damage_grade_3_pc90',
            'dg4_mean': 'damage_grade_4_mean',
            'dg4_pc10': 'damage_grade_4_pc10',
            'dg4_pc90': 'damage_grade_4_pc90',
            'dg5_mean': 'damage_grade_5_mean',
            'dg5_pc10': 'damage_grade_5_pc10',
            'dg5_pc90': 'damage_grade_5_pc90',
            'damage_mean': 'damage_mean',
            'damage_pc10': 'damage_pc10',
            'damage_pc90': 'damage_pc90',
            'damage_percentage': 'damage_percentage',
            'buildings': 'buildings',
        }
    },
    'loss': {
        'structural': {
            'loss_mean': 'loss_mean',
            'loss_pc10': 'loss_pc10',
            'loss_pc90': 'loss_pc90',
        }
    }
}
