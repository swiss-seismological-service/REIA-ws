# REIA-ws

API for the data from the rapid earthquake risk assessment.

## Data Structure

At the highest level there are `RiskAssessments` which calculate the impact of an earthquake, referenced by its origin id. There can exist multiple RiskAssessments for the same origin id.

Each RiskAssessment consists of a `LossCalculation` (considering a vulnerability model) and a `DamageCalculation` (considering a fragility model):

```json
{
  "_oid": 0,
  "originid": "string",
  "type": "scenario",
  "losscalculation": {
    "_oid": 0,
    "aggregateby": [
      "string"
    ],
    "creationinfo": {
      ...
    },
    "status": 1,
    "description": "string",
    "_type": "string"
  },
  "damagecalculation": {
    "_oid": 0,
    "aggregateby": [
      "string"
    ],
    "creationinfo": {
      ...
    },
    "status": 1,
    "description": "string",
    "_type": "string"
  },
  "creationinfo": {
    ...
  },
  "preferred": true,
  "published": true
}
```

The `RiskAssessments` just like the `DamageCalculations` and `LossCalculations` can be queried using their object id's.
