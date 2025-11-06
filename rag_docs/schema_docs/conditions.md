# Table: conditions

Stores medical conditions or diagnoses recorded for a patient during an encounter.

---

## Columns

| Column | Type | Description |
| ------- | ---- | ----------- |
| start | DATE | Date when the condition began. |
| stop | DATE | Date when the condition resolved (if applicable). |
| patient | UUID | Reference to `patients.id`. |
| encounter | UUID | Reference to `encounters.id`. |
| code | TEXT | Condition code (ICD10/SNOMED). |
| description | TEXT | Description of the condition. |

---

## Relationships
- `conditions.patient → patients.id`
- `conditions.encounter → encounters.id`

---

## Example Row

| patient | code | description | start |
| -------- | ---- | ------------ | ------ |
| 9a1e56e2-13ac-4d9a-bc1b-32905a6e0a9d | 250.00 | Type 2 diabetes mellitus | 2018-11-03 |
