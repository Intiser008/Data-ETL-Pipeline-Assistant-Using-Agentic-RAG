# Table: encounters

Represents healthcare events or visits where a patient interacted with the healthcare system.

---

## Columns

| Column | Type | Description |
| ------- | ---- | ----------- |
| id | UUID | Encounter identifier (Primary Key). |
| date | DATE | Encounter date (single-day synthetic visit). |
| patient | UUID | Reference to `patients.id`. |
| code | TEXT | Encounter type code (e.g., 170258001). |
| description | TEXT | Description of encounter type. |
| reasoncode | TEXT | Code describing reason for visit (may be null). |
| reasondescription | TEXT | Free-text reason description (may be null). |

---

## Relationships
- `encounters.patient → patients.id`
- `conditions.encounter → encounters.id`
- `procedures.encounter → encounters.id`
- `medications.encounter → encounters.id`
- `observations.encounter → encounters.id`

---

## Example Row

| id | patient | date | code | description |
| -- | -------- | ---- | ---- | ------------ |
| d0ec6727-9463-44c2-9c52-b904949ff183 | d3ae0579-ac2c-48b0-a0c1-a858b63e3b99 | 2013-12-02 | 170258001 | Outpatient Encounter |
