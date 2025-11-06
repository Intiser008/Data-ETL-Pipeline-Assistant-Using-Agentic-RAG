# Table: procedures

Lists medical procedures or interventions performed during encounters.

---

## Columns

| Column | Type | Description |
| ------- | ---- | ----------- |
| date | DATE | Date of procedure. |
| patient | UUID | Reference to `patients.id`. |
| encounter | UUID | Reference to `encounters.id`. |
| code | TEXT | Procedure code (CPT/SNOMED). |
| description | TEXT | Description of the procedure. |
| reasoncode | TEXT | Code for procedure reason (optional). |
| reasondescription | TEXT | Description of reason for procedure (optional). |

---

## Relationships
- `procedures.patient → patients.id`
- `procedures.encounter → encounters.id`

---

## Example Row

| patient | code | description | date |
| -------- | ---- | ------------ | ---- |
| d3ae0579-ac2c-48b0-a0c1-a858b63e3b99 | 4010003 | Appendectomy | 2017-12-15 |
