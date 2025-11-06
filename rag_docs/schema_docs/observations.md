# Table: observations

Contains vital signs, lab results, and other measurements or observations collected during patient encounters.

---

## Columns

| Column | Type | Description |
| ------- | ---- | ----------- |
| date | DATE | Date when the observation was recorded. |
| patient | UUID | Reference to `patients.id`. |
| encounter | UUID | Reference to `encounters.id`. |
| code | TEXT | Observation code (LOINC/SNOMED). |
| description | TEXT | Text description of the observation. |
| value | TEXT | Recorded value (string, may represent number). |
| units | TEXT | Units of measurement when available. |

---

## Relationships
- `observations.patient → patients.id`
- `observations.encounter → encounters.id`

---

## Example Row

| patient | code | description | value | units |
| -------- | ---- | ------------ | ------ | ------ |
| d3ae0579-ac2c-48b0-a0c1-a858b63e3b99 | 8302-2 | Body Height | 55.68 | cm |
