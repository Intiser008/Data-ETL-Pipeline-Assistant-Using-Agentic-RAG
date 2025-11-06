# Table: medications

Tracks prescribed medications for each patient, including start and stop dates.

---

## Columns

| Column | Type | Description |
| ------- | ---- | ----------- |
| start | DATE | Prescription start date. |
| stop | DATE | Prescription end date. |
| patient | UUID | Reference to `patients.id`. |
| encounter | UUID | Reference to `encounters.id`. |
| code | TEXT | Medication code (RxNorm). |
| description | TEXT | Medication name or description. |
| reasoncode | TEXT | Reason code for prescription (optional). |
| reasondescription | TEXT | Text description of reason (optional). |

---

## Relationships
- `medications.patient → patients.id`
- `medications.encounter → encounters.id`

---

## Example Row

| patient | code | description | start | stop |
| -------- | ---- | ------------ | ------ | ---- |
| cce97723-e1c9-40df-b299-5241a59aefa5 | 55966010709 | aspirin 500 MG Delayed Release Oral Tablet | 2023-10-29 | 2023-11-01 |
