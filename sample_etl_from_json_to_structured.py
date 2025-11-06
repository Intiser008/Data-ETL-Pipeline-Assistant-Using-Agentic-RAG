import json
import pandas as pd
from pathlib import Path

# ─────────── CONFIG ───────────
RAW_DIR = Path("etl_pipeline/raw")  # folder containing all json bundles
OUT_DIR = Path("etl_pipeline/processed_preview")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ─────────── HELPERS ───────────
def get_ref_id(ref):
    if not ref:
        return None
    return ref.split("/")[-1].split(":")[-1]

def safe_get(obj, path_list):
    """nested get"""
    for k in path_list:
        if isinstance(obj, dict):
            obj = obj.get(k)
        else:
            return None
    return obj

# ─────────── INIT TABLES ───────────
patients, encounters, conditions, observations, medications, procedures = [], [], [], [], [], []

# ─────────── MAIN LOOP ───────────
for file in RAW_DIR.glob("*.json"):
    print(f"📂 Processing {file.name}")
    with open(file, "r", encoding="utf-8") as f:
        data = json.load(f)
    entries = data.get("entry", [])

    for e in entries:
        r = e.get("resource", {})
        rt = r.get("resourceType")

        # PATIENT
        if rt == "Patient":
            addr_obj = (r.get("address") or [{}])[0]
            addr_line = addr_obj.get("line") or []
            full_address = " ".join(addr_line + [
                addr_obj.get("city", ""), addr_obj.get("state", ""),
                addr_obj.get("postalCode", ""), addr_obj.get("country", "")
            ]).strip()

            ssn = None
            for ext in r.get("extension", []):
                if "SocialSecurityNumber" in ext.get("url", ""):
                    ssn = ext.get("valueString")

            patients.append({
                "id": r.get("id"),
                "birthdate": r.get("birthDate"),
                "deathdate": r.get("deceasedDateTime"),
                "ssn": ssn,
                "drivers": None,
                "passport": None,
                "prefix": safe_get(r, ["name", 0, "prefix", 0]),
                "first": safe_get(r, ["name", 0, "given", 0]),
                "last": safe_get(r, ["name", 0, "family"]),
                "suffix": safe_get(r, ["name", 0, "suffix", 0]),
                "maiden": None,
                "marital": safe_get(r, ["maritalStatus", "text"]),
                "race": None,
                "ethnicity": None,
                "gender": r.get("gender"),
                "birthplace": safe_get(r, ["extension", 2, "valueAddress", "city"]),
                "address": full_address,
            })

        # ENCOUNTER
        elif rt == "Encounter":
            encounters.append({
                "id": r.get("id"),
                "date": safe_get(r, ["period", "start"]) or safe_get(r, ["meta", "lastUpdated"]),
                "patient": get_ref_id(safe_get(r, ["subject", "reference"])),
                "code": safe_get(r, ["type", 0, "coding", 0, "code"]) or safe_get(r, ["class", "code"]),
                "description": safe_get(r, ["type", 0, "text"]) or safe_get(r, ["class", "display"]),
                "reasoncode": safe_get(r, ["reasonCode", 0, "coding", 0, "code"]),
                "reasondescription": safe_get(r, ["reasonCode", 0, "text"]),
            })

        # CONDITION
        elif rt == "Condition":
            conditions.append({
                "start": r.get("onsetDateTime"),
                "stop": r.get("abatementDateTime") or r.get("assertedDate"),
                "patient": get_ref_id(safe_get(r, ["subject", "reference"])),
                "encounter": get_ref_id(safe_get(r, ["encounter", "reference"]) or safe_get(r, ["context", "reference"])),
                "code": safe_get(r, ["code", "coding", 0, "code"]),
                "description": safe_get(r, ["code", "text"]),
            })

        # OBSERVATION
        elif rt == "Observation":
            val, units = None, None
            if "valueQuantity" in r:
                val = safe_get(r, ["valueQuantity", "value"])
                units = safe_get(r, ["valueQuantity", "unit"])
            elif "valueCodeableConcept" in r:
                val = safe_get(r, ["valueCodeableConcept", "text"])
            observations.append({
                "date": r.get("effectiveDateTime"),
                "patient": get_ref_id(safe_get(r, ["subject", "reference"])),
                "encounter": get_ref_id(safe_get(r, ["encounter", "reference"]) or safe_get(r, ["context", "reference"])),
                "code": safe_get(r, ["code", "coding", 0, "code"]),
                "description": safe_get(r, ["code", "text"]),
                "value": val,
                "units": units,
            })

        # MEDICATIONREQUEST
        elif rt in ["MedicationRequest", "MedicationOrder", "MedicationPrescription"]:
            medications.append({
                "start": r.get("authoredOn") or safe_get(r, ["dispenseRequest", "validityPeriod", "start"]),
                "stop": safe_get(r, ["dispenseRequest", "validityPeriod", "end"]),
                "patient": get_ref_id(safe_get(r, ["subject", "reference"])),
                "encounter": get_ref_id(safe_get(r, ["encounter", "reference"]) or safe_get(r, ["context", "reference"])),
                "code": safe_get(r, ["medicationCodeableConcept", "coding", 0, "code"]),
                "description": safe_get(r, ["medicationCodeableConcept", "text"]),
                "reasoncode": safe_get(r, ["reasonCode", 0, "coding", 0, "code"]),
                "reasondescription": safe_get(r, ["reasonCode", 0, "text"]),
            })

        # PROCEDURE
        elif rt == "Procedure":
            procedures.append({
                "date": r.get("performedDateTime"),
                "patient": get_ref_id(safe_get(r, ["subject", "reference"])),
                "encounter": get_ref_id(safe_get(r, ["encounter", "reference"]) or safe_get(r, ["context", "reference"])),
                "code": safe_get(r, ["code", "coding", 0, "code"]),
                "description": safe_get(r, ["code", "text"]),
                "reasoncode": safe_get(r, ["reasonCode", 0, "coding", 0, "code"]),
                "reasondescription": safe_get(r, ["reasonCode", 0, "text"]),
            })

# ─────────── TO DATAFRAMES ───────────
dfs = {
    "patients": pd.DataFrame(patients, columns=[
        "id","birthdate","deathdate","ssn","drivers","passport","prefix","first","last",
        "suffix","maiden","marital","race","ethnicity","gender","birthplace","address"
    ]),
    "encounters": pd.DataFrame(encounters, columns=[
        "id","date","patient","code","description","reasoncode","reasondescription"
    ]),
    "conditions": pd.DataFrame(conditions, columns=[
        "start","stop","patient","encounter","code","description"
    ]),
    "observations": pd.DataFrame(observations, columns=[
        "date","patient","encounter","code","description","value","units"
    ]),
    "medications": pd.DataFrame(medications, columns=[
        "start","stop","patient","encounter","code","description","reasoncode","reasondescription"
    ]),
    "procedures": pd.DataFrame(procedures, columns=[
        "date","patient","encounter","code","description","reasoncode","reasondescription"
    ]),
}

# ─────────── EXPORT ───────────
for name, df in dfs.items():
    if not df.empty:
        df.drop_duplicates(inplace=True)
        df.to_csv(OUT_DIR / f"{name}.csv", index=False)
        print(f"✅ {name}.csv written ({len(df)} rows)")
    else:
        print(f"⚠️  No data for {name}")

print("\n🎯 Extraction complete. CSVs saved to:", OUT_DIR.resolve())
