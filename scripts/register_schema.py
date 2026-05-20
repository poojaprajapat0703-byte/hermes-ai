"""
scripts/register_schema.py

Registers Avro schemas with Schema Registry.
Run with: uv run python scripts/register_schema.py
"""

import json
import urllib.request
import urllib.error

SCHEMA_REGISTRY_URL = "http://localhost:8081"
SCHEMA_FILE = "schemas/incident.avsc"


def register_schema(subject: str, schema_file: str) -> int:
    """
    Register a schema under a subject.
    Subject naming convention: {topic-name}-value
    Returns the schema ID assigned by Schema Registry.
    """
    # Read the .avsc file
    with open(schema_file) as f:
        schema_str = f.read()

    # Schema Registry expects: {"schema": "<escaped schema string>"}
    payload = json.dumps({"schema": schema_str}).encode("utf-8")

    url = f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions"
    req = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req) as response:
            result = json.loads(response.read())
            print(f"✓ Schema registered: subject='{subject}' id={result['id']}")
            return result["id"]
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"✗ Failed to register schema: {e.code} {body}")
        raise


def list_subjects() -> None:
    url = f"{SCHEMA_REGISTRY_URL}/subjects"
    with urllib.request.urlopen(url) as response:
        subjects = json.loads(response.read())
        print(f"✓ Registered subjects: {subjects}")


if __name__ == "__main__":
    print("→ Registering Hermes schemas...\n")
    register_schema("raw.alerts-value", SCHEMA_FILE)
    register_schema("normalized.incidents-value", SCHEMA_FILE)
    print("\n→ All subjects:")
    list_subjects()