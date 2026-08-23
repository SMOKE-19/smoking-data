from __future__ import annotations


def workspace_contribution() -> dict[str, object]:
    return {
        "package": "smoking_data",
        "resource_root": "vscode",
        "schemas": {
            "csv-source-v1.schema.json": [
                "**/*.0103.yaml",
                "**/*.0103.yml",
            ]
        },
        "snippet_name": "smoking-data-0103-source.code-snippets",
        "snippet_resource": "smoking-data-0103-source.code-snippets",
    }
