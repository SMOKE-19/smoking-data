from __future__ import annotations


def workspace_contribution() -> dict[str, object]:
    return {
        "package": "smoking_data",
        "resource_root": "vscode",
        "schemas": {
            "source-0101.schema.json": [
                "**/*.0101.yaml",
                "**/*.0101.yml",
            ]
        },
        "snippet_name": "smoking-data-0101-source.code-snippets",
        "snippet_resource": "smoking-data-0101-source.code-snippets",
    }
