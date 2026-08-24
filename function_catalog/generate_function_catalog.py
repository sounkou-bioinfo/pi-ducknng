#!/usr/bin/env python3
"""Render generated function catalog artifacts from functions.yaml.

The manifest is stored in JSON-formatted YAML so this script can use only the
Python standard library. Following the duckhts pattern, it also renders the
DuckDB community-extension descriptor from the same source manifest plus the
repo-level version metadata in description.yml.
"""

from __future__ import annotations

import csv
import json
import shutil
import subprocess
import sys
from collections import OrderedDict
from pathlib import Path
from string import Template


def die(message: str) -> None:
    print(message, file=sys.stderr)
    raise SystemExit(1)


def escape_md(text: str) -> str:
    return text.replace("|", "\\|").replace("\n", " ")


def load_root_description(path: Path) -> dict[str, object]:
    if not path.exists():
        die(f"Extension metadata not found: {path}")

    metadata: dict[str, object] = {}
    current_list_key: str | None = None
    current_list: list[str] | None = None

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        if raw_line.startswith((" ", "\t")):
            if current_list_key is None or current_list is None:
                die(f"Unsupported indentation in {path}: {raw_line!r}")
            item = stripped
            if not item.startswith("- "):
                die(f"Unsupported list item syntax in {path}: {raw_line!r}")
            current_list.append(item[2:].strip())
            continue

        current_list_key = None
        current_list = None
        key, sep, value = raw_line.partition(":")
        if not sep:
            die(f"Invalid metadata line in {path}: {raw_line!r}")

        key = key.strip()
        value = value.strip()
        if not key:
            die(f"Invalid metadata key in {path}: {raw_line!r}")

        if value:
            metadata[key] = value
            continue

        metadata[key] = []
        current_list_key = key
        current_list = metadata[key]

    version = metadata.get("version")
    description = metadata.get("description")
    if not isinstance(version, str) or not version:
        die(f"{path} is missing a non-empty 'version' field")
    if not isinstance(description, str) or not description:
        die(f"{path} is missing a non-empty 'description' field")

    return metadata


def load_manifest(path: Path) -> OrderedDict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=OrderedDict)
    except json.JSONDecodeError as exc:
        die(f"Failed to parse {path}: {exc}")

    community_extension = payload.get("community_extension")
    if not isinstance(community_extension, dict):
        die(f"{path} is missing a top-level 'community_extension' object")

    extension = community_extension.get("extension")
    if not isinstance(extension, dict):
        die(f"{path} is missing community_extension.extension")
    required_extension = {"name", "description", "language", "build", "license", "maintainers"}
    missing_extension = sorted(required_extension - set(extension))
    if missing_extension:
        die(
            "community_extension.extension is missing required fields: "
            + ", ".join(missing_extension)
        )
    if not isinstance(extension["maintainers"], list) or not all(
        isinstance(x, str) and x for x in extension["maintainers"]
    ):
        die("community_extension.extension.maintainers must be a list of non-empty strings")

    repo = community_extension.get("repo")
    if not isinstance(repo, dict):
        die(f"{path} is missing community_extension.repo")
    required_repo = {"github"}
    missing_repo = sorted(required_repo - set(repo))
    if missing_repo:
        die("community_extension.repo is missing required fields: " + ", ".join(missing_repo))

    docs = community_extension.get("docs")
    if not isinstance(docs, dict):
        die(f"{path} is missing community_extension.docs")
    for field in ("hello_world_lines", "extended_intro", "feature_notes"):
        value = docs.get(field)
        if not isinstance(value, list) or not all(isinstance(x, str) for x in value):
            die(f"community_extension.docs.{field} must be a list of strings")

    functions = payload.get("functions")
    if not isinstance(functions, list):
        die(f"{path} is missing a top-level 'functions' array")

    required = {
        "name",
        "kind",
        "category",
        "signature",
        "returns",
        "since",
        "implemented",
        "description",
    }
    seen: set[str] = set()
    for index, entry in enumerate(functions):
        if not isinstance(entry, dict):
            die(f"functions[{index}] must be an object")
        missing = sorted(required - set(entry))
        if missing:
            die(f"functions[{index}] is missing required fields: {', '.join(missing)}")
        name = entry["name"]
        if not isinstance(name, str) or not name:
            die(f"functions[{index}].name must be a non-empty string")
        if name in seen:
            die(f"Duplicate function entry for {name}")
        seen.add(name)
    return payload


def extract_arguments(signature: str) -> str:
    start = signature.find("(")
    end = signature.rfind(")")
    if start == -1 or end == -1 or end < start:
        return ""
    return signature[start + 1 : end]


def render_markdown(functions: list[dict[str, object]]) -> str:
    by_category: OrderedDict[str, list[dict[str, object]]] = OrderedDict()
    for entry in functions:
        by_category.setdefault(str(entry["category"]), []).append(entry)

    lines: list[str] = []
    lines.append("# Function Catalog")
    lines.append("")
    lines.append("This file is generated from `function_catalog/functions.yaml`.")
    lines.append("")
    for category, entries in by_category.items():
        lines.append(f"## {category}")
        lines.append("")
        lines.append("| name | kind | arguments | returns | description |")
        lines.append("|---|---|---|---|---|")
        for entry in entries:
            arguments = escape_md(extract_arguments(str(entry["signature"])))
            arguments_md = f"`{arguments}`" if arguments else ""
            lines.append(
                "| `{name}` | {kind} | {arguments} | `{returns}` | {description} |".format(
                    name=escape_md(str(entry["name"])),
                    kind=escape_md(str(entry["kind"])),
                    arguments=arguments_md,
                    returns=escape_md(str(entry["returns"])),
                    description=escape_md(str(entry["description"])),
                )
            )
        lines.append("")
    return "\n".join(lines)


def write_tsv(path: Path, functions: list[dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t", lineterminator="\n")
        writer.writerow(
            ["name", "kind", "category", "arguments", "signature", "returns", "since", "description"]
        )
        for entry in functions:
            writer.writerow(
                [
                    entry["name"],
                    entry["kind"],
                    entry["category"],
                    extract_arguments(str(entry["signature"])),
                    entry["signature"],
                    entry["returns"],
                    entry["since"],
                    entry["description"],
                ]
            )


def quote_yaml_scalar(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def build_extended_description(
    functions: list[dict[str, object]], docs: dict[str, object]
) -> list[str]:
    by_category: OrderedDict[str, list[dict[str, object]]] = OrderedDict()
    for function in functions:
        by_category.setdefault(str(function["category"]), []).append(function)

    lines: list[str] = []
    for paragraph in docs["extended_intro"]:
        lines.extend(str(paragraph).splitlines())
        lines.append("")

    lines.append("Functions included in this extension:")
    lines.append("")
    for category, entries in by_category.items():
        lines.append(f"### {category}")
        for entry in entries:
            lines.append(f"- `{entry['signature']}`: {entry['description']}")
        lines.append("")

    lines.append("Operational notes:")
    for note in docs["feature_notes"]:
        lines.append(f"- {note}")

    return lines


def resolve_repo_ref(repo_root: Path, repo: dict[str, object]) -> str:
    explicit_ref = repo.get("ref")
    if isinstance(explicit_ref, str) and explicit_ref:
        return explicit_ref

    ref_source = repo.get("ref_source", "git_head")
    if ref_source != "git_head":
        die(f"Unsupported community_extension.repo.ref_source: {ref_source}")

    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
            cwd=repo_root,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        die(f"Failed to resolve git HEAD for community descriptor: {exc}")

    return result.stdout.strip()


def indent_block(lines: list[str], spaces: int = 4) -> str:
    prefix = " " * spaces
    return "\n".join(f"{prefix}{line}" if line else prefix for line in lines)


def render_description_yaml(
    repo_root: Path,
    manifest: OrderedDict[str, object],
    functions: list[dict[str, object]],
    root_description: dict[str, object],
    template_path: Path,
) -> str:
    community_extension = manifest["community_extension"]
    extension = community_extension["extension"]
    repo = community_extension["repo"]
    docs = community_extension["docs"]
    repo_ref = resolve_repo_ref(repo_root, repo)
    extended_description_lines = build_extended_description(functions, docs)

    template = Template(template_path.read_text(encoding="utf-8"))
    optional_fields: list[str] = []
    for field in ("requires_toolchains", "excluded_platforms"):
        value = extension.get(field)
        if isinstance(value, str) and value:
            optional_fields.append(f"  {field}: {quote_yaml_scalar(value)}")

    return template.substitute(
        extension_name=quote_yaml_scalar(str(extension["name"])),
        extension_description=quote_yaml_scalar(str(root_description["description"])),
        extension_version=quote_yaml_scalar(str(root_description["version"])),
        extension_language=quote_yaml_scalar(str(extension["language"])),
        extension_build=quote_yaml_scalar(str(extension["build"])),
        extension_license=quote_yaml_scalar(str(extension["license"])),
        optional_extension_fields=("\n".join(optional_fields) + ("\n" if optional_fields else "")),
        maintainers_block=indent_block(
            [f"- {quote_yaml_scalar(str(maintainer))}" for maintainer in extension["maintainers"]]
        ),
        repo_github=quote_yaml_scalar(str(repo["github"])),
        repo_ref=quote_yaml_scalar(repo_ref),
        hello_world_block=indent_block([str(line) for line in docs["hello_world_lines"]]),
        extended_description_block=indent_block(extended_description_lines),
    )


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    manifest_path = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else repo_root / "function_catalog" / "functions.yaml"
    outdir = Path(sys.argv[2]).resolve() if len(sys.argv) > 2 else repo_root / "function_catalog"
    outdir.mkdir(parents=True, exist_ok=True)
    root_description_path = repo_root / "description.yml"
    template_path = repo_root / "function_catalog" / "community_extension_description.template.yml"

    manifest = load_manifest(manifest_path)
    root_description = load_root_description(root_description_path)
    functions = manifest["functions"]
    (outdir / "functions.md").write_text(render_markdown(functions), encoding="utf-8")
    write_tsv(outdir / "functions.tsv", functions)
    catalog_manifest_path = outdir / "functions.yaml"
    if manifest_path != catalog_manifest_path:
        shutil.copyfile(manifest_path, catalog_manifest_path)
    community_description_path = (
        repo_root / "community-extensions" / "extensions" / str(manifest["community_extension"]["extension"]["name"]) / "description.yml"
    )
    community_description_path.parent.mkdir(parents=True, exist_ok=True)
    community_description_path.write_text(
        render_description_yaml(repo_root, manifest, functions, root_description, template_path),
        encoding="utf-8",
    )
    print(f"wrote {outdir / 'functions.md'}")
    print(f"wrote {outdir / 'functions.tsv'}")
    print(f"wrote {community_description_path}")


if __name__ == "__main__":
    main()
