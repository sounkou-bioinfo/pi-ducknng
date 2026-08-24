#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import PurePosixPath


NEWS_PATH = "NEWS.md"
GENERATED_ONLY = {
    "README.md",
    "README.html",
    "demo/subscriber_gateway.md",
    "demo/subscriber_gateway.html",
    "function_catalog/functions.md",
    "function_catalog/functions.tsv",
}


def run_git(args: list[str]) -> list[str]:
    proc = subprocess.run(
        ["git", *args],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return [line.strip() for line in proc.stdout.splitlines() if line.strip()]


def changed_files(base: str | None, head: str) -> list[str]:
    if base:
        return sorted(
            {
                path
                for path in run_git(
                    ["diff", "--name-only", "--diff-filter=ACMR", f"{base}...{head}", "--"]
                )
                if path != NEWS_PATH and not path.startswith(".sync/")
            }
        )

    tracked = set(run_git(["diff", "--name-only", "HEAD", "--"]))
    untracked = set(run_git(["ls-files", "--others", "--exclude-standard"]))
    return sorted(
        path
        for path in tracked | untracked
        if path != NEWS_PATH and not path.startswith(".sync/")
    )


def is_user_visible(path: str) -> bool:
    if path in GENERATED_ONLY:
        return False
    if path in {"Makefile", "README.Rmd", "description.yml", "function_catalog/functions.yaml"}:
        return True
    if path.startswith("src/"):
        return True
    if path.startswith("docs/"):
        return True
    if path.startswith("demo/"):
        suffix = PurePosixPath(path).suffix
        return suffix not in {".md", ".html"}
    return False


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Require NEWS.md changes when user-visible ducknng files change."
    )
    parser.add_argument(
        "--base",
        help="Optional git base ref. When omitted, compare the current worktree against HEAD.",
    )
    parser.add_argument(
        "--head",
        default="HEAD",
        help="Optional git head ref when --base is used. Defaults to HEAD.",
    )
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    files = changed_files(args.base, args.head)
    user_visible = [path for path in files if is_user_visible(path)]
    news_changed = NEWS_PATH in files or bool(run_git(["diff", "--name-only", "HEAD", "--", NEWS_PATH]))

    if not user_visible:
        print("check_news: no user-visible file changes detected")
        return 0

    if news_changed:
        print("check_news: NEWS.md updated")
        return 0

    print("check_news: NEWS.md is missing for user-visible changes", file=sys.stderr)
    print("", file=sys.stderr)
    for path in user_visible:
        print(f"  - {path}", file=sys.stderr)
    print("", file=sys.stderr)
    print(
        "Touch NEWS.md when changing public behavior, docs, demos, or user-facing build workflow.",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
