#!/usr/bin/env bash
# Usage: ./scripts/bump-version.sh <major|minor|patch|VERSION>
# Bumps version across Cargo.toml, node/package.json, and pyproject.toml
set -euo pipefail

if [ $# -ne 1 ]; then
    echo "Usage: $0 <major|minor|patch|VERSION>"
    echo "Example: $0 patch        # 0.1.0 -> 0.1.1"
    echo "Example: $0 minor        # 0.1.0 -> 0.2.0"
    echo "Example: $0 1.0.0        # explicit version"
    exit 1
fi

CURRENT=$(grep '^version' Cargo.toml | head -1 | sed 's/.*"\(.*\)".*/\1/')
IFS='.' read -r MAJOR MINOR PATCH <<< "$CURRENT"

case "$1" in
    major) NEW="$((MAJOR + 1)).0.0" ;;
    minor) NEW="${MAJOR}.$((MINOR + 1)).0" ;;
    patch) NEW="${MAJOR}.${MINOR}.$((PATCH + 1))" ;;
    *)     NEW="$1" ;;
esac

echo "Bumping version: $CURRENT -> $NEW"

# Update Cargo.toml
sed -i.bak "0,/^version = \"$CURRENT\"/s//version = \"$NEW\"/" Cargo.toml && rm -f Cargo.toml.bak

# Update node/package.json
if [ -f node/package.json ]; then
    sed -i.bak "s/\"version\": \"$CURRENT\"/\"version\": \"$NEW\"/" node/package.json && rm -f node/package.json.bak
fi

# Update pyproject.toml
if [ -f pyproject.toml ]; then
    sed -i.bak "s/version = \"$CURRENT\"/version = \"$NEW\"/" pyproject.toml && rm -f pyproject.toml.bak
fi

echo "Updated files:"
echo "  Cargo.toml:        $NEW"
echo "  node/package.json: $NEW"
echo "  pyproject.toml:    $NEW"
echo ""
echo "Next steps:"
echo "  git add -A && git commit -m 'chore: bump version to $NEW'"
echo "  git tag v$NEW"
echo "  git push origin main --tags"
