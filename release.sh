#!/bin/bash

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Function to get current version from Cargo.toml
get_current_version() {
    grep -E "^version = " Cargo.toml | head -1 | cut -d'"' -f2
}

# Get current version
CURRENT_VERSION=$(get_current_version)

echo -e "${GREEN}=== PGSQLite Release Script ===${NC}"

# Switch to main branch and pull latest
echo -e "${YELLOW}=== Switching to main branch and pulling latest ===${NC}"
if ! git checkout main; then
    echo -e "${RED}Error: Failed to switch to main branch${NC}"
    exit 1
fi

if ! git pull origin main; then
    echo -e "${RED}Error: Failed to pull latest changes from main branch${NC}"
    exit 1
fi

echo -e "Current version: ${YELLOW}$CURRENT_VERSION${NC}"
echo

# Ask for new version
read -p "Enter new version (e.g., 0.2.0): " NEW_VERSION

# Ask for version name
read -p "Enter version name (e.g., 'Performance Boost'): " VERSION_NAME

# Display confirmation
echo
echo -e "${YELLOW}=== Release Summary ===${NC}"
echo -e "Current version: $CURRENT_VERSION"
echo -e "New version: ${GREEN}$NEW_VERSION${NC}"
echo -e "Version name: ${GREEN}$VERSION_NAME${NC}"
echo

# Ask for confirmation
read -p "Do you want to proceed with this release? (y/N): " CONFIRM

if [[ ! "$CONFIRM" =~ ^[Yy]$ ]]; then
    echo -e "${RED}Release cancelled.${NC}"
    exit 1
fi

echo
echo -e "${GREEN}=== Step 1: Bumping version ===${NC}"

# Use Claude Code to bump version (non-interactive mode)
echo "Bump the version in Cargo.toml from $CURRENT_VERSION to $NEW_VERSION. Only update the version field, nothing else." | npx @anthropic-ai/claude-code

# Check if version was updated
NEW_VERSION_CHECK=$(get_current_version)
if [ "$NEW_VERSION_CHECK" != "$NEW_VERSION" ]; then
    echo -e "${RED}Error: Version bump failed. Expected $NEW_VERSION but got $NEW_VERSION_CHECK${NC}"
    exit 1
fi

echo -e "${GREEN}Version bumped successfully!${NC}"

echo
echo -e "${GREEN}=== Step 2: Committing version bump ===${NC}"

# Commit the version bump
git add Cargo.toml Cargo.lock
git commit -m "chore: bump version to $NEW_VERSION"

echo
echo -e "${GREEN}=== Step 3: Creating and pushing tag ===${NC}"

# Create tag with version name
TAG_NAME="v$NEW_VERSION"
git tag -a "$TAG_NAME" -m "Release $NEW_VERSION: $VERSION_NAME"

# Push commit and tag
git push origin main
git push origin "$TAG_NAME"

echo -e "${GREEN}Tag $TAG_NAME created and pushed!${NC}"

echo
echo -e "${GREEN}=== Step 4: Generating release notes ===${NC}"

# Get the previous tag
PREV_TAG=$(git describe --tags --abbrev=0 "$TAG_NAME^" 2>/dev/null || echo "")

if [ -z "$PREV_TAG" ]; then
    echo -e "${YELLOW}No previous tag found. Generating release notes from beginning of history.${NC}"
    RANGE="$TAG_NAME"
else
    echo -e "Generating release notes between ${YELLOW}$PREV_TAG${NC} and ${YELLOW}$TAG_NAME${NC}"
    RANGE="$PREV_TAG..$TAG_NAME"
fi

# Create temporary file for release notes
TEMP_FILE=$(mktemp)

# Use Claude Code to generate release notes (non-interactive mode)
cat << EOF | npx @anthropic-ai/claude-code > "$TEMP_FILE"
Generate release notes in markdown format for version $NEW_VERSION ($VERSION_NAME) of pgsqlite. Include the following sections:

## Release Notes for v$NEW_VERSION - $VERSION_NAME

### Summary
Brief overview of this release

### New Features
List of new features added

### Improvements
Performance improvements and enhancements

### Bug Fixes
List of bugs fixed

### Breaking Changes
Any breaking changes (if applicable)

Base the release notes on these commits:
$(git log --oneline $RANGE)

Output only the markdown content, no explanations.
EOF

echo
echo -e "${GREEN}=== Release Notes ===${NC}"
echo
cat "$TEMP_FILE"

# Clean up
rm "$TEMP_FILE"

echo
echo -e "${GREEN}=== Release Complete! ===${NC}"
echo -e "Version $NEW_VERSION has been tagged and pushed as $TAG_NAME"
echo -e "You can now create a GitHub release with the notes above."