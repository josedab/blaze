#!/bin/bash
# Scaffolds a new Blaze module with boilerplate files.
# Usage: ./scripts/new-module.sh <module_name>

set -e

MODULE_NAME="$1"

if [ -z "$MODULE_NAME" ]; then
    echo "Usage: $0 <module_name>"
    echo "Example: $0 my_feature"
    exit 1
fi

# Validate module name (lowercase, underscores, no hyphens)
if ! echo "$MODULE_NAME" | grep -qE '^[a-z][a-z0-9_]*$'; then
    echo "❌ Invalid module name: '$MODULE_NAME'"
    echo "Use lowercase letters, numbers, and underscores (e.g., my_feature)"
    exit 1
fi

MODULE_DIR="src/$MODULE_NAME"

if [ -e "src/$MODULE_NAME.rs" ] || [ -d "$MODULE_DIR" ]; then
    echo "❌ Module '$MODULE_NAME' already exists"
    exit 1
fi

echo "Creating module: $MODULE_NAME"

# Ask if it should be a directory module or single file
if [ "${2:-dir}" = "file" ]; then
    # Single file module
    cat > "src/$MODULE_NAME.rs" << EOF
//! $MODULE_NAME module for Blaze.
//!
//! TODO: Add module documentation.

use crate::error::Result;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_placeholder() {
        // TODO: Add tests
    }
}
EOF
    echo "  Created src/$MODULE_NAME.rs"
else
    # Directory module
    mkdir -p "$MODULE_DIR"

    cat > "$MODULE_DIR/mod.rs" << EOF
//! $MODULE_NAME module for Blaze.
//!
//! TODO: Add module documentation.

use crate::error::Result;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_placeholder() {
        // TODO: Add tests
    }
}
EOF
    echo "  Created $MODULE_DIR/mod.rs"
fi

echo ""
echo "✅ Module '$MODULE_NAME' scaffolded."
echo ""
echo "Next steps:"
echo "  1. Add 'pub mod $MODULE_NAME;' to src/lib.rs"
echo "  2. Implement the module logic"
echo "  3. Run 'make' to verify"
