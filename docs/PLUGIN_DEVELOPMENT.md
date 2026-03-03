# Plugin Development Guide

## Overview

Blaze has a plugin system that lets you extend the engine with custom
functions, storage backends, and more. Plugins are managed by a
`PluginRegistry` and can be loaded from dynamic libraries at runtime.

## Creating a Plugin

Blaze plugins implement the `Plugin` trait:

```rust
use blaze::plugin::{Plugin, PluginMetadata, PluginCapabilities};

struct MyPlugin {
    meta: PluginMetadata,
}

impl MyPlugin {
    fn new() -> Self {
        Self {
            meta: PluginMetadata::new("my-plugin", "0.1.0", "A custom Blaze plugin"),
        }
    }
}

impl Plugin for MyPlugin {
    fn metadata(&self) -> &PluginMetadata {
        &self.meta
    }

    fn init(&self, _caps: &mut PluginCapabilities) -> blaze::error::Result<()> {
        // Register custom functions, storage backends, etc.
        Ok(())
    }
}
```

## Plugin Lifecycle

1. **Load** – Plugin is discovered and loaded via `PluginRegistry::load()`.
2. **Initialize** – `init()` is called with a mutable `PluginCapabilities`
   handle so the plugin can register its extensions.
3. **Active** – The plugin provides functionality to the engine.
4. **Unload** – `PluginRegistry::unload()` removes the plugin and triggers
   any registered `on_unload` callbacks.

## Hot Reload

The registry supports hot reload during development:

```rust
use blaze::plugin::PluginRegistry;

let registry = PluginRegistry::new();
registry.enable_hot_reload(true);
```

When hot reload is enabled the registry watches plugin search paths and
automatically reloads plugins whose files change on disk.

## Plugin Search Paths

Add directories where the registry should look for dynamic-library plugins:

```rust
registry.add_search_path("/usr/local/lib/blaze/plugins");
```

## Callbacks

You can register callbacks that fire when plugins are loaded or unloaded:

```rust
registry.on_load(|name, meta| {
    println!("Loaded plugin {} v{}", name, meta.version);
});

registry.on_unload(|name| {
    println!("Unloaded plugin {}", name);
});
```
