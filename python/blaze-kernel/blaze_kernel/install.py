"""Install the Blaze SQL kernel into Jupyter."""

import json
import os
import sys


def install_kernel():
    """Install the kernel spec for Jupyter."""
    from jupyter_client.kernelspec import KernelSpecManager

    kernel_spec = {
        "argv": [sys.executable, "-m", "blaze_kernel", "-f", "{connection_file}"],
        "display_name": "Blaze SQL",
        "language": "sql",
    }

    # Write kernel.json to a temp directory
    kernel_dir = os.path.join(os.path.dirname(__file__), "kernel_spec")
    os.makedirs(kernel_dir, exist_ok=True)
    with open(os.path.join(kernel_dir, "kernel.json"), "w") as f:
        json.dump(kernel_spec, f)

    ksm = KernelSpecManager()
    ksm.install_kernel_spec(kernel_dir, kernel_name="blaze_sql", user=True)
    print("Installed Blaze SQL kernel.")


if __name__ == "__main__":
    install_kernel()
