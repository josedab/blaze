"""Allow running as python -m blaze_kernel."""

from ipykernel.kernelapp import IPKernelApp
from blaze_kernel.kernel import BlazeKernel

IPKernelApp.launch_instance(kernel_class=BlazeKernel)
