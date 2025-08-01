"""XLSX Reader Microkernel - A plugin-based Excel processing system."""

__version__ = "0.1.0"
__author__ = "XLS Reader Team"
__email__ = "team@xlsreader.com"

from .core.kernel import XLSReaderKernel
from .core.exceptions import XLSReaderError

__all__ = ["XLSReaderKernel", "XLSReaderError"] 