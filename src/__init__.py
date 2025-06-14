"""Init with automatic AB Cancel Translator stub loading."""
import os
import sys
import subprocess
import logging
from pathlib import Path

__version__ = "0.0.9"
__author__ = "ravi@databricks.com"

logger = logging.getLogger("dlt-meta")

class ABCancelTranslatorLoader:
    """Automatically load AB Cancel Translator from catalog volume"""
    
    def __init__(self):
        self.volume_paths = [
            "/Volumes/catalog_dlt_meta/default/dlt-meta-volume/ab_cancel_translator_stubs/ab_cancel_translator_stubs-1.0.0-py3-none-any.whl",
            # Add your specific path here
        ]
        self.is_loaded = False
        self.version = None
    
    def find_wheel_in_volumes(self):
        """Find AB Cancel Translator wheel in catalog volumes"""
        for wheel_path in self.volume_paths:
            if os.path.exists(wheel_path):
                return wheel_path
        return None
    
    def load_from_wheel(self, wheel_path):
        """Load AB Cancel Translator directly from wheel without pip install"""
        try:
            # Add wheel to sys.path for direct import
            if wheel_path not in sys.path:
                sys.path.insert(0, wheel_path)
            
            # Test import
            import ab_cancel_translator_stubs
            self.is_loaded = True
            self.version = getattr(ab_cancel_translator_stubs, '__version__', 'unknown')
            logger.info(f"AB Cancel Translator loaded from wheel: {wheel_path}")
            return True
            
        except Exception as e:
            logger.warning(f"Failed to load from wheel {wheel_path}: {e}")
            return False
    
    def install_if_needed(self, wheel_path):
        """Install from wheel if not already available"""
        try:
            # Check if already installed
            import ab_cancel_translator_stubs
            self.is_loaded = True
            self.version = getattr(ab_cancel_translator_stubs, '__version__', 'unknown')
            return True
        except ImportError:
            pass
        
        try:
            # Silent installation
            subprocess.check_call([
                sys.executable, "-m", "pip", "install", wheel_path, "--quiet"
            ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            
            # Verify installation
            import ab_cancel_translator_stubs
            self.is_loaded = True
            self.version = getattr(ab_cancel_translator_stubs, '__version__', 'unknown')
            logger.info(f"AB Cancel Translator auto-installed from: {wheel_path}")
            return True
            
        except Exception as e:
            logger.warning(f"Auto-installation failed: {e}")
            return False
    
    def ensure_available(self):
        """Ensure AB Cancel Translator is available"""
        if self.is_loaded:
            return True
        
        # Try to import if already installed
        try:
            import ab_cancel_translator_stubs
            self.is_loaded = True
            self.version = getattr(ab_cancel_translator_stubs, '__version__', 'unknown')
            return True
        except ImportError:
            pass
        
        # Find wheel in catalog volumes
        wheel_path = self.find_wheel_in_volumes()
        if wheel_path:
            # Try direct loading first (faster)
            if self.load_from_wheel(wheel_path):
                return True
            
            # Fallback to installation
            if self.install_if_needed(wheel_path):
                return True
        
        logger.warning("AB Cancel Translator not available in any catalog volume")
        return False

# Initialize AB loader globally
_ab_loader = ABCancelTranslatorLoader()

# Auto-load on framework import
AB_CANCEL_TRANSLATOR_AVAILABLE = _ab_loader.ensure_available()
AB_CANCEL_TRANSLATOR_VERSION = _ab_loader.version

# Make loader available to other modules
def get_ab_loader():
    return _ab_loader
