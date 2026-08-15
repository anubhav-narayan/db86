"""
UI Service for DB86 Atlas Studio.
Handles discovering and mounting the frontend static assets onto a FastAPI application.
"""
import os
import logging
from typing import Optional, List
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

log = logging.getLogger("DB86 UI Service")


class UIService:
    """
    Manages frontend UI static asset discovery and mounting for FastAPI.
    """

    def __init__(
        self,
        custom_dist_path: Optional[str] = None,
        mount_path: str = "/ui",
        mount_name: str = "ui",
    ):
        """
        Initialize the UI Service.

        Args:
            custom_dist_path (Optional[str]): Explicit path to UI dist directory.
            mount_path (str): The URL prefix where UI should be mounted. Default is '/ui'.
            mount_name (str): The internal mount route name. Default is 'ui'.
        """
        self.mount_path = mount_path
        self.mount_name = mount_name
        self.dist_path = custom_dist_path or self._discover_dist_path()

    def _discover_dist_path(self) -> Optional[str]:
        """
        Locates the compiled frontend assets across known directory structures.

        Returns:
            Optional[str]: Absolute path to the UI dist directory if found, otherwise None.
        """
        possible_paths: List[str] = [
            os.path.join(os.path.dirname(__file__), "..", "..", "ui", "dist"),
            os.path.join(os.path.dirname(__file__), "..", "..", "..", "db86-ui", "dist"),
            os.path.join(os.path.dirname(__file__), "..", "ui", "dist"),
            os.path.abspath("ui/dist"),
            os.path.abspath("db86-ui/dist"),
            os.path.abspath("../ui/dist"),
            os.path.abspath("../db86-ui/dist"),
        ]
        for path in possible_paths:
            if os.path.exists(path) and os.path.isdir(path):
                return os.path.abspath(path)
        return None

    @property
    def is_available(self) -> bool:
        """Returns True if built UI distribution files are found."""
        return self.dist_path is not None and os.path.isdir(self.dist_path)

    def mount(self, app: FastAPI) -> bool:
        """
        Mounts the UI onto the given FastAPI app instance.

        Args:
            app (FastAPI): The FastAPI application instance.

        Returns:
            bool: True if successfully mounted, False if dist assets not found.
        """
        if self.is_available and self.dist_path:
            log.info("Mounting DB86 Atlas UI from %s at %s", self.dist_path, self.mount_path)
            app.mount(
                self.mount_path,
                StaticFiles(directory=self.dist_path, html=True),
                name=self.mount_name,
            )
            return True
        else:
            log.debug("DB86 UI dist directory not found. Skipping UI mount.")
            return False
