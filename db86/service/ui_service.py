"""
UI Service for DB86 Atlas Studio.
Handles discovering, configuring CORS, and mounting frontend static assets onto a FastAPI application.
"""
import os
import logging
from typing import Optional, List
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

log = logging.getLogger("DB86 UI Service")


class UIService:
    """
    Manages frontend UI static asset discovery, CORS configuration, and mounting for FastAPI.
    """

    def __init__(
        self,
        custom_dist_path: Optional[str] = None,
        mount_path: str = "/ui",
        mount_name: str = "ui",
        enable_cors: bool = True,
        cors_origins: Optional[List[str]] = None,
    ):
        """
        Initialize the UI Service.

        Args:
            custom_dist_path (Optional[str]): Explicit path to UI dist directory.
            mount_path (str): The URL prefix where UI should be mounted. Default is '/ui'.
            mount_name (str): The internal mount route name. Default is 'ui'.
            enable_cors (bool): Whether to enable CORS middleware on the FastAPI app. Default is True.
            cors_origins (Optional[List[str]]): List of allowed CORS origins. Default is ["*"].
        """
        self.mount_path = mount_path
        self.mount_name = mount_name
        self.enable_cors = enable_cors
        self.cors_origins = cors_origins or ["*"]
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

    def configure_cors(self, app: FastAPI) -> None:
        """
        Configures CORS middleware on the FastAPI app.

        Args:
            app (FastAPI): The FastAPI application instance.
        """
        if self.enable_cors:
            log.info("Configuring CORS middleware with allowed origins: %s", self.cors_origins)
            app.add_middleware(
                CORSMiddleware,
                allow_origins=self.cors_origins,
                allow_credentials=True,
                allow_methods=["*"],
                allow_headers=["*"],
            )

    def mount(self, app: FastAPI) -> bool:
        """
        Configures CORS and mounts the UI onto the given FastAPI app instance.

        Args:
            app (FastAPI): The FastAPI application instance.

        Returns:
            bool: True if successfully mounted, False if dist assets not found.
        """
        if self.enable_cors:
            self.configure_cors(app)

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
