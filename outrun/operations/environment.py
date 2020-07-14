"""Module that contains the local environment RPC service."""

import os
from typing import Dict, List


class LocalEnvironmentService:
    """An RPC service with details about the local environment."""

    def __init__(self, command: List[str]):
        """Construct the local environment service with the command to be executed."""
        self._command = command

    def get_command(self) -> List[str]:
        """Get the command to be executed."""
        return self._command

    @staticmethod
    def get_working_dir() -> str:
        """Get the working directory at the moment of execution."""
        return os.getcwd()

    @staticmethod
    def get_environment() -> Dict[str, str]:
        """Get all environment variables at the moment of execution."""
        return dict(os.environ)
