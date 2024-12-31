"""Utility functions for testing."""

import requests


class MockRequestUtils:
    """Mock request utilities for testing."""

    @staticmethod
    def mock_successful_request(*args, **kwargs):
        """Helper method to mock successful requests."""

        class MockResponse:
            def raise_for_status(self):
                pass

            @property
            def content(self):
                return b"mock file content"

        return MockResponse()

    @staticmethod
    def mock_failed_request(*args, **kwargs):
        """Helper method to mock failed requests."""

        class MockResponse:
            def raise_for_status(self):
                raise requests.exceptions.RequestException("Invalid URL")

        return MockResponse()
