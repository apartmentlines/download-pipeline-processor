class TransientPipelineError(Exception):
    """Custom exception for transient pipeline errors."""

    def __init__(self, exception):
        super().__init__(f"Transient pipeline error: {str(exception)}")
