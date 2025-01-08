import pytest

from download_pipeline_processor.error import TransientPipelineError

def test_transcription_error_message():
    original_exception = RuntimeError("Out of memory")
    with pytest.raises(TransientPipelineError) as exc_info:
        raise TransientPipelineError(original_exception)
    assert str(exc_info.value) == "Transient pipeline error: Out of memory"
