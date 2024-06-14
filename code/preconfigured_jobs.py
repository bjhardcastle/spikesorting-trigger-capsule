import os
from pathlib import Path
from aind_codeocean_api.models.computations_requests import (
    RunCapsuleRequest
)
from aind_codeocean_utils.codeocean_job import (
    CodeOceanJobConfig, ProcessConfig, CaptureConfig
)
from dotenv import load_dotenv


# Loading environment variables
dotenv_path = Path(os.path.dirname(os.path.realpath(__file__))) / ".env"
load_env_file = load_dotenv(dotenv_path=dotenv_path)


class EcephysJob(CodeOceanJobConfig):
    process_config: ProcessConfig = ProcessConfig(
        request=RunCapsuleRequest(
            pipeline_id=os.getenv("ECEPHYS_PIPELINE_ID"),
            version=os.getenv("ECEPHYS_PIPELINE_VERSION"),
        ),
        input_data_mount=os.getenv("ECEPHYS_INPUT_MOUNT")
    )
    capture_config: CaptureConfig = CaptureConfig(
        process_name="sorted",
    )

class EcephysOptoJob(CodeOceanJobConfig):
    process_config: ProcessConfig = ProcessConfig(
        request=RunCapsuleRequest(
            pipeline_id=os.getenv("ECEPHYS_OPTO_PIPELINE_ID"),
            version=os.getenv("ECEPHYS_OPTO_PIPELINE_VERSION"),
        ),
        input_data_mount=os.getenv("ECEPHYS_OPTO_INPUT_MOUNT")
    )
    capture_config: CaptureConfig = CaptureConfig(
        process_name="sorted-opto",
    )
