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


class EcephysKS25Job(CodeOceanJobConfig):
    process_config: ProcessConfig = ProcessConfig(
        request=RunCapsuleRequest(
            pipeline_id=os.getenv("ECEPHYS_KS25_PIPELINE_ID"),
            version=os.getenv("ECEPHYS_KS25_PIPELINE_VERSION"),
        ),
        input_data_mount=os.getenv("ECEPHYS_INPUT_MOUNT")
    )
    capture_config: CaptureConfig = CaptureConfig(
        process_name="sorted",
    )

class EcephysOptoKS25Job(CodeOceanJobConfig):
    process_config: ProcessConfig = ProcessConfig(
        request=RunCapsuleRequest(
            pipeline_id=os.getenv("ECEPHYS_OPTO_PIPELINE_ID"),
            version=os.getenv("ECEPHYS_KS25_OPTO_PIPELINE_VERSION"),
        ),
        input_data_mount=os.getenv("ECEPHYS_INPUT_MOUNT")
    )
    capture_config: CaptureConfig = CaptureConfig(
        process_name="sorted-opto",
    )

class EcephysKS4Job(CodeOceanJobConfig):
    process_config: ProcessConfig = ProcessConfig(
        request=RunCapsuleRequest(
            pipeline_id=os.getenv("ECEPHYS_KS4_PIPELINE_ID"),
            version=os.getenv("ECEPHYS_KS4_PIPELINE_VERSION"),
        ),
        input_data_mount=os.getenv("ECEPHYS_INPUT_MOUNT")
    )
    capture_config: CaptureConfig = CaptureConfig(
        process_name="sorted",
    )

class EcephysSC2Job(CodeOceanJobConfig):
    process_config: ProcessConfig = ProcessConfig(
        request=RunCapsuleRequest(
            pipeline_id=os.getenv("ECEPHYS_SC2_PIPELINE_ID"),
            version=os.getenv("ECEPHYS_SC2_PIPELINE_VERSION"),
        ),
        input_data_mount=os.getenv("ECEPHYS_INPUT_MOUNT")
    )
    capture_config: CaptureConfig = CaptureConfig(
        process_name="sorted",
    )
