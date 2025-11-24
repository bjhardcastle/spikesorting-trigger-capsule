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

KS25_PIPELINE_VERSION = os.getenv("ECEPHYS_KS25_PIPELINE_VERSION")
KS4_PIPELINE_VERSION = os.getenv("ECEPHYS_KS4_PIPELINE_VERSION")
SC2_PIPELINE_VERSION = os.getenv("ECEPHYS_SC2_PIPELINE_VERSION")


class EcephysKS25Job(CodeOceanJobConfig):
    process_config: ProcessConfig = ProcessConfig(
        request=RunCapsuleRequest(
            pipeline_id=os.getenv("ECEPHYS_KS25_PIPELINE_ID"),
            version=int(KS25_PIPELINE_VERSION) if KS25_PIPELINE_VERSION is not None else None,
        ),
        input_data_mount=os.getenv("ECEPHYS_INPUT_MOUNT")
    )
    capture_config: CaptureConfig = CaptureConfig(
        process_name="sorted",
    )

class EcephysKS4Job(CodeOceanJobConfig):
    process_config: ProcessConfig = ProcessConfig(
        request=RunCapsuleRequest(
            pipeline_id=os.getenv("ECEPHYS_KS4_PIPELINE_ID"),
            version=int(KS4_PIPELINE_VERSION) if KS4_PIPELINE_VERSION is not None else None,
        ),
        input_data_mount=os.getenv("ECEPHYS_INPUT_MOUNT")
    )
    capture_config: CaptureConfig = CaptureConfig(
        process_name="sorted",
    )


class EcephysKS4MainJob(CodeOceanJobConfig):
    process_config: ProcessConfig = ProcessConfig(
        request=RunCapsuleRequest(
            pipeline_id=os.getenv("ECEPHYS_KS4_MAIN_PIPELINE_ID"),
            version=None,
        ),
        input_data_mount=os.getenv("ECEPHYS_INPUT_MOUNT")
    )
    capture_config: CaptureConfig = CaptureConfig(
        process_name="sorted",
    )

class EcephysKS4DevJob(CodeOceanJobConfig):
    process_config: ProcessConfig = ProcessConfig(
        request=RunCapsuleRequest(
            pipeline_id=os.getenv("ECEPHYS_KS4_DEV_PIPELINE_ID"),
            version=None,
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
            version=int(SC2_PIPELINE_VERSION) if SC2_PIPELINE_VERSION is not None else None,
        ),
        input_data_mount=os.getenv("ECEPHYS_INPUT_MOUNT")
    )
    capture_config: CaptureConfig = CaptureConfig(
        process_name="sorted",
    )

class EcephysKS25OptoJob(CodeOceanJobConfig):
    process_config: ProcessConfig = ProcessConfig(
        request=RunCapsuleRequest(
            pipeline_id=os.getenv("ECEPHYS_KS25_OPTO_PIPELINE_ID"),
            version=os.getenv("ECEPHYS_KS25_OPTO_PIPELINE_VERSION"),
        ),
        input_data_mount=os.getenv("ECEPHYS_INPUT_MOUNT")
    )
    capture_config: CaptureConfig = CaptureConfig(
        process_name="sorted-opto",
    )

class EcephysKS4OptoJob(CodeOceanJobConfig):
    process_config: ProcessConfig = ProcessConfig(
        request=RunCapsuleRequest(
            pipeline_id=os.getenv("ECEPHYS_KS4_OPTO_PIPELINE_ID"),
            version=os.getenv("ECEPHYS_KS4_OPTO_PIPELINE_VERSION"),
        ),
        input_data_mount=os.getenv("ECEPHYS_INPUT_MOUNT")
    )
    capture_config: CaptureConfig = CaptureConfig(
        process_name="sorted-opto",
    )

class EcephysSC2OptoJob(CodeOceanJobConfig):
    process_config: ProcessConfig = ProcessConfig(
        request=RunCapsuleRequest(
            pipeline_id=os.getenv("ECEPHYS_SC2_OPTO_PIPELINE_ID"),
            version=os.getenv("ECEPHYS_SC2_OPTO_PIPELINE_VERSION"),
        ),
        input_data_mount=os.getenv("ECEPHYS_INPUT_MOUNT")
    )
    capture_config: CaptureConfig = CaptureConfig(
        process_name="sorted-opto",
    )

class EcephysKS25LegacyJob(CodeOceanJobConfig):
    process_config: ProcessConfig = ProcessConfig(
        request=RunCapsuleRequest(
            pipeline_id=os.getenv("ECEPHYS_KS25_LEGACY_PIPELINE_ID"),
            version=os.getenv("ECEPHYS_KS25_LEGACY_PIPELINE_VERSION"),
        ),
        input_data_mount=os.getenv("ECEPHYS_INPUT_MOUNT")
    )
    capture_config: CaptureConfig = CaptureConfig(
        process_name="sorted",
    )