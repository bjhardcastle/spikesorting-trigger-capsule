"""
Main file to execute code ocean jobs
"""

import argparse
import logging
import os
import sys
from pathlib import Path

from aind_codeocean_api.codeocean import CodeOceanClient
from aind_codeocean_api.models.computations_requests import (
    ComputationProcess
)
from aind_codeocean_utils.codeocean_job import CodeOceanJob
from aind_codeocean_utils.alert_bot import AlertBot
from dotenv import load_dotenv

from preconfigured_jobs import (
    EcephysJob,
    EcephysOptoJob
)

LOG_FMT = "%(asctime)s %(message)s"
LOG_DATE_FMT = "%Y-%m-%d %H:%M"

logging.basicConfig(format=LOG_FMT, datefmt=LOG_DATE_FMT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


valid_job_types = [
    "ecephys",
    "ecephys_opto",
]

process_names = dict(
    ecephys=dict(
        job_dispatch="capsule_aind_ephys_job_dispatch_4",
        nwb_subject="capsule_nwb_packaging_subject_capsule_10",
        preprocessing="capsule_aind_ephys_preprocessing_1",
        postprocessing="capsule_aind_ephys_postprocessing_5",
    ),
    ecephys_opto=dict(
        job_dispatch="capsule_aind_ephys_job_dispatch_4",
        nwb_subject="capsule_nwb_packaging_subject_capsule_10",
        preprocessing="capsule_opto_preprocess_ecephys_1",
        postprocessing="capsule_aind_ephys_postprocessing_5",
    ),
)

def construct_data_assets(input_id_str, mount_point_str):
    data_assets = []
    if input_id_str is None:
        return data_assets
    assert mount_point_str is not None, (
        "If input_data_asset_id is provided, input_data_mount "
        "should also be provided to attach the data assets."
    )
    input_ids = input_id_str.split(";")
    mounts = mount_point_str.split(";")
    for input_id, mount in zip(input_ids, mounts):
        data_assets.append(
            dict(
                id=input_id,
                mount=mount,
            )
        )
    return data_assets



parser = argparse.ArgumentParser(description="Trigger codeocean job")

parser.add_argument(
    "pipeline-type",
    type=str,
    help=(
        "Pipeline to trigger, either 'ecephys' or 'ecephys_opto'."
    ),
)
parser.add_argument(
    "input_data_asset_id",
    type=str,
    help=(
        "Input data asset to spike sort"
    ),
    nargs="?",
)
## SPIKE SORTING SPECIFIC ARGUMENTS

# job dispatch
parser.add_argument(
    "job-dispatch-concatenate",
    type=str,
    help="",
    nargs="?",
)
parser.add_argument(
    "job-dispatch-input",
    type=str,
    help="",
    nargs="?",
)
# NWB
parser.add_argument(
    "nwb-backend",
    type=str,
    help="hdf5 or zarr",
    nargs="?"
)
# preprocessing
parser.add_argument(
    "preprocessing-debug",
    type=str,
    help="Whether to run in DEBUG mode",
    nargs="?"
)
parser.add_argument(
    "preprocessing-denoising",
    type=str,
    help="Which denoising strategy to use. Can be 'cmr' or 'destripe'. Default 'cmr'",
    nargs="?"
)
parser.add_argument(
    "preprocessing-remove-out-channels",
    type=str,
    help="Whether to remove out channels",
    nargs="?"
)
parser.add_argument(
    "preprocessing-remove-bad-channels",
    type=str,
    help="Whether to remove bad channels",
    nargs="?"
)
parser.add_argument(
    "preprocessing-max-bad-channel-fraction",
    type=float,
    help="Maximum fraction of bad channels to remove. If more than this fraction, processing is skipped",
    nargs="?"
)
parser.add_argument(
    "preprocessing-motion",
    type=str,
    help="How to deal with motion correction. Can be 'skip', 'compute', or 'apply'. Default 'compute'",
    nargs="?"
)
parser.add_argument(
    "preprocessing-motion-preset",
    type=str,
    help="What motion preset to use. Can be 'nonrigid_accurate', 'kilosort_like', or 'nonrigid_fast_and_accurate'. Default 'nonrigid_fast_and_accurate'",
    nargs="?"
)
parser.add_argument(
    "preprocessing-debug-duration",
    type=int,
    help="Duration of clipped recording in debug mode. Default is 30 seconds. Only used if debug is enabled",
    nargs="?"
)
# postprocessing
parser.add_argument(
    "postprocessing-use-motion-corrected",
    type=str,
    help="Whether to use motion corrected data",
    nargs="?"
)



def main():
    """
    Main function to execute pipelines
    """
    # TODO: Add module to CodeOceanClient to parse configs.
    parameters = sys.argv[1:]
    args = parser.parse_args(parameters)

    pipeline_type = args.pipeline_type
    assert (
        pipeline_type in valid_job_types
    ), f"job_type must be one of: {valid_job_types}"
    input_data_asset_id = args.input_data_asset_id
    job_dispatch_concatenate = args.job_dispatch_concatenate
    job_dispatch_input = args.job_dispatch_input
    nwb_backend = args.nwb_backend
    preprocessing_debug = args.preprocessing_debug
    preprocessing_denoising = args.preprocessing_denoising
    preprocessing_remove_out_channels = args.preprocessing_remove_out_channels
    preprocessing_remove_bad_channels = args.preprocessing_remove_bad_channels
    preprocessing_max_bad_channel_fraction = args.preprocessing_max_bad_channel_fraction
    preprocessing_motion = args.preprocessing_motion
    preprocessing_motion_preset = args.preprocessing_motion_preset
    preprocessing_debug_duration = args.preprocessing_debug_duration
    postprocessing_use_motion_corrected = args.postprocessing_use_motion_corrected

    # Loading environment variables
    dotenv_path = Path(os.path.dirname(os.path.realpath(__file__))) / ".env"
    load_env_file = load_dotenv(dotenv_path=dotenv_path)
    logger.info(f"Load env file status: {load_env_file}")

    if not load_env_file:
        logger.error(f"Error loading env file in path {dotenv_path}")
        exit(1)

    # Create a code ocean client that can execute api calls
    co_client = CodeOceanClient(
        domain=os.getenv("CODEOCEAN_DOMAIN"), token=os.getenv("API_SECRET")
    )

    alert_bot_url = None
    
    # for each job type
    if pipeline_type == "ecephys":
        alert_bot_url = os.getenv("ECEPHYS_ALERT_BOT_URL")
        data_assets = construct_data_assets(
            input_data_asset_id,
            os.getenv("ECEPHYS_INPUT_MOUNT")
        )
        job_config = EcephysJob()
        job_config.process_config.request.data_assets = data_assets
    elif pipeline_type == "ecephys_opto":
        alert_bot_url = os.getenv("ECEPHYS_ALERT_BOT_URL")
        data_assets = construct_data_assets(
            input_data_asset_id,
            os.getenv("ECEPHYS_OPTO_INPUT_MOUNT")
        )
        job_config = EcephysOptoJob()
        job_config.process_config.request.data_assets = data_assets
    else:
        logger.error(
            f"""
            Pipeline job_type {pipeline_type} not recognized. 
            Please enter a valid job_type among: {valid_job_types}.
            """
        )

    # Update processes with parameters
    job_dispatch_parameters = ["--input", job_dispatch_input]
    if job_dispatch_concatenate:
        job_dispatch_parameters.append("--concatenate")
    job_dispatch_process = ComputationProcess(
        name=process_names[pipeline_type]["job_dispatch"],
        parameters=job_dispatch_parameters
    )

    nwb_subject_parameters = [
        "--backend", nwb_backend
    ]
    nwb_subject_process = ComputationProcess(
        name=process_names[pipeline_type]["nwb_subject"],
        parameters=nwb_subject_parameters
    )

    preprocessing_parameters = [
        "--denoising", preprocessing_denoising,
        "--max-bad-channel-fraction", preprocessing_max_bad_channel_fraction,
        "--motion", preprocessing_motion,
        "--motion-preset", preprocessing_motion_preset,
        "--debug-duration", preprocessing_debug_duration
    ]
    if preprocessing_debug:
        preprocessing_parameters.append("--debug")
    if not preprocessing_remove_out_channels:
        preprocessing_parameters.append("--no-remove-out-channels")
    if not preprocessing_remove_bad_channels:
        preprocessing_parameters.append("--no-remove-bad-channels")
    preprocessing_process = ComputationProcess(
        name=process_names[pipeline_type]["preprocessing"],
        parameters=preprocessing_parameters
    )

    # TODO: postprocessing

    processes = [
        job_dispatch_process,
        nwb_subject_process,
        preprocessing_process
    ]

    job_config.process_config.request.processes = processes


    if alert_bot_url:
        alert_bot = AlertBot(alert_bot_url)
        data_asset_response = co_client.get_data_asset(input_data_asset_id)
        data_asset_json = data_asset_response.json()
        session_name = data_asset_json["name"]
    else:
        alert_bot = None

    print(f"Register config:\n{job_config.register_config}")
    print(f"Process config:\n{job_config.process_config}")
    print(f"Capture config:\n{job_config.capture_config}")


    codeocean_job = CodeOceanJob(
        co_client=co_client, job_config=job_config
    )
    if alert_bot:
        alert_bot.send_message(f"Starting {session_name}")
    # run the job
    try:
        codeocean_job.run_job()
        if alert_bot:
            alert_bot.send_message(message=f"Finished {session_name}")
    except Exception as e:
        if alert_bot:
            alert_bot.send_message(
                message=f"Error with {session_name}", extra_text=str(e)
            )
        raise e


if __name__ == "__main__":
    main()
