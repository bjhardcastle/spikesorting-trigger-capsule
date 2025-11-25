"""
Main file to execute code ocean jobs
"""

import argparse
import datetime
import logging
import os
import sys
from pathlib import Path
from typing import Optional

from aind_codeocean_utils.alert_bot import AlertBot
from dotenv import load_dotenv
from codeocean.client import CodeOcean as CodeOceanClient
from codeocean.computation import RunParams, DataAssetsRunParam, PipelineProcessParams, ComputationEndStatus
from codeocean.data_asset import DataAssetParams, ComputationSource, Source, Permissions, AWSS3Source

from job_config_models import get_job_config

LOG_FMT = "%(asctime)s %(message)s"
LOG_DATE_FMT = "%Y-%m-%d %H:%M"

logging.basicConfig(format=LOG_FMT, datefmt=LOG_DATE_FMT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)



def construct_data_assets(input_data_asset_id: Optional[str], input_data_mount: Optional[str]):
    data_assets: list[DataAssetsRunParam] = []
    if input_data_asset_id is None:
        return data_assets
    if input_data_mount is None:
        raise ValueError(
            "If input_data_asset_id is provided, input_data_mount must also be provided to attach the data assets."
        )
    input_ids = input_data_asset_id.split(";")
    mounts = input_data_mount.split(";")
    if len(input_ids) != len(mounts):
        raise ValueError(
            f"The number of input_data_asset_id and input_data_mount entries must be the same. Got: {input_ids!r} and {mounts!r}"
        )
    for input_id, mount in zip(input_ids, mounts):
        data_assets.append(
            DataAssetsRunParam(
                id=input_id,
                mount=mount,
            )
        )
    return data_assets



parser = argparse.ArgumentParser(description="Trigger codeocean job")

parser.add_argument(
    "pipeline_type",
    type=str,
    help="Pipeline to trigger (see job_configs.json for available types)",
)
parser.add_argument(
    "input_data_asset_id",
    type=str,
    help=(
        "Input data asset to spike sort"
    ),
    nargs="?",
)
parser.add_argument(
    "result_suffix",
    type=str,
    help=(
        "If provided, the result suffix to append to the processed data asset. "
        "Default is 'sorted' for 'ecephys', 'sorted-opto' for 'ecephys_opto'"
    ),
    nargs="?",
)
parser.add_argument(
    "output_bucket",
    type=str,
    help="Bucket to save data to",
    nargs="?",
)
parser.add_argument(
    "resume_run_id",
    type=str,
    help="Computation ID of a previous run to resume",
    nargs="?",
)
## SPIKE SORTING SPECIFIC ARGUMENTS

# job dispatch
parser.add_argument(
    "job_dispatch_split_segments",
    type=str,
    help="",
    nargs="?",
)
parser.add_argument(
    "job_dispatch_split_groups",
    type=str,
    help="",
    nargs="?",
)
parser.add_argument(
    "job_dispatch_debug",
    type=str,
    help="Whether to run in DEBUG mode",
    nargs="?"
)
parser.add_argument(
    "job_dispatch_debug_duration",
    type=int,
    help="Duration of clipped recording in debug mode. Default is 30 seconds. Only used if debug is enabled",
    nargs="?"
)
parser.add_argument(
    "job_dispatch_skip_timestamps_check",
    type=str,
    help="",
    nargs="?",
)
parser.add_argument(
    "job_dispatch_input",
    type=str,
    help="",
    nargs="?",
)
# NWB
parser.add_argument(
    "nwb_backend",
    type=str,
    help="hdf5 or zarr",
    nargs="?"
)
# preprocessing
parser.add_argument(
    "preprocessing_denoising",
    type=str,
    help="Which denoising strategy to use. Can be 'cmr' or 'destripe'. Default 'cmr'",
    nargs="?"
)
parser.add_argument(
    "preprocessing_filter_type",
    type=str,
    help="Which filter to use. Can be 'highpass' or 'bandpass'. Default 'highpass'",
    nargs="?"
)
parser.add_argument(
    "preprocessing_remove_out_channels",
    type=str,
    help="Whether to remove out channels",
    nargs="?"
)
parser.add_argument(
    "preprocessing_remove_bad_channels",
    type=str,
    help="Whether to remove bad channels",
    nargs="?"
)
parser.add_argument(
    "preprocessing_max_bad_channel_fraction",
    type=float,
    help="Maximum fraction of bad channels to remove. If more than this fraction, processing is skipped",
    nargs="?"
)
parser.add_argument(
    "preprocessing_motion",
    type=str,
    help="How to deal with motion correction. Can be 'skip', 'compute', or 'apply'. Default 'compute'",
    nargs="?"
)
parser.add_argument(
    "preprocessing_motion_preset",
    type=str,
    help="What motion preset to use. Can be 'nonrigid_accurate', 'kilosort_like', or 'nonrigid_fast_and_accurate'. Default 'nonrigid_fast_and_accurate'",
    nargs="?"
)
parser.add_argument(
    "preprocessing_motion_temporal_bin_s",
    type=float,
    help="Temporal bin size in seconds for motion estimation",
    nargs="?"
)
parser.add_argument(
    "preprocessing_t_start",
    type=str,
    help="Start time of clipped recording. Default is None",
    nargs="?"
)
parser.add_argument(
    "preprocessing_t_stop",
    type=str,
    help="Stop time of clipped recording. Default is None",
    nargs="?"
)
parser.add_argument(
    "preprocessing_min_duration",
    type=str,
    help="Min duration for preprocessing. Default is 120",
    nargs="?"
)
# spike sorting
parser.add_argument(
    "spikesorting_raise_if_fails",
    type=str,
    help="Whether to raise an error in case of failure or continue. True means 'raise'",
    nargs="?"
)
parser.add_argument(
    "spikesorting_skip_motion",
    type=str,
    help="Whether to apply the sorter motion correction.",
    nargs="?"
)
parser.add_argument(
    "spikesorting_min_channels_motion",
    type=str,
    help="Minimum number of channels to enable motion correction",
    nargs="?"
)
parser.add_argument(
    "spikesorting_clear_cache",
    type=str,
    help="Whether to enable Kilosort4 clear cache.",
    nargs="?"
)
# postprocessing
parser.add_argument(
    "postprocessing_use_motion_corrected",
    type=str,
    help="Whether to use motion corrected data",
    nargs="?"
)



def main():
    """
    Main function to execute pipelines
    """

    parameters = sys.argv[1:]
    args = parser.parse_args(parameters)

    job_config = get_job_config(args.pipeline_type)

    result_suffix: Optional[str] = args.result_suffix or None
    output_bucket: Optional[str] = args.output_bucket or None
    resume_run_id: Optional[str] = args.resume_run_id or None
    input_data_asset_id: str = args.input_data_asset_id
    job_dispatch_split_segments = args.job_dispatch_split_segments
    job_dispatch_split_groups = args.job_dispatch_split_groups
    job_dispatch_debug = args.job_dispatch_debug
    job_dispatch_debug_duration = args.job_dispatch_debug_duration
    job_dispatch_skip_timestamps_check = args.job_dispatch_skip_timestamps_check
    job_dispatch_input = args.job_dispatch_input
    nwb_backend = args.nwb_backend
    preprocessing_denoising = args.preprocessing_denoising
    preprocessing_filter_type = args.preprocessing_filter_type
    preprocessing_remove_out_channels = args.preprocessing_remove_out_channels
    preprocessing_remove_bad_channels = args.preprocessing_remove_bad_channels
    preprocessing_max_bad_channel_fraction = args.preprocessing_max_bad_channel_fraction
    preprocessing_motion = args.preprocessing_motion
    preprocessing_motion_preset = args.preprocessing_motion_preset
    preprocessing_motion_temporal_bin_s = args.preprocessing_motion_temporal_bin_s
    preprocessing_t_start = args.preprocessing_t_start
    preprocessing_t_stop = args.preprocessing_t_stop
    preprocessing_min_duration = args.preprocessing_min_duration
    spikesorting_raise_if_fails = args.spikesorting_raise_if_fails
    spikesorting_skip_motion = args.spikesorting_skip_motion
    spikesorting_min_channels_motion = args.spikesorting_min_channels_motion
    spikesorting_clear_cache = args.spikesorting_clear_cache
    postprocessing_use_motion_corrected = args.postprocessing_use_motion_corrected

    if ";" in input_data_asset_id:
        raise NotImplementedError("Attempted to process multiple data assets. This is no longer supported.")

    # Update processes with parameters
    if job_config.name == "ecephys_ks25_v0.1.0":
        # for previous versions, the parameter was 'concatenate' instead of 'split-segments'
        if job_dispatch_split_segments == "true":
            job_dispatch_split_segments = "false"
        else:
            job_dispatch_split_segments = "true"
    job_dispatch_parameters = [
        job_dispatch_split_segments,
        job_dispatch_split_groups
    ]
    if job_config.name == "ecephys_ks25_v0.1.0":
        job_dispatch_parameters.append(job_dispatch_input)
    else:
        job_dispatch_parameters.extend(
            [
                job_dispatch_debug,
                job_dispatch_debug_duration,
                job_dispatch_skip_timestamps_check,
                job_dispatch_input
            ]
        )
    print(job_dispatch_parameters)

    job_dispatch_process = PipelineProcessParams(
        name=job_config.process_names.job_dispatch,
        parameters=[str(p) for p in job_dispatch_parameters]
    )

    nwb_parameters = [nwb_backend]
    if job_config.name != "ecephys_ks25_v0.1.0":
        backend_process = "nwb_ecephys"
    else:
        backend_process = "nwb_subject"
    nwb_backend_process = PipelineProcessParams(
        name=job_config.process_names.model_dump().get(backend_process),
        parameters=[str(p) for p in nwb_parameters]
    )

    if job_config.name == "ecephys_ks25_v0.1.0":
        # dredge was not supported
        if preprocessing_motion_preset == "dredge":
            preprocessing_motion_preset = "nonrigid_accurate"
        elif preprocessing_motion_preset == "dredge_fast":
            preprocessing_motion_preset = "nonrigid_fast_and_accurate"
        preprocessing_parameters = [job_dispatch_debug]
    else:
        preprocessing_parameters = []
    preprocessing_parameters.extend(
        [
            preprocessing_denoising,
            preprocessing_filter_type,
            preprocessing_remove_out_channels,
            preprocessing_remove_bad_channels,
            preprocessing_max_bad_channel_fraction,
            preprocessing_motion,
            preprocessing_motion_preset
        ]
    )
    if job_config.name != "ecephys_ks25_v0.1.0":
        preprocessing_parameters.extend(
            [
                preprocessing_motion_temporal_bin_s,
                preprocessing_t_start,
                preprocessing_t_stop,
            ]
        )
    else:
        preprocessing_parameters.extend(
            [
                preprocessing_t_start,
                preprocessing_t_stop,
            ]
        )
    if job_config.name == "ecephys_ks25_v0.1.0":
        preprocessing_parameters.append(job_dispatch_debug_duration)
    else:
        preprocessing_parameters.append(preprocessing_min_duration)

    preprocessing_process = PipelineProcessParams(
        name=job_config.process_names.preprocessing,
        parameters=[str(p) for p in preprocessing_parameters]
    )

    spikesorting_parameters = [
        spikesorting_raise_if_fails,
        spikesorting_skip_motion,
        spikesorting_min_channels_motion
    ]
    # ks4 accepts an additional clear_cache parameter
    if "ks4" in job_config.name:
        spikesorting_parameters.append(spikesorting_clear_cache)
    if job_config.name != "ecephys_ks25_v0.1.0":
        spikesorting_process = PipelineProcessParams(
            name=job_config.process_names.spikesorting,
            parameters=[str(p) for p in spikesorting_parameters]
        )
    else:
        spikesorting_process = None

    postprocessing_parameters = [postprocessing_use_motion_corrected]
    postprocessing_process = PipelineProcessParams(
        name=job_config.process_names.postprocessing,
        parameters=[str(p) for p in postprocessing_parameters]
    )

    collect_results_parameters = [result_suffix]
    collect_results_process = PipelineProcessParams(
        name=job_config.process_names.collect_results,
        parameters=[str(p) for p in collect_results_parameters]
    )

    processes = [
        job_dispatch_process,
        nwb_backend_process,
        preprocessing_process,
        collect_results_process
    ]

    # For the KS4 pipeline, app panel for postprocessing is disabled for shared-mem issues
    if job_config.name != "ecephys_ks25_v0.1.0":
        processes.append(postprocessing_process)

    if spikesorting_process is not None:
        processes.append(spikesorting_process)
        
    # Load environment variables to create client
    dotenv_path = Path(os.path.dirname(os.path.realpath(__file__))) / ".env"
    load_env_file = load_dotenv(dotenv_path=dotenv_path)
    logger.info(f"Load env file status: {load_env_file}")

    if not load_env_file:
        logger.error(f"Error loading env file from {dotenv_path}")
        exit(1)

    # Create a code ocean client that can execute api calls
    co_client = CodeOceanClient(
        domain=os.environ["CODEOCEAN_DOMAIN"], token=os.environ["API_SECRET"], retries=3,
    )
    input_data_asset_info = co_client.data_assets.get_data_asset(input_data_asset_id)


    alert_bot_url = os.getenv("ECEPHYS_ALERT_BOT_URL")
    if alert_bot_url:
        logger.info("Creating alert bot")
        alert_bot = AlertBot(alert_bot_url)
    else:
        alert_bot = None

    logger.info("Fetching input data asset info")
    input_data_asset_params = DataAssetsRunParam(
        id=input_data_asset_id,
        mount=job_config.input_data_mount,
    )

    run_params = RunParams(
        pipeline_id=job_config.pipeline_id,
        version=job_config.version,
        data_assets=[input_data_asset_params],
        processes=processes,
        resume_run_id=resume_run_id,
    )
    print(f"Pipeleine run params:\n{run_params.to_dict()}")


    logger.info(f"Starting pipeline for {input_data_asset_info.name}")
    if alert_bot:
        logger.info("Sending alert")
        alert_bot.send_message(f"Starting pipeline {job_config.name} for {input_data_asset_info.name}")
    # run the job
    try:
        computation = co_client.computations.run_capsule(run_params)
    except Exception as e:
        if alert_bot:
            alert_bot.send_message(
                message=f"Error with {input_data_asset_info.name}", extra_text=str(e)
            )
        raise e
    else:
        if alert_bot:
            alert_bot.send_message(message=f"Finished pipeline {job_config.name} for {input_data_asset_info.name}")
    
    logger.info("Preparing data asset capture parameters")
    captured_asset_name = f"{input_data_asset_info.name}_{job_config.captured_asset_label}_{datetime.datetime.now().isoformat(sep='_', timespec='seconds')}"
    platform, subject_id = input_data_asset_info.name.split("_")[:2]
    if output_bucket is not None:
        source = Source(
            aws=AWSS3Source(bucket=output_bucket, prefix=captured_asset_name)
        )
    else:
        source = Source(computation=ComputationSource(id=computation.id))
    asset_capture_params = DataAssetParams(
        name=captured_asset_name,
        tags=["derived", platform, subject_id],
        mount=captured_asset_name,
        custom_metadata={
            "data level": "derived",
            "experiment type": platform,
            "subject id": subject_id,
        },
        source=source,
    )
    print(f"Waiting for sorting to finish, then capturing result as a data asset with params:\n{asset_capture_params.to_dict()}")
    
    completed_computation = co_client.computations.wait_until_completed(computation=computation, polling_interval=300, timeout=7 * 24 * 3600)
    
    logger.info(f"Sorting finished: {completed_computation.end_status}")
    if not completed_computation.end_status == ComputationEndStatus.Succeeded:
        logger.error(f"Computation ended with status {completed_computation.end_status}, not 'succeeded'. Not capturing result.")
        if alert_bot:
            logger.info("Sending alert")
            alert_bot.send_message(
                message=f"Computation for {input_data_asset_info.name} ended with status {completed_computation.end_status}, not 'succeeded'. Result not captured."
            )
        exit(1)
    
    assert completed_computation.id == computation.id, f"Completed computation ID {completed_computation.id!r} does not match the original computation ID {computation.id!r}: something wrong with computation wait code or codeocean API has changed"
    
    logger.info("Capturing result as sorted data asset")
    captured_asset = co_client.data_assets.create_data_asset(data_asset_params=asset_capture_params)
    ready_captured_asset = co_client.data_assets.wait_until_ready(data_asset=captured_asset, polling_interval=10, timeout=300)
    assert ready_captured_asset.id == captured_asset.id, f"Asset ID after waiting for readiness {ready_captured_asset.id!r} does not match the original captured asset ID {captured_asset.id!r}: something wrong with asset capture code or codeocean API has changed"

    logger.info("Updating captured asset permissions to be viewable by everyone")
    co_client.data_assets.update_permissions(
        data_asset_id=captured_asset.id,
        permissions=Permissions(everyone="viewer", share_assets=True)
    )
    logger.info(f"Captured sorted data asset with ID {captured_asset.id!r} and updated permissions.")
    
if __name__ == "__main__":
    main()
