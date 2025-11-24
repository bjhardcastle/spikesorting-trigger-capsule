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
    EcephysKS25Job,
    EcephysKS4Job,
    EcephysKS4MainJob,
    EcephysKS4DevJob,
    EcephysSC2Job,
    EcephysKS25OptoJob,
    EcephysKS4OptoJob,
    EcephysKS25LegacyJob
)

LOG_FMT = "%(asctime)s %(message)s"
LOG_DATE_FMT = "%Y-%m-%d %H:%M"

logging.basicConfig(format=LOG_FMT, datefmt=LOG_DATE_FMT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


valid_job_types = [
    "ecephys_ks25",
    "ecephys_ks4",
    "ecephys_ks4_main",
    "ecephys_ks4_dev",
    "ecephys_sc2",
    "ecephys_ks25_opto",
    "ecephys_ks4_opto",
    "ecephys_sc2_opto",
    "ecephys_ks25_v0.1.0",
    "ecephys_ks25_v0_1_0",
]

process_names = {
    "job_dispatch": "capsule_aind_ephys_job_dispatch_4",
    "nwb_subject": "capsule_nwb_packaging_subject_capsule_10",
    "nwb_ecephys": "capsule_nwb_packaging_ecephys_capsule_12",
    "preprocessing": "capsule_aind_ephys_preprocessing_1",
    "postprocessing": "capsule_aind_ephys_postprocessing_5",
    "collect_results": "capsule_aind_ephys_results_collector_9"
}
preprocessing_process_name = {
    "ecephys_ks25": "capsule_aind_ephys_preprocessing_1",
    "ecephys_ks4": "capsule_aind_ephys_preprocessing_1",
    "ecephys_ks4_main": "capsule_aind_ephys_preprocessing_1",
    "ecephys_ks4_dev": "capsule_aind_ephys_preprocessing_1",
    "ecephys_sc2": "capsule_aind_ephys_preprocessing_1",
    "ecephys_ks25_v0.1.0": "capsule_aind_ephys_preprocessing_1",
    "ecephys_ks25_v0_1_0": "capsule_aind_ephys_preprocessing_1",
    "ecephys_ks25_opto": "capsule_opto_preprocess_ecephys_1",
    "ecephys_ks4_opto": "capsule_opto_preprocess_ecephys_1",
    "ecephys_sc2_opto": "capsule_opto_preprocess_ecephys_1"
}
spikesorting_process_name = {
    "ecephys_ks25": "capsule_aind_ephys_spikesort_kilosort_25_7",
    "ecephys_ks4": "capsule_spikesort_kilosort_4_ecephys_7",
    "ecephys_ks4_main": "capsule_spikesort_kilosort_4_ecephys_7",
    "ecephys_ks4_dev": "capsule_spikesort_kilosort_4_ecephys_7",
    "ecephys_sc2": "capsule_spikesort_spyking_circus_2_ecephys_7",
    "ecephys_ks25_opto": "capsule_aind_ephys_spikesort_kilosort_25_7",
    "ecephys_ks4_opto": "capsule_spikesort_kilosort_4_ecephys_7",
    "ecephys_sc2_opto": "capsule_spikesort_spyking_circus_2_ecephys_7"
}

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
    "pipeline_type",
    type=str,
    help=(
        f"Pipeline to trigger: {valid_job_types}"
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
    # TODO: Add module to CodeOceanClient to parse configs.
    parameters = sys.argv[1:]
    args = parser.parse_args(parameters)

    pipeline_type = args.pipeline_type
    if pipeline_type == "ecephys":
        pipeline_type = "ecephys_ks25"
    elif pipeline_type == "ecephys_opto":
        pipeline_type = "ecephys_opto_ks25"
    assert (
        pipeline_type in valid_job_types
    ), f"job_type must be one of: {valid_job_types}"
    result_suffix = args.result_suffix
    if result_suffix == "":
        result_suffix = None
    output_bucket = args.output_bucket
    if output_bucket == "":
        output_bucket = None
    input_data_asset_id = args.input_data_asset_id
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

    alert_bot_url = os.getenv("ECEPHYS_ALERT_BOT_URL")
    data_assets = construct_data_assets(
        input_data_asset_id,
        os.getenv("ECEPHYS_INPUT_MOUNT")
    )

    # for each job type
    if pipeline_type == "ecephys_ks25":
        job_config = EcephysKS25Job()
    elif pipeline_type == "ecephys_ks25_opto":
        job_config = EcephysKS25OptoJob()
    elif pipeline_type == "ecephys_ks4":
        job_config = EcephysKS4Job()
    elif pipeline_type == "ecephys_ks4_main":
        job_config = EcephysKS4MainJob()
    elif pipeline_type == "ecephys_ks4_dev":
        job_config = EcephysKS4DevJob()
    elif pipeline_type == "ecephys_ks4_opto":
        job_config = EcephysKS4OptoJob()
    elif pipeline_type == "ecephys_sc2":
        job_config = EcephysSC2Job()
    elif pipeline_type == "ecephys_sc2_opto":
        job_config = EcephysSC2OptoJob()
    elif pipeline_type == "ecephys_ks25_v0.1.0" or pipeline_type == "ecephys_ks25_v0_1_0":
        pipeline_type = "ecephys_ks25_v0.1.0"
        job_config = EcephysKS25LegacyJob()
    else:
        logger.error(
            f"Pipeline job_type {pipeline_type} not recognized. "
            f"Please enter a valid job_type among: {valid_job_types}."
            
        )
        raise ValueError("Pipeline type not recognized")
    job_config.process_config.request.data_assets = data_assets

    if result_suffix is not None:
        print(f"Setting result process name to: {result_suffix}")
        job_config.capture_config.process_name = result_suffix

    if output_bucket is not None:
        print(f"Setting output bucket to: {output_bucket}")
        job_config.capture_config.output_bucket = output_bucket

    # Update processes with parameters
    if pipeline_type == "ecephys_ks25_v0.1.0":
        # for previous versions, the parameter was 'concatenate' instead of 'split-segments'
        if job_dispatch_split_segments == "true":
            job_dispatch_split_segments = "false"
        else:
            job_dispatch_split_segments = "true"
    job_dispatch_parameters = [
        job_dispatch_split_segments,
        job_dispatch_split_groups
    ]
    if pipeline_type == "ecephys_ks25_v0.1.0":
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

    job_dispatch_process = ComputationProcess(
        name=process_names["job_dispatch"],
        parameters=[str(p) for p in job_dispatch_parameters]
    )

    nwb_parameters = [nwb_backend]
    if pipeline_type != "ecephys_ks25_v0.1.0":
        backend_process = "nwb_ecephys"
    else:
        backend_process = "nwb_subject"
    nwb_backend_process = ComputationProcess(
        name=process_names[backend_process],
        parameters=[str(p) for p in nwb_parameters]
    )

    if pipeline_type == "ecephys_ks25_v0.1.0":
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
    if pipeline_type != "ecephys_ks25_v0.1.0":
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
    if pipeline_type == "ecephys_ks25_v0.1.0":
        preprocessing_parameters.append(job_dispatch_debug_duration)
    else:
        preprocessing_parameters.append(preprocessing_min_duration)

    preprocessing_process = ComputationProcess(
        name=preprocessing_process_name[pipeline_type],
        parameters=[str(p) for p in preprocessing_parameters]
    )

    spikesorting_parameters = [
        spikesorting_raise_if_fails,
        spikesorting_skip_motion,
        spikesorting_min_channels_motion
    ]
    # ks4 accepts an additional clear_cache parameter
    if "ks4" in pipeline_type:
        spikesorting_parameters.append(spikesorting_clear_cache)
    if pipeline_type != "ecephys_ks25_v0.1.0":
        spikesorting_process = ComputationProcess(
            name=spikesorting_process_name[pipeline_type],
            parameters=[str(p) for p in spikesorting_parameters]
        )
    else:
        spikesorting_process = None

    postprocessing_parameters = [postprocessing_use_motion_corrected]
    postprocessing_process = ComputationProcess(
        name=process_names["postprocessing"],
        parameters=[str(p) for p in postprocessing_parameters]
    )

    collect_results_parameters = [result_suffix]
    collect_results_process = ComputationProcess(
        name=process_names["collect_results"],
        parameters=[str(p) for p in collect_results_parameters]
    )

    processes = [
        job_dispatch_process,
        nwb_backend_process,
        preprocessing_process,
        collect_results_process
    ]

    # For the KS4 pipeline, app panel for postprocessing is disabled for shared-mem issues
    if pipeline_type != "ecephys_ks25_v0.1.0":
        processes.append(postprocessing_process)

    if spikesorting_process is not None:
        processes.append(spikesorting_process)

    job_config.process_config.request.processes = processes


    if alert_bot_url:
        alert_bot = AlertBot(alert_bot_url)
        data_asset_response = co_client.get_data_asset(input_data_asset_id)
        data_asset_json = data_asset_response.json()
        if "name" not in data_asset_json:
            raise RuntimeError(
                "Could not fetch data asset. Maybe Code Ocean credentials are not properly set?"
            )
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
        alert_bot.send_message(f"Starting pipeline {pipeline_type} for {session_name}")
    # run the job
    try:
        codeocean_job.run_job()
        if alert_bot:
            alert_bot.send_message(message=f"Finished pipeline {pipeline_type} for {session_name}")
    except Exception as e:
        if alert_bot:
            alert_bot.send_message(
                message=f"Error with {session_name}", extra_text=str(e)
            )
        raise e


if __name__ == "__main__":
    main()
