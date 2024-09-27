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
    EcephysOptoKS25Job,
    EcephysKS4Job,
    EcephysSC2Job
)

LOG_FMT = "%(asctime)s %(message)s"
LOG_DATE_FMT = "%Y-%m-%d %H:%M"

logging.basicConfig(format=LOG_FMT, datefmt=LOG_DATE_FMT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


valid_job_types = [
    "ecephys_ks25",
    "ecephys_opto_ks25",
    "ecephys_ks4",
    "ecephys_sc2"
]

process_names = dict(
    ecephys_ks25=dict(
        job_dispatch="capsule_aind_ephys_job_dispatch_4",
        nwb_subject="capsule_nwb_packaging_subject_capsule_10",
        preprocessing="capsule_aind_ephys_preprocessing_1",
        spikesorting="capsule_aind_ephys_spikesort_kilosort_25_7",
        postprocessing="capsule_aind_ephys_postprocessing_5",
    ),
    ecephys_opto_ks25=dict(
        job_dispatch="capsule_aind_ephys_job_dispatch_4",
        nwb_subject="capsule_nwb_packaging_subject_capsule_10",
        preprocessing="capsule_opto_preprocess_ecephys_1",
        spikesorting="capsule_aind_ephys_spikesort_kilosort_25_7",
        postprocessing="capsule_aind_ephys_postprocessing_5",
    ),
    ecephys_ks4=dict(
        job_dispatch="capsule_job_dispatch_ecephys_1",
        nwb_subject="capsule_nwb_packaging_subject_capsule_10",
        preprocessing="capsule_preprocess_ecephys_2",
        spikesorting="capsule_spikesort_ecephys_kilosort_4_analyzer_11",
        postprocessing="capsule_postprocess_ecephys_4",
    ),
    ecephys_sc2=dict(
        job_dispatch="capsule_job_dispatch_ecephys_1",
        nwb_subject="capsule_nwb_packaging_subject_capsule_9",
        preprocessing="capsule_preprocess_ecephys_2",
        spikesorting="capsule_spikesort_spyking_circus_2_ecephys_3",
        postprocessing="capsule_postprocess_ecephys_4",
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
    "pipeline_type",
    type=str,
    help=(
        "Pipeline to trigger, either 'ecephys_ks25' or 'ecephys_opto_ks25', 'ecephys_ks4', or 'ecephys_sc2'."
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
    "job_dispatch_concatenate",
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
    "preprocessing_debug",
    type=str,
    help="Whether to run in DEBUG mode",
    nargs="?"
)
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
    "preprocessing_debug_duration",
    type=int,
    help="Duration of clipped recording in debug mode. Default is 30 seconds. Only used if debug is enabled",
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
    "spikesorting_apply_motion",
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
    job_dispatch_concatenate = args.job_dispatch_concatenate
    job_dispatch_split_groups = args.job_dispatch_split_groups
    job_dispatch_input = args.job_dispatch_input
    nwb_backend = args.nwb_backend
    preprocessing_debug = args.preprocessing_debug
    preprocessing_denoising = args.preprocessing_denoising
    preprocessing_filter_type = args.preprocessing_filter_type
    preprocessing_remove_out_channels = args.preprocessing_remove_out_channels
    preprocessing_remove_bad_channels = args.preprocessing_remove_bad_channels
    preprocessing_max_bad_channel_fraction = args.preprocessing_max_bad_channel_fraction
    preprocessing_motion = args.preprocessing_motion
    preprocessing_motion_preset = args.preprocessing_motion_preset
    preprocessing_t_start = args.preprocessing_t_start
    preprocessing_t_stop = args.preprocessing_t_stop
    preprocessing_debug_duration = args.preprocessing_debug_duration
    spikesorting_raise_if_fails = args.spikesorting_raise_if_fails
    spikesorting_apply_motion = args.spikesorting_apply_motion
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
    elif pipeline_type == "ecephys_opto_ks25":
        job_config = EcephysOptoKS25Job()
    elif pipeline_type == "ecephys_ks4":
        job_config = EcephysKS4Job()
    elif pipeline_type == "ecephys_sc2":
        job_config = EcephysSC2Job()
    else:
        logger.error(
            f"""
            Pipeline job_type {pipeline_type} not recognized. 
            Please enter a valid job_type among: {valid_job_types}.
            """
        )
    job_config.process_config.request.data_assets = data_assets

    if result_suffix is not None:
        print(f"Setting result process name to: {result_suffix}")
        job_config.capture_config.process_name = result_suffix

    if output_bucket is not None:
        print(f"Setting output bucket to: {output_bucket}")
        job_config.capture_config.output_bucket = output_bucket

    # Update processes with parameters
    job_dispatch_parameters = [
        job_dispatch_concatenate,
        job_dispatch_split_groups,
        job_dispatch_input
    ]
    job_dispatch_process = ComputationProcess(
        name=process_names[pipeline_type]["job_dispatch"],
        parameters=[str(p) for p in job_dispatch_parameters]
    )

    nwb_subject_parameters = [nwb_backend]
    nwb_subject_process = ComputationProcess(
        name=process_names[pipeline_type]["nwb_subject"],
        parameters=[str(p) for p in nwb_subject_parameters]
    )

    preprocessing_parameters = [
        preprocessing_debug,
        preprocessing_denoising,
        preprocessing_filter_type,
        preprocessing_remove_out_channels,
        preprocessing_remove_bad_channels,
        preprocessing_max_bad_channel_fraction,
        preprocessing_motion,
        preprocessing_motion_preset,
        preprocessing_t_start,
        preprocessing_t_stop,
        preprocessing_debug_duration
    ]
    preprocessing_process = ComputationProcess(
        name=process_names[pipeline_type]["preprocessing"],
        parameters=[str(p) for p in preprocessing_parameters]
    )


    spikesorting_parameters = [
        spikesorting_raise_if_fails,
        spikesorting_apply_motion,
        spikesorting_min_channels_motion
    ]
    # ks4 accepts an additional clear_cache parameter
    if "ks4" in pipeline_type:
        spikesorting_parameters.append(spikesorting_clear_cache)
    spikesorting_process = ComputationProcess(
        name=process_names[pipeline_type]["spikesorting"],
        parameters=[str(p) for p in spikesorting_parameters]
    )

    postprocessing_parameters = [postprocessing_use_motion_corrected]
    postprocessing_process = ComputationProcess(
        name=process_names[pipeline_type]["postprocessing"],
        parameters=[str(p) for p in postprocessing_parameters]
    )

    processes = [
        job_dispatch_process,
        nwb_subject_process,
        preprocessing_process,
        postprocessing_process
    ]

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
