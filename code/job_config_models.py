"""
Pydantic models for job configuration
"""
import json
from pathlib import Path
from typing import Optional

from pydantic import BaseModel

job_config_name_mapping = {
    "ecephys": "ecephys_ks25",
    "ecephys_opto": "ecephys_ks25_opto",
    "ecephys_ks25_v0_1_0": "ecephys_ks25_v0.1.0",
    "ecephys_ks25_v0.1.0": "ecephys_ks25_v0.1.0",
    "ecephys_legacy": "ecephys_ks25_v0.1.0",
}

class ProcessNames(BaseModel):
    """Process names for different pipeline stages"""
    job_dispatch: str
    nwb_subject: Optional[str] = None
    nwb_ecephys: Optional[str] = None
    preprocessing: str
    spikesorting: str
    postprocessing: str
    collect_results: str


class JobConfig(BaseModel):
    """Configuration for a single job type"""
    name: str 
    """Key used in job configs json file"""
    pipeline_id: str
    input_data_mount: str
    captured_asset_label: str
    version: Optional[int] = None
    process_names: ProcessNames

    
def get_job_config(job_name: str, json_path: str = "job_configs.json") -> JobConfig:
    """
    Get configuration for a specific job type
    
    Args:
        job_name: Name of the job type (e.g., 'ecephys_ks25'). A mapping is applied to support
        alternative names.
        
        json_path: Path to job configs .json file. If None, uses default location.
        
    Returns:
        JobConfig object for the specified job type.
        
    Raises:
        KeyError: If the job type is not found in the configurations file.
    """
    configs = json.loads(Path(json_path).read_text())
    job_name = job_config_name_mapping.get(job_name, job_name)
    if job_name in configs:
        return JobConfig(name=job_name, **configs[job_name])
    raise KeyError(f"Job {job_name!r} not found in {json_path}. Available jobs: {list(configs.keys())}")