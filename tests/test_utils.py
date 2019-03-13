
import csv
import os
import time
from collections import OrderedDict

from mock import MagicMock

from delivery.models.project import RunfolderProject
from delivery.models.runfolder import Runfolder
from delivery.models.sample import SampleFile, Sample


class MockIOLoop():

    def __init__(self):
        pass

    def spawn_callback(self, f, **args):
        f(**args)

class TestUtils:
    DUMMY_CONFIG = {"monitored_directory": "/foo"}


class DummyConfig:

    def __getitem__(self, key):
        return TestUtils.DUMMY_CONFIG[key]

fake_directories = ["160930_ST-E00216_0111_BH37CWALXX",
                    "160930_ST-E00216_0112_BH37CWALXX"]
fake_projects = ["ABC_123", "DEF_456"]


def mock_file_system_service(directories, projects, fastq_files=None):
    mock_file_system_service_instance = MagicMock()
    mock_file_system_service_instance.find_runfolder_directories.return_value = directories
    mock_file_system_service_instance.find_project_directories.return_value = projects
    mock_file_system_service_instance.list_files_recursively.return_value = fastq_files or []
    return mock_file_system_service_instance


def _item_generator(prefix=None, suffix=None):
    nxt = 1
    while True:
        yield "".join([prefix or "", str(nxt), suffix or ""])
        nxt += 1


def sample_name_generator():
    yield from _item_generator(prefix="Sample_")


def sample_index_generator():
    yield from _item_generator(prefix="S")


def lane_generator():
    yield from _item_generator()


def project_sample(project, sample_name, sample_index, lane_no, subdir=False):
    sample_files = []
    if subdir:
        sample_dir = os.path.join(project.path, sample_name)
    else:
        sample_dir = project.path
    for is_index in [False, True]:
        for read_no in [1, 2]:
            sample_path = os.path.join(
                sample_dir,
                "{}_{}_L00{}_{}{}_001.fastq.gz".format(
                    sample_name,
                    sample_index,
                    str(lane_no),
                    "I" if is_index else "R",
                    str(read_no)))
            sample_files.append(
                SampleFile(
                    sample_path=sample_path,
                    sample_name=sample_name,
                    sample_index=sample_index,
                    lane_no=lane_no,
                    read_no=read_no,
                    is_index=is_index,
                    checksum="checksum-for-{}".format(sample_path)))
    return Sample(
        name=sample_name,
        project_name=project.name,
        sample_files=sample_files
    )


def runfolder_project(
        runfolder,
        project_name="ABC_123",
        sample_indexes=sample_index_generator(),
        lane_numbers=lane_generator(),
        project_root="Unaligned"):
    project = RunfolderProject(
        name=project_name,
        path=os.path.join(runfolder.path, project_root, project_name),
        runfolder_path=runfolder.path,
        runfolder_name=runfolder.name
    )

    sample_names = sample_name_generator()

    # a straight-forward sample with files on one lane
    lane_number = next(lane_numbers)
    samples = [project_sample(project, next(sample_names), next(sample_indexes), lane_number)]

    # a sample with files on two lanes
    sample_name = next(sample_names)
    sample_index = next(sample_indexes)
    sample = project_sample(project, sample_name, sample_index, lane_number)
    lane_number = next(lane_numbers)
    t_sample = project_sample(project, sample_name, sample_index, lane_number)
    sample.sample_files.extend(t_sample.sample_files)
    samples.append(sample)

    # a sample with two preps on two lanes and sample files in subdirectories
    t_samples = [
        project_sample(
            project,
            sample_name="Sample_3",
            sample_index=si,
            lane_no=l,
            subdir=True)
        for si in [next(sample_indexes), next(sample_indexes)] for l in [next(lane_numbers), next(lane_numbers)]]
    sample = t_samples[0]
    sample.sample_files = [f for s in t_samples for f in s.sample_files]
    samples.append(sample)

    project.samples = samples
    return project


def unorganised_runfolder(name="180124_A00181_0019_BH72M5DMXX", root_path="/foo"):
    sample_indexes = sample_index_generator()
    lane_numbers = lane_generator()
    runfolder = Runfolder(name=name, path=os.path.join(root_path, name))
    runfolder.projects = [
        runfolder_project(
            runfolder,
            project_name=p,
            sample_indexes=sample_indexes,
            lane_numbers=lane_numbers) for p in fake_projects]
    checksums = {}
    for project in runfolder.projects:
        for sample in project.samples:
            for sample_file in sample.sample_files:
                checksums[sample_file.checksum] = os.path.relpath(
                    sample_file.sample_path,
                    os.path.dirname(runfolder.path))
    runfolder.checksums = checksums
    return runfolder


def samplesheet_data_for_runfolder(runfolder):
    samplesheet_data_headers = [
        "Lane",
        "Sample_ID",
        "Sample_Name",
        "Sample_Plate",
        "Sample_Well",
        "index",
        "Sample_Project",
        "Description"
    ]
    samplesheet_data = []
    for project in runfolder.projects:
        for sample in project.samples:
            for sample_file in sample.sample_files:
                if sample_file.read_no == 1 and not sample_file.is_index:
                    samplesheet_data.append(
                        OrderedDict(zip(
                            samplesheet_data_headers,
                            [
                                str(sample_file.lane_no),
                                sample_file.sample_id,
                                sample_file.sample_name,
                                str(),
                                str(),
                                "index_seq_{}".format(sample_file.sample_index),
                                project.name,
                                "PROJECT:{};SAMPLE:{};LANE:{};INDEX:{}".format(
                                    project.name,
                                    sample.name,
                                    str(sample_file.lane_no),
                                    sample_file.sample_index)])))
    return samplesheet_data


def samplesheet_file_from_runfolder(runfolder):
    header_stuff = """[Header],,,,,,,,
IEMFileVersion,4,,,,,,,
Experiment Name,Hiseq-2500-single-index,,,,,,,
Date,02/26/2019,,,,,,,
Workflow,Resequencing,,,,,,,
Application,Human Genome Resequencing,,,,,,,
Assay,TruSeq LT,,,,,,,
Description,,,,,,,,
Chemistry,Default,,,,,,,
,,,,,,,,
[Reads],,,,,,,,
50,,,,,,,,
50,,,,,,,,
,,,,,,,,
[Settings],,,,,,,,
FlagPCRDuplicates,1,,,,,,,
Adapter,,,,,,,,
AdapterRead2,,,,,,,,
,,,,,,,,
[Data],,,,,,,,
"""
    samplesheet_data = samplesheet_data_for_runfolder(runfolder)
    samplesheet_file = os.path.join(runfolder.path, "SampleSheet.csv")
    with open(samplesheet_file, "w") as fh:
        fh.write(header_stuff)
        writer = csv.DictWriter(fh, fieldnames=samplesheet_data[0].keys())
        writer.writeheader()
        writer.writerows(samplesheet_data)
    return samplesheet_file, samplesheet_data


_runfolder1 = Runfolder(name="160930_ST-E00216_0111_BH37CWALXX",
                        path="/foo/160930_ST-E00216_0111_BH37CWALXX")

_runfolder1.projects = [RunfolderProject(name="ABC_123",
                                         path="/foo/160930_ST-E00216_0111_BH37CWALXX/Projects/ABC_123",
                                         runfolder_path=_runfolder1.path,
                                         runfolder_name="160930_ST-E00216_0111_BH37CWALXX"),
                        RunfolderProject(name="DEF_456",
                                         path="/foo/160930_ST-E00216_0111_BH37CWALXX/Projects/DEF_456",
                                         runfolder_path=_runfolder1.path,
                                         runfolder_name="160930_ST-E00216_0111_BH37CWALXX")]

_runfolder2 = Runfolder(name="160930_ST-E00216_0112_BH37CWALXX",
                        path="/foo/160930_ST-E00216_0112_BH37CWALXX")

_runfolder2.projects = [RunfolderProject(name="ABC_123",
                                         path="/foo/160930_ST-E00216_0112_BH37CWALXX/Projects/ABC_123",
                                         runfolder_path=_runfolder2.path,
                                         runfolder_name="160930_ST-E00216_0112_BH37CWALXX"),
                        RunfolderProject(name="DEF_456",
                                         path="/foo/160930_ST-E00216_0112_BH37CWALXX/Projects/DEF_456",
                                         runfolder_path=_runfolder2.path,
                                         runfolder_name="160930_ST-E00216_0112_BH37CWALXX")]

FAKE_RUNFOLDERS = [_runfolder1, _runfolder2]
UNORGANISED_RUNFOLDER = unorganised_runfolder()


def assert_eventually_equals(self, timeout, f, expected, delay=0.1):
    start_time = time.time()

    while True:
        try:
            value = f()
            self.assertEquals(value, expected)
            break
        except AssertionError:
            if time.time() - start_time <= timeout:
                time.sleep(delay)
                continue
            else:
                raise
