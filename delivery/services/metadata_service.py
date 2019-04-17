import csv
import logging

from delivery.exceptions import ChecksumFileNotFoundException, SamplesheetNotFoundException

log = logging.getLogger(__name__)


class MetadataService(object):
    """
    Metadata service, used for reading and writing metadata files associated with the service.
    """

    @staticmethod
    def extract_samplesheet_data(samplesheet_file):

        def _extract_samplesheet_data_section(filehandle):
            return list(csv.DictReader(filehandle))

        try:
            with open(samplesheet_file, "r") as fh:
                while not next(fh).startswith("[Data]"):
                    pass
                return _extract_samplesheet_data_section(fh)
        except IOError as e:
            raise SamplesheetNotFoundException(e)

    @staticmethod
    def parse_checksum_file(checksum_file):
        file_checksums = {}
        try:
            with open(checksum_file) as chksumh:
                for entry in chksumh:
                    checksum, file_path = entry.split()
                    file_checksums[file_path] = checksum
        except IOError as e:
            raise ChecksumFileNotFoundException("Checksum file '{}' could not be opened: {}".format(checksum_file, e))
        return file_checksums

    @staticmethod
    def write_checksum_file(checksum_file, checksums):
        with open(checksum_file, "w") as fh:
            for file_path, checksum in checksums.items():
                fh.write("{}  {}\n".format(checksum, file_path))

    @staticmethod
    def write_samplesheet_file(samplesheet_file, samplesheet_data):
        header = samplesheet_data[0].keys()
        with open(samplesheet_file, "w") as fh:
            fh.write("[Data]\n")
            writer = csv.DictWriter(fh, fieldnames=header)
            writer.writeheader()
            writer.writerows(samplesheet_data)
