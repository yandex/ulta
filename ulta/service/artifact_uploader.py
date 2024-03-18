import logging
import os
from pathlib import Path
from typing import Generator, Iterable, Protocol
from zipfile import ZipFile, ZIP_DEFLATED

from ulta.common.cancellation import Cancellation, CancellationRequest, CancellationType
from ulta.common.exceptions import ObjectStorageError, ArtifactUploadError
from ulta.common.job import ArtifactSettings, Job
from ulta.common.interfaces import JobStatusClient, S3Client

ROOT_SEGMENT = '__root'


class ArtifactUploader(Protocol):
    def publish_artifacts(self, job: Job) -> None:
        ...


class S3ArtifactUploader(ArtifactUploader):
    def __init__(
        self,
        loadtesting_client: JobStatusClient,
        s3_client: S3Client,
        cancellation: Cancellation,
        logger: logging.Logger,
    ):
        self.s3_client = s3_client
        self.loadtesting_client = loadtesting_client
        self.cancellation = cancellation
        self.logger = logger

    def publish_artifacts(self, job: Job):
        if not job.upload_artifact_settings:
            self.logger.info('Artifact settings not provided. Nothing to upload.')
            return

        if not job or not job.artifact_dir_path:
            self.logger.info('Job has no artifacts. Nothing to upload.')
            return

        try:
            self.cancellation.raise_on_set(CancellationType.FORCED)
            artifacts = self.collect_artifacts(job.upload_artifact_settings, job.artifact_dir_path)
            self._upload_artifacts(artifacts, job.upload_artifact_settings.output_bucket)
        except (ArtifactUploadError, CancellationRequest):
            raise
        except Exception as e:
            raise ArtifactUploadError(str(e)) from e

    def _upload_artifacts(self, artifacts: Iterable[tuple[str, str]], output_bucket: str):
        # TODO: make async awaiter
        errors: list[Exception] = []
        for local_path, s3_filename in artifacts:
            self.cancellation.raise_on_set(CancellationType.FORCED)
            try:
                self.s3_client.upload(
                    source_file=local_path,
                    s3_filename=s3_filename,
                    s3_bucket=output_bucket,
                )
            except ObjectStorageError as e:
                errors.append(e)
                self.logger.exception('Failed to publish artifact %s to %s: %s', local_path, s3_filename, str(e))
        if errors:
            msg = '\n'.join([str(e) for e in errors])
            raise ArtifactUploadError(f'Failed to upload one or more artifacts to s3: {msg}')

    def _filter_readable(self, paths: Iterable[Path]) -> Generator[Path, None, None]:
        for path in paths:
            if os.access(path, os.R_OK):
                yield path
            else:
                self.logger.error('File %s is not readable', path)

    def _collect_files(
        self, path: Path, filter_include: list[str], filter_exclude: list[str]
    ) -> Generator[Path, None, None]:
        if not path.is_dir():
            path = path.parent

        files = set()
        for filter in filter_include:
            files |= {p.resolve() for p in path.rglob(filter) if p.is_file()}
        for filter in filter_exclude:
            files -= {p.resolve() for p in path.rglob(filter) if p.is_file()}
        return self._filter_readable(files)

    def collect_artifacts(self, settings: ArtifactSettings, path: str | Path) -> Generator[tuple[str, str], None, None]:
        root = path if isinstance(path, Path) else Path(path)
        root = root.resolve()
        collected_files = self._collect_files(root, settings.filter_include, settings.filter_exclude)

        if settings.is_archive:
            archive_name = settings.output_name + '.zip'
            archive_path = root.joinpath(os.path.split(archive_name)[-1])
            with ZipFile(archive_path, 'w', compression=ZIP_DEFLATED) as archive:
                for f in collected_files:
                    archive.write(f, arcname=_relative_to(f, root))
            yield str(archive_path), archive_name
        else:
            for f in collected_files:
                yield str(f), settings.output_name + '/' + _relative_to(f, root)


def _relative_to(path: Path | str, root: Path | str) -> str:
    if os.path.commonpath((root, path)) == str(root):
        return os.path.relpath(path, root)
    else:
        relpath = os.path.splitdrive(path)[1]
        while relpath[0] in (os.sep, os.altsep):
            relpath = relpath[1:]
        return os.path.join(ROOT_SEGMENT, relpath)
