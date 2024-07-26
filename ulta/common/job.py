from dataclasses import dataclass, field
from enum import IntEnum
from strenum import StrEnum
from functools import cached_property

from ulta.common.job_status import AdditionalJobStatus, JobStatus
from ulta.common.ammo import Ammo


class JobPluginType(StrEnum):
    TELEGRAF = 'yandextank.plugins.Telegraf'
    AUTOSTOP = 'yandextank.plugins.Autostop'
    UPLOADER = 'yandextank.plugins.DataUploader'
    PHANTOM = 'yandextank.plugins.Phantom'
    PANDORA = 'yandextank.plugins.Pandora'
    JMETER = 'yandextank.plugins.JMeter'
    RESOURCE_CHECK = 'yandextank.plugins.ResourceCheck'


class Generator(IntEnum):
    UNKNOWN = 0
    PHANTOM = 1
    PANDORA = 2
    JMETER = 3


@dataclass
class ArtifactSettings:
    output_bucket: str
    output_name: str
    is_archive: bool
    filter_include: list[str]
    filter_exclude: list[str]


@dataclass
class JobResult:
    status: str
    exit_code: int = 0


@dataclass
class Job:
    id: str
    ammos: list[Ammo] = field(default_factory=list)
    log_group_id: str | None = None
    tank_job_id: str | None = None
    config: dict | None = None
    test_data_dir: str | None = None
    upload_artifact_settings: ArtifactSettings | None = None
    artifact_dir_path: str | None = None
    last_status: JobStatus = field(
        default_factory=lambda: JobStatus.from_status(AdditionalJobStatus.JOB_STATUS_UNSPECIFIED)
    )

    def __repr__(self):
        return f'Test(id={self.id}, internal_id={self.tank_job_id}, log_group_id={self.log_group_id})'

    @cached_property
    def generator(self) -> Generator:
        if self.plugin_enabled(JobPluginType.PANDORA):
            return Generator.PANDORA
        elif self.plugin_enabled(JobPluginType.PHANTOM):
            return Generator.PHANTOM
        elif self.plugin_enabled(JobPluginType.JMETER):
            return Generator.JMETER
        return Generator.UNKNOWN

    def plugin_enabled(self, plugin_type: JobPluginType) -> bool:
        return any(self.get_plugins(plugin_type))

    def get_plugins(self, plugin_type: JobPluginType) -> list[tuple[str, dict]]:
        return [
            (key, plugin)
            for key, plugin in self.config.items()
            if plugin.get('package') == plugin_type and plugin.get('enabled')
        ]

    @property
    def status(self) -> JobStatus:
        return self.last_status

    def update_status(self, status: JobStatus):
        assert isinstance(status, JobStatus)
        self.last_status = status

    def finished(self) -> bool:
        return self.last_status is not None and self.last_status.finished()

    def result(self) -> JobResult:
        return JobResult(
            status=self.last_status.status,
            exit_code=self.last_status.exit_code,
        )
