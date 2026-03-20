#!/usr/bin/env python3
"""Extracao em lote do ERA5-Land com checkpoint e retomada automatica."""

from __future__ import annotations

import argparse
import calendar
import json
import os
import shlex
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

DATASET = "reanalysis-era5-land"
DEFAULT_START_YEAR = 2020
DEFAULT_END_YEAR = 2024
DEFAULT_CHUNK_DAYS = 31
DEFAULT_RETRY_DELAY_SECONDS = 30
DEFAULT_MAX_RETRIES = 3
DEFAULT_VARIABLE_SET = "core_local_climate"
DEFAULT_REQUEST_OFFSET_SECONDS = 30
DEFAULT_MAX_PARALLEL_REQUESTS = 10

CORE_LOCAL_CLIMATE_VARIABLES = [
    "2m_dewpoint_temperature",
    "2m_temperature",
    "10m_u_component_of_wind",
    "10m_v_component_of_wind",
    "surface_solar_radiation_downwards",
]

SURFACE_PLUS_OBSERVED_GAPS_VARIABLES = [
    *CORE_LOCAL_CLIMATE_VARIABLES,
    "surface_pressure",
    "total_precipitation",
]

LEGACY_FULL_CATALOG_VARIABLES = [
    "2m_dewpoint_temperature",
    "2m_temperature",
    "skin_temperature",
    "soil_temperature_level_1",
    "soil_temperature_level_2",
    "soil_temperature_level_3",
    "soil_temperature_level_4",
    "lake_bottom_temperature",
    "lake_ice_depth",
    "lake_ice_temperature",
    "lake_mix_layer_depth",
    "lake_mix_layer_temperature",
    "lake_shape_factor",
    "lake_total_layer_temperature",
    "snow_albedo",
    "snow_cover",
    "snow_density",
    "snow_depth",
    "snow_depth_water_equivalent",
    "snowfall",
    "snowmelt",
    "temperature_of_snow_layer",
    "skin_reservoir_content",
    "volumetric_soil_water_layer_1",
    "volumetric_soil_water_layer_2",
    "volumetric_soil_water_layer_3",
    "volumetric_soil_water_layer_4",
    "forecast_albedo",
    "surface_latent_heat_flux",
    "surface_net_solar_radiation",
    "surface_net_thermal_radiation",
    "surface_sensible_heat_flux",
    "surface_solar_radiation_downwards",
    "surface_thermal_radiation_downwards",
    "evaporation_from_bare_soil",
    "evaporation_from_open_water_surfaces_excluding_oceans",
    "evaporation_from_the_top_of_canopy",
    "evaporation_from_vegetation_transpiration",
    "potential_evaporation",
    "runoff",
    "snow_evaporation",
    "sub_surface_runoff",
    "surface_runoff",
    "total_evaporation",
    "10m_u_component_of_wind",
    "10m_v_component_of_wind",
    "surface_pressure",
    "total_precipitation",
    "leaf_area_index_high_vegetation",
    "leaf_area_index_low_vegetation",
    "high_vegetation_cover",
    "glacier_mask",
    "lake_cover",
    "low_vegetation_cover",
    "lake_total_depth",
    "geopotential",
    "land_sea_mask",
    "soil_type",
    "type_of_high_vegetation",
    "type_of_low_vegetation",
]

VARIABLE_SETS = {
    "core_local_climate": CORE_LOCAL_CLIMATE_VARIABLES,
    "surface_plus_observed_gaps": SURFACE_PLUS_OBSERVED_GAPS_VARIABLES,
    "legacy_full_catalog": LEGACY_FULL_CATALOG_VARIABLES,
}

VARIABLE_SET_DESCRIPTIONS = {
    "core_local_climate": (
        "Conjunto minimo para clima local de superficie com apoio de IAG + INMET. "
        "Mantem somente temperatura, umidade termodinamica, vento vetorial e radiacao."
    ),
    "surface_plus_observed_gaps": (
        "Extende o conjunto minimo com pressao e precipitacao para cenarios de "
        "gap filling ou estudos mais dependentes dessas duas series."
    ),
    "legacy_full_catalog": (
        "Replica a lista historica completa do script, incluindo variaveis de lago, "
        "neve, solo, vegetacao e mascaras estaticas."
    ),
}

TIMES = [f"{hour:02d}:00" for hour in range(24)]


@dataclass(frozen=True)
class ChunkJob:
    year: int
    month: int
    day_start: int
    day_end: int

    @property
    def key(self) -> str:
        return (
            f"{self.year:04d}-{self.month:02d}-"
            f"{self.day_start:02d}-{self.day_end:02d}"
        )

    @property
    def month_dir(self) -> str:
        return f"{self.month:02d}"

    @property
    def filename(self) -> str:
        return (
            f"era5_land_{self.year:04d}_{self.month:02d}_"
            f"{self.day_start:02d}-{self.day_end:02d}.zip"
        )

    @property
    def days(self) -> list[str]:
        return [f"{day:02d}" for day in range(self.day_start, self.day_end + 1)]

    def output_path(self, output_root: Path) -> Path:
        return output_root / f"{self.year:04d}" / self.month_dir / self.filename

    def partial_path(self, output_root: Path) -> Path:
        return self.output_path(output_root).with_name(f"{self.filename}.part")

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "year": self.year,
            "month": self.month,
            "day_start": self.day_start,
            "day_end": self.day_end,
            "days": self.days,
            "filename": self.filename,
        }

    def build_request(self, variables: list[str]) -> dict[str, Any]:
        return {
            "variable": variables,
            "year": f"{self.year:04d}",
            "month": f"{self.month:02d}",
            "day": self.days,
            "time": TIMES,
            "data_format": "netcdf",
            "download_format": "zip",
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Baixa dados do ERA5-Land em lotes, organiza por ano/mes "
            "e retoma automaticamente de onde parou."
        )
    )
    parser.add_argument("--start-year", type=int, default=DEFAULT_START_YEAR)
    parser.add_argument("--end-year", type=int, default=DEFAULT_END_YEAR)
    parser.add_argument("--chunk-days", type=int, default=DEFAULT_CHUNK_DAYS)
    parser.add_argument(
        "--variable-set",
        choices=sorted(VARIABLE_SETS),
        default=DEFAULT_VARIABLE_SET,
        help=(
            "Preset de variaveis do ERA5-Land. "
            f"O padrao e '{DEFAULT_VARIABLE_SET}'."
        ),
    )
    parser.add_argument(
        "--list-variable-sets",
        action="store_true",
        help="Lista os presets de variaveis disponiveis e encerra.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data") / "era5-land",
        help="Diretorio raiz onde os zips serao gravados.",
    )
    parser.add_argument(
        "--state-file",
        type=Path,
        default=None,
        help="Arquivo JSON com o estado do processo.",
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        default=None,
        help="Arquivo JSONL append-only com o historico da extracao.",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=DEFAULT_MAX_RETRIES,
        help="Numero de tentativas adicionais apos a primeira falha.",
    )
    parser.add_argument(
        "--retry-delay-seconds",
        type=int,
        default=DEFAULT_RETRY_DELAY_SECONDS,
        help="Espera entre tentativas em caso de falha.",
    )
    parser.add_argument(
        "--sleep-between-requests",
        type=int,
        default=0,
        help="Pausa opcional entre downloads concluidos.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Refaz downloads mesmo se o zip final ja existir.",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continua para o proximo lote quando esgotar retries.",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Mostra o status atual sem chamar a API.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Mostra o primeiro lote pendente e encerra sem chamar a API.",
    )
    parser.add_argument(
        "--request-offset-seconds",
        type=int,
        default=DEFAULT_REQUEST_OFFSET_SECONDS,
        help=(
            "Intervalo minimo entre o disparo de novos workers. "
            f"O padrao e {DEFAULT_REQUEST_OFFSET_SECONDS}s."
        ),
    )
    parser.add_argument(
        "--max-parallel-requests",
        type=int,
        default=DEFAULT_MAX_PARALLEL_REQUESTS,
        help=(
            "Numero maximo de workers simultaneos. "
            f"O padrao e {DEFAULT_MAX_PARALLEL_REQUESTS}."
        ),
    )
    parser.add_argument(
        "--terminal-command",
        default="auto",
        help=(
            "Comando para abrir um terminal por worker. "
            "Use 'auto' para autodetectar, 'none' para rodar sem terminal, "
            "ou um template com {command}."
        ),
    )
    parser.add_argument(
        "--worker",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--worker-job",
        nargs=4,
        type=int,
        metavar=("YEAR", "MONTH", "DAY_START", "DAY_END"),
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help=argparse.SUPPRESS,
    )
    args = parser.parse_args()

    if args.start_year > args.end_year:
        parser.error("--start-year nao pode ser maior que --end-year.")
    if args.chunk_days < 1:
        parser.error("--chunk-days precisa ser maior que zero.")
    if args.max_retries < 0:
        parser.error("--max-retries nao pode ser negativo.")
    if args.retry_delay_seconds < 0:
        parser.error("--retry-delay-seconds nao pode ser negativo.")
    if args.sleep_between_requests < 0:
        parser.error("--sleep-between-requests nao pode ser negativo.")
    if args.request_offset_seconds < 0:
        parser.error("--request-offset-seconds nao pode ser negativo.")
    if args.max_parallel_requests < 1:
        parser.error("--max-parallel-requests precisa ser maior que zero.")
    if args.worker and args.worker_job is None:
        parser.error("--worker exige --worker-job YEAR MONTH DAY_START DAY_END.")

    return args


def utc_now() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def resolve_state_paths(
    output_dir: Path,
    state_file: Path | None,
    log_file: Path | None,
) -> tuple[Path, Path]:
    meta_dir = output_dir / "_meta"
    return (
        state_file or meta_dir / "state.json",
        log_file or meta_dir / "history.jsonl",
    )


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def meta_dir(output_dir: Path) -> Path:
    return output_dir / "_meta"


def claim_path(output_dir: Path, job: ChunkJob) -> Path:
    return meta_dir(output_dir) / "claims" / f"{job.key}.json"


def result_path(output_dir: Path, job: ChunkJob) -> Path:
    return meta_dir(output_dir) / "results" / f"{job.key}.json"


def write_json(path: Path, payload: dict[str, Any]) -> None:
    ensure_parent(path)
    tmp_path = path.with_name(f"{path.name}.tmp")
    tmp_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=True, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    tmp_path.replace(path)


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    ensure_parent(path)
    with path.open("a", encoding="utf-8") as file_handle:
        file_handle.write(json.dumps(payload, ensure_ascii=True, sort_keys=True))
        file_handle.write("\n")


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def is_pid_running(pid: int | None) -> bool:
    if pid is None or pid <= 0:
        return False

    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def build_claim_payload(job: ChunkJob, *, status: str, pid: int) -> dict[str, Any]:
    return {
        "job": job.to_dict(),
        "pid": pid,
        "status": status,
        "updated_at": utc_now(),
        "argv": sys.argv,
    }


def claim_is_active(payload: dict[str, Any]) -> bool:
    return payload.get("status") in {"launching", "running"} and is_pid_running(
        payload.get("pid")
    )


def remove_claim_if_stale(output_dir: Path, job: ChunkJob) -> bool:
    path = claim_path(output_dir, job)
    payload = load_json(path)
    if not payload:
        return False
    if claim_is_active(payload):
        return False
    if job.output_path(output_dir).exists():
        return False
    path.unlink(missing_ok=True)
    return True


def reserve_job_claim(output_dir: Path, job: ChunkJob) -> bool:
    path = claim_path(output_dir, job)
    ensure_parent(path)

    while True:
        if job.output_path(output_dir).exists():
            return False

        try:
            with path.open("x", encoding="utf-8") as file_handle:
                json.dump(
                    build_claim_payload(job, status="launching", pid=os.getpid()),
                    file_handle,
                    ensure_ascii=True,
                    sort_keys=True,
                )
                file_handle.write("\n")
            return True
        except FileExistsError:
            if not remove_claim_if_stale(output_dir, job):
                return False


def acquire_worker_claim(output_dir: Path, job: ChunkJob) -> bool:
    path = claim_path(output_dir, job)
    ensure_parent(path)
    payload = build_claim_payload(job, status="running", pid=os.getpid())

    existing_payload = load_json(path)
    if existing_payload:
        if claim_is_active(existing_payload):
            existing_pid = existing_payload.get("pid")
            if existing_pid != os.getpid() and existing_payload.get("status") == "running":
                return False
        elif not job.output_path(output_dir).exists():
            path.unlink(missing_ok=True)

    write_json(path, payload)
    return True


def release_job_claim(output_dir: Path, job: ChunkJob) -> None:
    claim_path(output_dir, job).unlink(missing_ok=True)


def write_result(
    output_dir: Path,
    job: ChunkJob,
    *,
    run_id: str | None,
    status: str,
    message: str | None = None,
) -> None:
    payload = {
        "job": job.to_dict(),
        "pid": os.getpid(),
        "run_id": run_id,
        "status": status,
        "updated_at": utc_now(),
    }
    if message:
        payload["message"] = message
    write_json(result_path(output_dir, job), payload)


def iter_jobs(start_year: int, end_year: int, chunk_days: int) -> Iterable[ChunkJob]:
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            total_days = calendar.monthrange(year, month)[1]
            for day_start in range(1, total_days + 1, chunk_days):
                day_end = min(day_start + chunk_days - 1, total_days)
                yield ChunkJob(year=year, month=month, day_start=day_start, day_end=day_end)


def load_log_tail(log_path: Path, limit: int = 5) -> list[dict[str, Any]]:
    if not log_path.exists():
        return []

    lines = log_path.read_text(encoding="utf-8").splitlines()
    tail = []
    for line in lines[-limit:]:
        if not line.strip():
            continue
        try:
            tail.append(json.loads(line))
        except json.JSONDecodeError:
            tail.append({"raw": line, "decode_error": True})
    return tail


def build_state(
    *,
    args: argparse.Namespace,
    variables: list[str],
    output_dir: Path,
    log_path: Path,
    run_id: str | None,
    total_jobs: int,
    completed_jobs: int,
    active_jobs: list[ChunkJob] | None = None,
    failed_jobs: list[ChunkJob] | None = None,
    last_started: ChunkJob | None = None,
    last_completed: ChunkJob | None = None,
    last_error: dict[str, Any] | None = None,
    status: str,
) -> dict[str, Any]:
    active_jobs = active_jobs or []
    failed_jobs = failed_jobs or []
    return {
        "dataset": DATASET,
        "run_id": run_id,
        "updated_at": utc_now(),
        "status": status,
        "range": {
            "start_year": args.start_year,
            "end_year": args.end_year,
            "chunk_days": args.chunk_days,
        },
        "parallelism": {
            "max_parallel_requests": args.max_parallel_requests,
            "request_offset_seconds": args.request_offset_seconds,
            "terminal_command": args.terminal_command,
        },
        "variable_set": args.variable_set,
        "variables": variables,
        "output_dir": str(output_dir),
        "log_file": str(log_path),
        "progress": {
            "completed_jobs": completed_jobs,
            "total_jobs": total_jobs,
            "active_jobs": len(active_jobs),
            "failed_jobs": len(failed_jobs),
            "pending_jobs": total_jobs - completed_jobs - len(active_jobs) - len(failed_jobs),
        },
        "active_jobs": [job.to_dict() for job in active_jobs],
        "failed_jobs": [job.to_dict() for job in failed_jobs],
        "last_started": last_started.to_dict() if last_started else None,
        "last_completed": last_completed.to_dict() if last_completed else None,
        "last_error": last_error,
    }


def load_run_failures(
    jobs: list[ChunkJob],
    output_dir: Path,
    run_id: str | None,
) -> dict[str, dict[str, Any]]:
    if not run_id:
        return {}

    failures: dict[str, dict[str, Any]] = {}
    for job in jobs:
        payload = load_json(result_path(output_dir, job))
        if (
            payload.get("run_id") == run_id
            and payload.get("status") in {"failed", "interrupted"}
        ):
            failures[job.key] = payload
    return failures


def summarize_jobs(
    jobs: list[ChunkJob],
    output_dir: Path,
    *,
    overwrite: bool,
    run_id: str | None = None,
) -> tuple[int, list[ChunkJob], list[ChunkJob], ChunkJob | None]:
    completed = 0
    active_jobs: list[ChunkJob] = []
    failed_jobs: list[ChunkJob] = []
    next_pending = None
    failures = load_run_failures(jobs, output_dir, run_id)

    for job in jobs:
        output_exists = job.output_path(output_dir).exists()
        job_result = load_json(result_path(output_dir, job))
        if (
            overwrite
            and run_id
            and job_result.get("run_id") == run_id
            and job_result.get("status") == "success"
        ):
            completed += 1
            continue

        if output_exists and not overwrite:
            completed += 1
            continue

        claim_payload = load_json(claim_path(output_dir, job))
        if claim_payload and claim_is_active(claim_payload):
            active_jobs.append(job)
            continue
        if claim_payload:
            remove_claim_if_stale(output_dir, job)

        if job.key in failures:
            failed_jobs.append(job)
            continue

        if next_pending is None:
            next_pending = job

    return completed, active_jobs, failed_jobs, next_pending


def print_status(
    *,
    args: argparse.Namespace,
    variables: list[str],
    output_dir: Path,
    state_path: Path,
    log_path: Path,
    jobs: list[ChunkJob],
) -> None:
    state = load_json(state_path)
    completed_jobs, active_jobs, failed_jobs, next_pending = summarize_jobs(
        jobs,
        output_dir,
        overwrite=args.overwrite,
        run_id=state.get("run_id"),
    )
    recent_events = load_log_tail(log_path)

    print(f"Dataset: {DATASET}")
    print(f"Periodo: {args.start_year}..{args.end_year}")
    print(f"Lote de dias: {args.chunk_days}")
    print(f"Preset de variaveis: {args.variable_set}")
    print(f"Total de variaveis: {len(variables)}")
    print(f"Diretorio de saida: {output_dir}")
    print(f"Arquivo de estado: {state_path}")
    print(f"Arquivo de log: {log_path}")
    print(f"Concluidos: {completed_jobs}/{len(jobs)}")
    print(f"Ativos: {len(active_jobs)}")
    print(f"Falhos no run atual: {len(failed_jobs)}")
    print(f"Pendentes: {len(jobs) - completed_jobs - len(active_jobs) - len(failed_jobs)}")

    if state:
        print(f"Ultimo status salvo: {state.get('status', 'desconhecido')}")
        if state.get("last_completed"):
            print(f"Ultimo lote concluido: {state['last_completed']['key']}")
        if state.get("last_error"):
            last_error = state["last_error"]
            print(
                "Ultimo erro: "
                f"{last_error.get('job_key', 'desconhecido')} "
                f"({last_error.get('error_type', 'Erro')}): "
                f"{last_error.get('message', '')}"
            )
    else:
        print("Estado salvo: inexistente")

    if next_pending:
        print(f"Proximo lote pendente: {next_pending.key}")
        print(f"Destino do proximo zip: {next_pending.output_path(output_dir)}")
    else:
        print("Todos os lotes do periodo solicitado ja estao baixados.")

    if recent_events:
        print("Ultimos eventos:")
        for event in recent_events:
            timestamp = event.get("timestamp", "sem_timestamp")
            event_name = event.get("event", "evento_desconhecido")
            job_key = event.get("job", {}).get("key", "-")
            print(f"  - {timestamp} | {event_name} | {job_key}")


def create_client():
    try:
        import cdsapi
    except ModuleNotFoundError as exc:
        raise SystemExit(
            "Pacote 'cdsapi' nao encontrado. Instale a dependencia antes de iniciar a extracao."
        ) from exc

    return cdsapi.Client()


def log_event(
    log_path: Path,
    event: str,
    job: ChunkJob,
    **extra: Any,
) -> None:
    payload = {
        "timestamp": utc_now(),
        "event": event,
        "dataset": DATASET,
        "job": job.to_dict(),
    }
    payload.update(extra)
    append_jsonl(log_path, payload)


def download_job(
    *,
    client: Any,
    job: ChunkJob,
    variables: list[str],
    output_dir: Path,
    log_path: Path,
    max_retries: int,
    retry_delay_seconds: int,
) -> Path:
    target_path = job.output_path(output_dir)
    partial_path = job.partial_path(output_dir)
    ensure_parent(target_path)

    if partial_path.exists():
        partial_path.unlink()

    total_attempts = max_retries + 1

    for attempt in range(1, total_attempts + 1):
        log_event(
            log_path,
            "download_started",
            job,
            attempt=attempt,
            target_path=str(target_path),
        )

        try:
            result = client.retrieve(DATASET, job.build_request(variables))
            result.download(str(partial_path))

            if not partial_path.exists() or partial_path.stat().st_size == 0:
                raise RuntimeError("Download concluido sem gerar arquivo valido.")

            partial_path.replace(target_path)

            log_event(
                log_path,
                "download_succeeded",
                job,
                attempt=attempt,
                target_path=str(target_path),
                size_bytes=target_path.stat().st_size,
            )
            return target_path
        except KeyboardInterrupt:
            if partial_path.exists():
                partial_path.unlink()

            log_event(log_path, "download_interrupted", job, attempt=attempt)
            raise
        except Exception as exc:
            if partial_path.exists():
                partial_path.unlink()

            log_event(
                log_path,
                "download_failed",
                job,
                attempt=attempt,
                error_type=type(exc).__name__,
                error_message=str(exc),
            )

            if attempt >= total_attempts:
                raise

            time.sleep(retry_delay_seconds)

    raise RuntimeError("Fluxo de retries terminou em estado invalido.")


def build_run_id() -> str:
    return f"{utc_now()}-pid{os.getpid()}"


def parse_worker_job(worker_job: list[int] | None) -> ChunkJob:
    if worker_job is None:
        raise SystemExit("Modo worker sem lote definido.")
    year, month, day_start, day_end = worker_job
    return ChunkJob(year=year, month=month, day_start=day_start, day_end=day_end)


def find_last_completed_job(jobs: list[ChunkJob], output_dir: Path) -> ChunkJob | None:
    last_completed = None
    for job in jobs:
        if job.output_path(output_dir).exists():
            last_completed = job
    return last_completed


def build_worker_command(
    args: argparse.Namespace,
    job: ChunkJob,
    *,
    output_dir: Path,
    state_path: Path,
    log_path: Path,
    run_id: str,
) -> list[str]:
    return [
        sys.executable,
        str(Path(__file__).resolve()),
        "--worker",
        "--worker-job",
        str(job.year),
        str(job.month),
        str(job.day_start),
        str(job.day_end),
        "--run-id",
        run_id,
        "--start-year",
        str(args.start_year),
        "--end-year",
        str(args.end_year),
        "--chunk-days",
        str(args.chunk_days),
        "--variable-set",
        args.variable_set,
        "--output-dir",
        str(output_dir),
        "--state-file",
        str(state_path),
        "--log-file",
        str(log_path),
        "--max-retries",
        str(args.max_retries),
        "--retry-delay-seconds",
        str(args.retry_delay_seconds),
    ] + (["--overwrite"] if args.overwrite else [])


def build_terminal_launch_command(
    worker_command: list[str],
    terminal_command: str,
) -> tuple[list[str], str]:
    if terminal_command == "none":
        return worker_command, "background"

    worker_shell_command = " ".join(shlex.quote(part) for part in worker_command)
    hold_shell_command = (
        f"{worker_shell_command}; "
        'exit_code=$?; '
        'printf "\\nWorker finalizado com status %s\\n" "$exit_code"; '
        "exec bash"
    )

    if terminal_command != "auto":
        if "{command}" not in terminal_command:
            raise SystemExit("--terminal-command customizado precisa conter {command}.")
        rendered = terminal_command.format(command=shlex.quote(hold_shell_command))
        return shlex.split(rendered), "custom"

    if not os.environ.get("DISPLAY") and not os.environ.get("WAYLAND_DISPLAY"):
        return worker_command, "background"

    if shutil.which("x-terminal-emulator"):
        return ["x-terminal-emulator", "-e", "bash", "-lc", hold_shell_command], "x-terminal-emulator"
    if shutil.which("gnome-terminal"):
        return ["gnome-terminal", "--", "bash", "-lc", hold_shell_command], "gnome-terminal"
    if shutil.which("xterm"):
        return ["xterm", "-hold", "-e", "bash", "-lc", hold_shell_command], "xterm"

    return worker_command, "background"


def launch_worker(
    *,
    args: argparse.Namespace,
    job: ChunkJob,
    output_dir: Path,
    state_path: Path,
    log_path: Path,
    run_id: str,
) -> bool:
    if not reserve_job_claim(output_dir, job):
        return False

    worker_command = build_worker_command(
        args,
        job,
        output_dir=output_dir,
        state_path=state_path,
        log_path=log_path,
        run_id=run_id,
    )
    launch_command, launch_mode = build_terminal_launch_command(
        worker_command,
        args.terminal_command,
    )

    try:
        subprocess.Popen(
            launch_command,
            start_new_session=True,
            stdout=None if launch_mode != "background" else subprocess.DEVNULL,
            stderr=None if launch_mode != "background" else subprocess.DEVNULL,
        )
    except Exception:
        release_job_claim(output_dir, job)
        raise

    log_event(
        log_path,
        "worker_launched",
        job,
        run_id=run_id,
        launch_mode=launch_mode,
        command=worker_command,
    )
    return True


def run_worker(
    *,
    args: argparse.Namespace,
    variables: list[str],
    output_dir: Path,
    log_path: Path,
) -> int:
    job = parse_worker_job(args.worker_job)
    target_path = job.output_path(output_dir)

    if target_path.exists() and not args.overwrite:
        log_event(log_path, "skipped_existing", job, target_path=str(target_path))
        write_result(output_dir, job, run_id=args.run_id, status="success")
        print(f"Lote {job.key} ja concluido em {target_path}.")
        return 0

    if not acquire_worker_claim(output_dir, job):
        print(f"Lote {job.key} ja esta em execucao por outro worker.")
        return 0

    client = create_client()

    try:
        download_job(
            client=client,
            job=job,
            variables=variables,
            output_dir=output_dir,
            log_path=log_path,
            max_retries=args.max_retries,
            retry_delay_seconds=args.retry_delay_seconds,
        )
    except KeyboardInterrupt:
        write_result(
            output_dir,
            job,
            run_id=args.run_id,
            status="interrupted",
            message="Worker interrompido pelo usuario.",
        )
        raise
    except Exception as exc:
        write_result(
            output_dir,
            job,
            run_id=args.run_id,
            status="failed",
            message=str(exc),
        )
        print(f"Falha no lote {job.key}: {exc}")
        return 1
    finally:
        release_job_claim(output_dir, job)

    if not target_path.exists():
        write_result(
            output_dir,
            job,
            run_id=args.run_id,
            status="failed",
            message=f"Arquivo final ausente em {target_path}",
        )
        print(f"Lote {job.key} terminou sem gerar arquivo final em {target_path}.")
        return 1

    write_result(output_dir, job, run_id=args.run_id, status="success")
    print(f"Lote {job.key} concluido em {target_path}.")
    return 0


def run_scheduler(
    *,
    args: argparse.Namespace,
    variables: list[str],
    output_dir: Path,
    state_path: Path,
    log_path: Path,
    jobs: list[ChunkJob],
) -> int:
    run_id = build_run_id()
    had_failures = False
    last_started_job: ChunkJob | None = None
    last_launch_at: float | None = None
    launch_delay = max(args.request_offset_seconds, args.sleep_between_requests)
    completed_jobs = 0
    active_jobs: list[ChunkJob] = []
    failed_jobs: list[ChunkJob] = []
    last_completed_job: ChunkJob | None = None
    last_error = None

    try:
        while True:
            completed_jobs, active_jobs, failed_jobs, next_pending = summarize_jobs(
                jobs,
                output_dir,
                overwrite=args.overwrite,
                run_id=run_id,
            )
            last_completed_job = find_last_completed_job(jobs, output_dir)
            last_error = None

            if failed_jobs:
                had_failures = True
                failure_payload = load_json(result_path(output_dir, failed_jobs[-1]))
                last_error = {
                    "job_key": failed_jobs[-1].key,
                    "error_type": failure_payload.get("status", "failed"),
                    "message": failure_payload.get("message", ""),
                    "timestamp": failure_payload.get("updated_at", utc_now()),
                }

                if not args.continue_on_error and not active_jobs:
                    write_json(
                        state_path,
                        build_state(
                            args=args,
                            variables=variables,
                            output_dir=output_dir,
                            log_path=log_path,
                            run_id=run_id,
                            total_jobs=len(jobs),
                            completed_jobs=completed_jobs,
                            active_jobs=active_jobs,
                            failed_jobs=failed_jobs,
                            last_started=last_started_job,
                            last_completed=last_completed_job,
                            last_error=last_error,
                            status="failed",
                        ),
                    )
                    print(
                        f"Falha no lote {failed_jobs[-1].key}. "
                        f"Consulte {log_path} e rode novamente para retomar."
                    )
                    return 1

            if completed_jobs == len(jobs) or (
                completed_jobs + len(failed_jobs) == len(jobs)
                and not active_jobs
                and next_pending is None
            ):
                final_status = "completed_with_errors" if had_failures else "completed"
                write_json(
                    state_path,
                    build_state(
                        args=args,
                        variables=variables,
                        output_dir=output_dir,
                        log_path=log_path,
                        run_id=run_id,
                        total_jobs=len(jobs),
                        completed_jobs=completed_jobs,
                        active_jobs=active_jobs,
                        failed_jobs=failed_jobs,
                        last_started=last_started_job,
                        last_completed=last_completed_job,
                        last_error=last_error,
                        status=final_status,
                    ),
                )
                print(f"Extracao concluida. Arquivos salvos em {output_dir}")
                return 0 if (not had_failures or args.continue_on_error) else 1

            if (
                next_pending
                and (args.continue_on_error or not failed_jobs)
                and len(active_jobs) < args.max_parallel_requests
                and (
                    last_launch_at is None
                    or time.monotonic() - last_launch_at >= launch_delay
                )
            ):
                if launch_worker(
                    args=args,
                    job=next_pending,
                    output_dir=output_dir,
                    state_path=state_path,
                    log_path=log_path,
                    run_id=run_id,
                ):
                    last_started_job = next_pending
                    last_launch_at = time.monotonic()

            write_json(
                state_path,
                build_state(
                    args=args,
                    variables=variables,
                    output_dir=output_dir,
                    log_path=log_path,
                    run_id=run_id,
                    total_jobs=len(jobs),
                    completed_jobs=completed_jobs,
                    active_jobs=active_jobs,
                    failed_jobs=failed_jobs,
                    last_started=last_started_job,
                    last_completed=last_completed_job,
                    last_error=last_error,
                    status="running",
                ),
            )
            time.sleep(1)
    except KeyboardInterrupt:
        write_json(
            state_path,
            build_state(
                args=args,
                variables=variables,
                output_dir=output_dir,
                log_path=log_path,
                run_id=run_id,
                total_jobs=len(jobs),
                completed_jobs=completed_jobs,
                active_jobs=active_jobs,
                failed_jobs=failed_jobs,
                last_started=last_started_job,
                last_completed=last_completed_job,
                last_error={
                    "job_key": last_started_job.key if last_started_job else None,
                    "error_type": "KeyboardInterrupt",
                    "message": "Scheduler interrompido pelo usuario.",
                    "timestamp": utc_now(),
                },
                status="interrupted",
            ),
        )
        raise


def main() -> int:
    args = parse_args()
    variables = list(VARIABLE_SETS[args.variable_set])

    if args.list_variable_sets:
        for name in sorted(VARIABLE_SETS):
            print(f"{name}: {VARIABLE_SET_DESCRIPTIONS[name]}")
            for variable in VARIABLE_SETS[name]:
                print(f"  - {variable}")
            print()
        return 0

    output_dir = args.output_dir.resolve()
    state_path, log_path = resolve_state_paths(
        output_dir=output_dir,
        state_file=args.state_file.resolve() if args.state_file else None,
        log_file=args.log_file.resolve() if args.log_file else None,
    )
    jobs = list(iter_jobs(args.start_year, args.end_year, args.chunk_days))

    if args.worker:
        return run_worker(
            args=args,
            variables=variables,
            output_dir=output_dir,
            log_path=log_path,
        )

    if args.status:
        print_status(
            args=args,
            variables=variables,
            output_dir=output_dir,
            state_path=state_path,
            log_path=log_path,
            jobs=jobs,
        )
        return 0

    completed_jobs, active_jobs, failed_jobs, next_pending = summarize_jobs(
        jobs,
        output_dir,
        overwrite=args.overwrite,
        run_id=None,
    )

    if args.dry_run:
        print(f"Total de lotes: {len(jobs)}")
        print(f"Lotes ja concluidos: {completed_jobs}")
        print(f"Lotes ativos: {len(active_jobs)}")
        print(f"Lotes falhos no run atual: {len(failed_jobs)}")
        if next_pending:
            print(f"Primeiro lote pendente: {next_pending.key}")
            print(f"Destino: {next_pending.output_path(output_dir)}")
        else:
            print("Nao ha lotes pendentes.")
        print(f"Preset de variaveis: {args.variable_set} ({len(variables)} variaveis)")
        return 0

    return run_scheduler(
        args=args,
        variables=variables,
        output_dir=output_dir,
        state_path=state_path,
        log_path=log_path,
        jobs=jobs,
    )


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("Execucao interrompida. Rode novamente para retomar do primeiro lote pendente.")
        raise SystemExit(130)
