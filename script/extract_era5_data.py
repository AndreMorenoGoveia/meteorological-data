#!/usr/bin/env python3
"""Extracao em lote do ERA5-Land com checkpoint e retomada automatica."""

from __future__ import annotations

import argparse
import calendar
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

DATASET = "reanalysis-era5-land"
DEFAULT_START_YEAR = 2020
DEFAULT_END_YEAR = 2024
DEFAULT_CHUNK_DAYS = 4
DEFAULT_RETRY_DELAY_SECONDS = 30
DEFAULT_MAX_RETRIES = 3

VARIABLES = [
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

    def build_request(self) -> dict[str, Any]:
        return {
            "variable": VARIABLES,
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
    output_dir: Path,
    log_path: Path,
    total_jobs: int,
    completed_jobs: int,
    last_started: ChunkJob | None = None,
    last_completed: ChunkJob | None = None,
    last_error: dict[str, Any] | None = None,
    status: str,
) -> dict[str, Any]:
    return {
        "dataset": DATASET,
        "updated_at": utc_now(),
        "status": status,
        "range": {
            "start_year": args.start_year,
            "end_year": args.end_year,
            "chunk_days": args.chunk_days,
        },
        "output_dir": str(output_dir),
        "log_file": str(log_path),
        "progress": {
            "completed_jobs": completed_jobs,
            "total_jobs": total_jobs,
            "pending_jobs": total_jobs - completed_jobs,
        },
        "last_started": last_started.to_dict() if last_started else None,
        "last_completed": last_completed.to_dict() if last_completed else None,
        "last_error": last_error,
    }


def summarize_jobs(jobs: list[ChunkJob], output_dir: Path) -> tuple[int, ChunkJob | None]:
    completed = 0
    next_pending = None

    for job in jobs:
        if job.output_path(output_dir).exists():
            completed += 1
        elif next_pending is None:
            next_pending = job

    return completed, next_pending


def print_status(
    *,
    args: argparse.Namespace,
    output_dir: Path,
    state_path: Path,
    log_path: Path,
    jobs: list[ChunkJob],
) -> None:
    completed_jobs, next_pending = summarize_jobs(jobs, output_dir)
    state = load_json(state_path)
    recent_events = load_log_tail(log_path)

    print(f"Dataset: {DATASET}")
    print(f"Periodo: {args.start_year}..{args.end_year}")
    print(f"Lote de dias: {args.chunk_days}")
    print(f"Diretorio de saida: {output_dir}")
    print(f"Arquivo de estado: {state_path}")
    print(f"Arquivo de log: {log_path}")
    print(f"Concluidos: {completed_jobs}/{len(jobs)}")
    print(f"Pendentes: {len(jobs) - completed_jobs}")

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
            result = client.retrieve(DATASET, job.build_request())
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


def main() -> int:
    args = parse_args()
    output_dir = args.output_dir.resolve()
    state_path, log_path = resolve_state_paths(
        output_dir=output_dir,
        state_file=args.state_file.resolve() if args.state_file else None,
        log_file=args.log_file.resolve() if args.log_file else None,
    )
    jobs = list(iter_jobs(args.start_year, args.end_year, args.chunk_days))

    if args.status:
        print_status(
            args=args,
            output_dir=output_dir,
            state_path=state_path,
            log_path=log_path,
            jobs=jobs,
        )
        return 0

    if args.overwrite:
        completed_jobs = 0
        next_pending = jobs[0] if jobs else None
    else:
        completed_jobs, next_pending = summarize_jobs(jobs, output_dir)

    if args.dry_run:
        print(f"Total de lotes: {len(jobs)}")
        print(f"Lotes ja concluidos: {completed_jobs}")
        if next_pending:
            print(f"Primeiro lote pendente: {next_pending.key}")
            print(f"Destino: {next_pending.output_path(output_dir)}")
        else:
            print("Nao ha lotes pendentes.")
        return 0

    last_completed_job: ChunkJob | None = None
    had_failures = False
    state = load_json(state_path)
    if state.get("last_completed"):
        completed = state["last_completed"]
        last_completed_job = ChunkJob(
            year=completed["year"],
            month=completed["month"],
            day_start=completed["day_start"],
            day_end=completed["day_end"],
        )

    write_json(
        state_path,
        build_state(
            args=args,
            output_dir=output_dir,
            log_path=log_path,
            total_jobs=len(jobs),
            completed_jobs=completed_jobs,
            last_completed=last_completed_job,
            status="running",
        ),
    )

    client = create_client()

    for job in jobs:
        target_path = job.output_path(output_dir)

        if target_path.exists() and not args.overwrite:
            last_completed_job = job
            log_event(
                log_path,
                "skipped_existing",
                job,
                target_path=str(target_path),
            )
            write_json(
                state_path,
                build_state(
                    args=args,
                    output_dir=output_dir,
                    log_path=log_path,
                    total_jobs=len(jobs),
                    completed_jobs=completed_jobs,
                    last_completed=last_completed_job,
                    status="running",
                ),
            )
            continue

        write_json(
            state_path,
            build_state(
                args=args,
                output_dir=output_dir,
                log_path=log_path,
                total_jobs=len(jobs),
                completed_jobs=completed_jobs,
                last_started=job,
                last_completed=last_completed_job,
                status="running",
            ),
        )

        try:
            download_job(
                client=client,
                job=job,
                output_dir=output_dir,
                log_path=log_path,
                max_retries=args.max_retries,
                retry_delay_seconds=args.retry_delay_seconds,
            )
        except KeyboardInterrupt:
            write_json(
                state_path,
                build_state(
                    args=args,
                    output_dir=output_dir,
                    log_path=log_path,
                    total_jobs=len(jobs),
                    completed_jobs=completed_jobs,
                    last_started=job,
                    last_completed=last_completed_job,
                    last_error={
                        "job_key": job.key,
                        "error_type": "KeyboardInterrupt",
                        "message": "Execucao interrompida pelo usuario.",
                        "timestamp": utc_now(),
                    },
                    status="interrupted",
                ),
            )
            raise
        except Exception as exc:
            had_failures = True
            error_payload = {
                "job_key": job.key,
                "error_type": type(exc).__name__,
                "message": str(exc),
                "timestamp": utc_now(),
            }
            write_json(
                state_path,
                build_state(
                    args=args,
                    output_dir=output_dir,
                    log_path=log_path,
                    total_jobs=len(jobs),
                    completed_jobs=completed_jobs,
                    last_started=job,
                    last_completed=last_completed_job,
                    last_error=error_payload,
                    status="failed",
                ),
            )

            if not args.continue_on_error:
                print(
                    f"Falha no lote {job.key}. "
                    f"Consulte {log_path} e rode novamente para retomar."
                )
                return 1

            continue

        if not target_path.exists():
            print(f"Lote {job.key} terminou sem gerar arquivo final em {target_path}.")
            return 1

        completed_jobs += 1
        last_completed_job = job
        write_json(
            state_path,
            build_state(
                args=args,
                output_dir=output_dir,
                log_path=log_path,
                total_jobs=len(jobs),
                completed_jobs=completed_jobs,
                last_started=job,
                last_completed=last_completed_job,
                status="running",
            ),
        )

        if args.sleep_between_requests:
            time.sleep(args.sleep_between_requests)

    write_json(
        state_path,
        build_state(
            args=args,
            output_dir=output_dir,
            log_path=log_path,
            total_jobs=len(jobs),
            completed_jobs=completed_jobs,
            last_completed=last_completed_job,
            status="completed_with_errors" if had_failures else "completed",
        ),
    )
    print(f"Extracao concluida. Arquivos salvos em {output_dir}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("Execucao interrompida. Rode novamente para retomar do primeiro lote pendente.")
        raise SystemExit(130)
