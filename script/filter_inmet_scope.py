#!/usr/bin/env python3
"""Seleciona um subconjunto do INMET compativel com o escopo local do projeto."""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path

PRESETS = {
    "strict_sp_local": {
        "description": (
            "Escopo minimo para o TCC local em Sao Paulo: Mirante + Interlagos."
        ),
        "stations": {
            "A701": "SAO PAULO - MIRANTE",
            "A771": "SAO PAULO - INTERLAGOS",
        },
    },
    "metro_sp": {
        "description": (
            "Escopo local expandido: Mirante + Interlagos + Barueri."
        ),
        "stations": {
            "A701": "SAO PAULO - MIRANTE",
            "A771": "SAO PAULO - INTERLAGOS",
            "A755": "BARUERI",
        },
    },
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Lista ou materializa um subconjunto do INMET coerente com o escopo local "
            "do projeto."
        )
    )
    parser.add_argument(
        "--preset",
        choices=sorted(PRESETS),
        default="strict_sp_local",
        help="Preset de estacoes a manter.",
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("inmet"),
        help="Diretorio raiz com os CSVs brutos do INMET.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help=(
            "Diretorio de destino para materializar o subconjunto. "
            "Se omitido, o script so imprime o resumo."
        ),
    )
    parser.add_argument(
        "--copy",
        action="store_true",
        help="Copia os arquivos em vez de criar symlinks no destino.",
    )
    parser.add_argument(
        "--list-presets",
        action="store_true",
        help="Lista os presets disponiveis e encerra.",
    )
    return parser.parse_args()


def iter_matching_files(input_dir: Path, station_codes: set[str]) -> list[Path]:
    files = []
    for path in sorted(input_dir.glob("*/*.CSV")):
        if any(f"_{station_code}_" in path.name for station_code in station_codes):
            files.append(path)
    return files


def print_presets() -> None:
    for name in sorted(PRESETS):
        preset = PRESETS[name]
        print(f"{name}: {preset['description']}")
        for station_code, station_name in preset["stations"].items():
            print(f"  - {station_code}: {station_name}")
        print()


def summarize(files: list[Path], station_names: dict[str, str]) -> None:
    counts_by_station: dict[str, int] = {code: 0 for code in station_names}
    counts_by_year: dict[str, int] = {}

    for path in files:
        year = path.parent.name
        counts_by_year[year] = counts_by_year.get(year, 0) + 1
        for station_code in station_names:
            if f"_{station_code}_" in path.name:
                counts_by_station[station_code] += 1
                break

    print(f"Arquivos selecionados: {len(files)}")
    print("Por estacao:")
    for station_code, station_name in station_names.items():
        print(f"  - {station_code} | {station_name}: {counts_by_station[station_code]}")
    print("Por ano:")
    for year in sorted(counts_by_year):
        print(f"  - {year}: {counts_by_year[year]}")


def materialize(files: list[Path], output_dir: Path, copy_files: bool) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    for source_path in files:
        target_path = output_dir / source_path.parent.name / source_path.name
        target_path.parent.mkdir(parents=True, exist_ok=True)

        if target_path.exists() or target_path.is_symlink():
            continue

        if copy_files:
            shutil.copy2(source_path, target_path)
        else:
            target_path.symlink_to(source_path.resolve())


def main() -> int:
    args = parse_args()

    if args.list_presets:
        print_presets()
        return 0

    preset = PRESETS[args.preset]
    station_names = preset["stations"]
    files = iter_matching_files(args.input_dir, set(station_names))

    print(f"Preset: {args.preset}")
    print(preset["description"])
    summarize(files, station_names)

    if args.output_dir is not None:
        materialize(files, args.output_dir, copy_files=args.copy)
        mode = "copiados" if args.copy else "linkados"
        print(f"Arquivos {mode} em {args.output_dir.resolve()}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
