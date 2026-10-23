import csv
from pathlib import Path

import numpy as np
from dolfinx import fem


def sensor_report(mesh, temperature, displacement, sensors):
    """
    Report nearest temperature/displacement DOF to requested coordinates.
    Intended for monitoring and quick validation, not metrology.
    """

    Vt = temperature.function_space
    Vu = displacement.function_space

    rows = []

    Tcoords = Vt.tabulate_dof_coordinates()
    Ucoords = Vu.tabulate_dof_coordinates()

    Tarr = temperature.x.array.real
    Uarr = displacement.x.array.real.reshape((-1, 3))

    for sensor in sensors:
        target = np.asarray(sensor["xyz"], dtype=float)

        ti = np.argmin(np.linalg.norm(Tcoords - target, axis=1))
        ui = np.argmin(np.linalg.norm(Ucoords - target, axis=1))

        rows.append({
            "sensor_id": sensor["id"],
            "x_m": target[0],
            "y_m": target[1],
            "z_m": target[2],
            "temperature_C": float(Tarr[ti]),
            "Ux_m": float(Uarr[ui, 0]),
            "Uy_m": float(Uarr[ui, 1]),
            "Uz_m": float(Uarr[ui, 2]),
        })

    return rows


def write_sensor_csv(rows, path: Path):
    if not rows:
        return

    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=list(rows[0].keys())
        )
        writer.writeheader()
        writer.writerows(rows)
