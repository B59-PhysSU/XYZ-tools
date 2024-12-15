from dataclasses import dataclass
from typing import List, Optional


@dataclass
class XYZFrame:
    num_atoms: int
    comment: str
    atom_labels: List[str | int]
    pos_x: List[float]
    pos_y: List[float]
    pos_z: List[float]
    extra: List[List[float]]


def parse_numeric(value: str) -> float | int:
    try:
        return int(value)
    except ValueError as _:
        return float(value)


class XYZReader:
    def __init__(self, filename):
        self.file = open(filename, "r", encoding="ascii")

    def __read_frame(self) -> XYZFrame:
        num_atoms = int(next(self.file))
        comment = next(self.file)

        labels = []
        pos_x = []
        pos_y = []
        pos_z = []
        extra_data: List[List[float]] = []
        for _ in range(num_atoms):
            row = next(self.file).split()

            atom_label = row[0]
            # support for int atom labels
            try:
                atom_label = int(atom_label)
            except ValueError as _:
                ...
            labels.append(atom_label)
            pos_x.append(float(row[1]))
            pos_y.append(float(row[2]))
            pos_z.append(float(row[3]))

            for idx, datum in enumerate(row[4:]):
                num = parse_numeric(datum)
                try:
                    extra_data[idx].append(num)
                except IndexError as _:
                    extra_data.append([num])

        return XYZFrame(num_atoms, comment, labels, pos_x, pos_y, pos_z, extra_data)

    def __iter__(self):
        return self

    def __next__(self) -> XYZFrame:
        return self.__read_frame()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.file.close()
