import re
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, List, Tuple

KV_REGEX = r'(\w+)=(".*?"|\S+)'


class DataType(Enum):
    STRING = 1
    REAL = 2
    INT = 3
    BOOL = 4

    def get_parser(self) -> Callable[[str], Any]:
        if self == DataType.STRING:
            return str
        elif self == DataType.REAL:
            return float
        elif self == DataType.INT:
            return int
        elif self == DataType.BOOL:
            return bool
        else:
            raise ValueError(f"Unknown data type: {self}")

    @staticmethod
    def from_string(data_type: str) -> DataType:
        if data_type == "S":
            return DataType.STRING
        elif data_type == "R":
            return DataType.REAL
        elif data_type == "I":
            return DataType.INT
        elif data_type == "B":
            return DataType.BOOL
        else:
            raise ValueError(f"Unknown data type: {data_type}")


@dataclass
class FrameProperties:
    label: str
    datatype: DataType
    column_count: int


@dataclass
class XYZFrame:
    num_atoms: int
    LatticeVectors: List[Tuple[float, float, float]]
    FrameProperties: List[FrameProperties]
    ExtraKV: Dict[str, str | int | float | bool]
    data: Dict[str, Any]


class ExtXYZReader:
    def __init__(self, filename):
        self.file = open(filename, "r", encoding="ascii")

    def __parse_extended_comment(self, comment: str) -> dict:
        kv_pairs = re.findall(KV_REGEX, comment)
        kv_dict = {k: v.strip('"') for k, v in kv_pairs}

        lattice_vec_data = map(float, kv_dict["Lattice"].split())
        lattice_vectors = zip(*[iter(lattice_vec_data)] * 3)
        kv_dict["Lattice"] = list(lattice_vectors)

        properties = kv_dict["Properties"].split(":")
        properties_list = []
        for i in range(0, len(properties), 3):
            label = properties[i]
            datatype = DataType.from_string(properties[i + 1])
            column_count = int(properties[i + 2])
            properties_list.append(FrameProperties(label, datatype, column_count))
        kv_dict["Properties"] = properties_list

        for key in kv_dict:
            if key == "Lattice":
                continue
            if key == "Properties":
                continue

            try:
                int_val = int(kv_dict[key])
                kv_dict[key] = int_val
                continue
            except ValueError as _:
                ...

            try:
                float_val = float(kv_dict[key])
                kv_dict[key] = float_val
                continue
            except ValueError as _:
                ...

            if kv_dict[key] == "T":
                kv_dict[key] = True
            elif kv_dict[key] == "F":
                kv_dict[key] = False

        return kv_dict

    def __read_frame(self) -> XYZFrame:
        num_atoms = int(next(self.file))
        comment = self.file.__next__()
        structured_comment = self.__parse_extended_comment(comment)
        total_num_columns = sum(
            [prop.column_count for prop in structured_comment["Properties"]]
        )

        data: Dict[str, Any] = {}
        for atom_idx in range(num_atoms):
            line = next(self.file)
            split = line.split()
            assert (
                len(split) == total_num_columns
            ), f"Expected {total_num_columns} columns, got {len(split)}, at line {atom_idx + 3}"

            offset = 0
            for prop in structured_comment["Properties"]:
                raw_data = split[offset : offset + prop.column_count]
                parser = prop.datatype.get_parser()
                if prop.label not in data:
                    data[prop.label] = []

                if prop.column_count == 1:
                    data[prop.label].append(parser(raw_data[0]))
                else:
                    data[prop.label].append([parser(val) for val in raw_data])

                offset += prop.column_count

        return XYZFrame(
            num_atoms=num_atoms,
            LatticeVectors=structured_comment["Lattice"],
            FrameProperties=structured_comment["Properties"],
            ExtraKV={
                k: v
                for k, v in structured_comment.items()
                if k not in ["Lattice", "Properties"]
            },
            data=data,
        )

    def close(self):
        self.file.close()

    def __iter__(self):
        return self

    def __next__(self):
        return self.__read_frame()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.file.close()
