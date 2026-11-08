from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from equity_data_platform.batch import write_parquet_parts


def test_write_parquet_parts_splits_and_describes_files(tmp_path: Path) -> None:
    schema = pa.schema([pa.field("value", pa.int64(), nullable=False)])
    files = write_parquet_parts(
        records=[{"value": value} for value in range(5)],
        schema=schema,
        output_dir=tmp_path,
        max_rows_per_file=2,
    )

    assert [file["path"] for file in files] == [
        "part-00000.parquet",
        "part-00001.parquet",
        "part-00002.parquet",
    ]
    assert [file["row_count"] for file in files] == [2, 2, 1]
    assert all(file["byte_size"] > 0 and file["sha256"] for file in files)
    assert sum(
        pq.ParquetFile(tmp_path / file["path"]).metadata.num_rows for file in files
    ) == 5


def test_write_parquet_parts_handles_empty_input(tmp_path: Path) -> None:
    schema = pa.schema([pa.field("value", pa.int64(), nullable=False)])
    assert write_parquet_parts(records=[], schema=schema, output_dir=tmp_path) == []
    assert list(tmp_path.iterdir()) == []


def test_write_parquet_parts_rejects_invalid_limit(tmp_path: Path) -> None:
    schema = pa.schema([pa.field("value", pa.int64(), nullable=False)])
    with pytest.raises(ValueError, match="greater than zero"):
        write_parquet_parts(
            records=[{"value": 1}],
            schema=schema,
            output_dir=tmp_path,
            max_rows_per_file=0,
        )
