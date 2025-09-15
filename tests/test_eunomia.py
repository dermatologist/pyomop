"""Tests for Eunomia data functionality."""

import asyncio
import os
import tempfile
import types
from unittest import mock

import pytest

from src.pyomop.eunomia import EunomiaData


class DummyEngine:
    def __init__(self):
        self.begin_called = False

        class DummyURL:
            def get_backend_name(self):
                return "sqlite"

        self.url = DummyURL()

    def begin(self):
        self.begin_called = True

        class DummyConn:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                pass

            async def run_sync(self, fn):
                # Simulate table list or DataFrame fetch
                if hasattr(fn, "__name__") and fn.__name__ == "get_tables":
                    return ["table1", "table2"]
                if hasattr(fn, "__name__") and fn.__name__ == "fetch_df":
                    import pandas as pd

                    return pd.DataFrame({"a": [1, 2]})
                # Simulate DataFrame to_sql
                return None

            async def exec_driver_sql(self, *args, **kwargs):
                # No-op for PRAGMA/SET calls in tests
                return None

        return DummyConn()


def test_init_defaults():
    e = EunomiaData()
    assert e.cdm is not None
    assert isinstance(e.base_url, str)


def test_download_eunomia_data_success(tmp_path):
    eunomia = EunomiaData()
    # Patch requests.get to simulate download
    with mock.patch("requests.get") as mget:
        mresp = mock.Mock()
        mresp.iter_content = lambda chunk_size: [b"data"]
        mresp.raise_for_status = lambda: None
        mget.return_value = mresp
        out = eunomia.download_eunomia_data(
            "testset", path_to_data=str(tmp_path), overwrite=True
        )
        assert os.path.exists(out)


def test_download_eunomia_data_already_exists(tmp_path):
    eunomia = EunomiaData()
    # Default version in code is 5.3
    f = tmp_path / "foo_5.3.zip"
    f.write_bytes(b"abc")
    out = eunomia.download_eunomia_data(
        "foo", path_to_data=str(tmp_path), overwrite=False
    )
    assert out == str(f)


def test_download_eunomia_data_invalid():
    eunomia = EunomiaData()
    with pytest.raises(ValueError):
        eunomia.download_eunomia_data("")


def test_download_eunomia_data_request_error(tmp_path):
    eunomia = EunomiaData()
    with mock.patch("requests.get", side_effect=Exception("fail")):
        with pytest.raises(Exception):
            eunomia.download_eunomia_data(
                "foo", path_to_data=str(tmp_path), overwrite=True
            )


@pytest.mark.asyncio
async def test_load_data_files_errors(tmp_path):
    eunomia = EunomiaData()
    # Invalid format
    with pytest.raises(ValueError):
        await eunomia.load_data_files(str(tmp_path), input_format="bad")
    # Path does not exist
    with pytest.raises(FileNotFoundError):
        await eunomia.load_data_files(str(tmp_path / "nope"), input_format="csv")
    # No files in dir
    empty = tmp_path / "empty"
    empty.mkdir()
    with pytest.raises(FileNotFoundError):
        await eunomia.load_data_files(str(empty), input_format="csv")


@pytest.mark.asyncio
async def test_load_data_files_success(tmp_path, monkeypatch):
    eunomia = EunomiaData()
    # Patch engine property using PropertyMock
    with mock.patch.object(
        type(eunomia.cdm), "engine", new_callable=mock.PropertyMock
    ) as eng_mock:
        eng_mock.return_value = DummyEngine()
        # Create a dummy csv file
        f = tmp_path / "foo.csv"
        f.write_text("a\n1\n2")
        # Patch pandas.read_csv to avoid actual IO
        monkeypatch.setattr(
            "pandas.read_csv",
            lambda path: __import__("pandas").DataFrame({"a": [1, 2]}),
        )
        await eunomia.load_data_files(str(tmp_path), input_format="csv")


@pytest.mark.asyncio
async def test_export_data_files_success(tmp_path, monkeypatch):
    eunomia = EunomiaData()
    with mock.patch.object(
        type(eunomia.cdm), "engine", new_callable=mock.PropertyMock
    ) as eng_mock:
        eng_mock.return_value = DummyEngine()
        outdir = tmp_path / "out"

        # Patch DataFrame to_csv only (parquet export skipped to avoid ImportError)
        class DummyDF:
            def to_csv(self, path, index):
                self.csv = path

        monkeypatch.setattr("pandas.read_sql_query", lambda q, c: DummyDF())
        await eunomia.export_data_files(str(outdir), output_format="csv")
        # Skipping parquet export to avoid ImportError if pyarrow/fastparquet is not installed


def test_extract_load_data_zip(tmp_path, monkeypatch):
    eunomia = EunomiaData()
    # Create a dummy zip file
    zf = tmp_path / "foo.zip"
    import zipfile

    with zipfile.ZipFile(zf, "w") as z:
        f = tmp_path / "bar.csv"
        f.write_text("a\n1")
        z.write(f, arcname="bar.csv")
    # Patch load_data_files to just check call
    called = {}

    async def fake_load_data_files(*args, **kwargs):
        called["yes"] = True

    monkeypatch.setattr(eunomia, "load_data_files", fake_load_data_files)
    asyncio.run(eunomia.extract_load_data(str(zf), dataset_name="TestSet"))
    assert called["yes"]
