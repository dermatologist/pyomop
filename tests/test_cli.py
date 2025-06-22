import pytest
from src.pyomop.main import main_routine


def test_main_routine(capsys):
    # assert exit code 0``
    with pytest.raises(SystemExit) as pytest_wrapped_e:
        main_routine()
        captured = capsys.readouterr()
        print(captured.out)
        assert "Pyomop" in captured.out
        # assert pytest_wrapped_e.type == SystemExit
        # assert pytest_wrapped_e.value.code == 0
