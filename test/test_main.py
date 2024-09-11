import pytest
import yaml
import os
from unittest.mock import patch


from main.main import load_config, arg_parser


def test_load_config():
    """Create a temporary YAML file for testing, load it and check if it's the same when loaded with load_config"""
    config_data = {
        "mytest": {
            "first_test": "This is the test",
            "second_test": "This is the second test",
        },
    }

    # Path for a temporary config file
    temp_config_path = "dummy_for_test.yaml"

    # Write test data to the temporary YAML file
    with open(temp_config_path, "w") as file:
        yaml.dump(config_data, file)

    try:
        config = load_config(temp_config_path)

        assert config == config_data, "Loaded data is not the same"

    finally:
        os.remove(temp_config_path)


def test_load_config_missing_file():
    """check if function properly raises FileNotFoundError in case of missing yaml file"""
    with pytest.raises(FileNotFoundError):
        load_config("non_existent_file.yaml")


def test_arg_parser_default():
    """Test with no command line arguments (default values)"""
    with patch("sys.argv", ["main/main.py"]):
        args = arg_parser()
        assert args.config_file == "param_config.yaml"


def test_arg_parser_with_args():
    """Test with specific command line arguments"""
    with patch("sys.argv", ["main/main.py", "--config_file", "custom_config.yaml"]):
        args = arg_parser()
        assert args.config_file == "custom_config.yaml"


def test_arg_parser_help(capsys):
    """Tests if the help output works properly"""
    with pytest.raises(SystemExit), patch("sys.argv", ["main/main.py", "--help"]):
        arg_parser()
        captured = capsys.readouterr()
        assert "--config_file" in captured.out


if __name__ == "__main__":
    pytest.main()
