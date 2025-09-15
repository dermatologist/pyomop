"""Test CLI integration for Eunomia functionality."""

import ast
from pathlib import Path


def test_cli_has_eunomia_options():
    """Test that the CLI has the new Eunomia options."""
    main_path = Path(__file__).parent.parent / "src" / "pyomop" / "main.py"
    
    with open(main_path, 'r') as f:
        content = f.read()
    
    # Check for the new CLI options
    assert "--eunomia-dataset" in content, "Missing --eunomia-dataset option"
    assert "--eunomia-path" in content, "Missing --eunomia-path option"
    assert "eunomia_dataset" in content, "Missing eunomia_dataset parameter"
    assert "eunomia_path" in content, "Missing eunomia_path parameter"
    
    # Check for import and usage
    assert "from .eunomia import EunomiaData" in content, "Missing EunomiaData import"
    assert "download_eunomia_data" in content, "Missing download_eunomia_data usage"
    assert "extract_load_data" in content, "Missing extract_load_data usage"
    
    print("✅ CLI has proper Eunomia integration")


def test_cli_syntax():
    """Test that the CLI module has valid syntax."""
    main_path = Path(__file__).parent.parent / "src" / "pyomop" / "main.py"
    
    with open(main_path, 'r') as f:
        source_code = f.read()
    
    try:
        ast.parse(source_code)
        print("✅ CLI module syntax is valid")
    except SyntaxError as e:
        print(f"❌ Syntax error in main.py: {e}")
        raise


def test_cli_function_signature():
    """Test that the CLI function has the correct signature."""
    main_path = Path(__file__).parent.parent / "src" / "pyomop" / "main.py"
    
    with open(main_path, 'r') as f:
        source_code = f.read()
    
    tree = ast.parse(source_code)
    
    # Find the cli function
    cli_function = None
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == "cli":
            cli_function = node
            break
    
    assert cli_function is not None, "CLI function not found"
    
    # Check parameters
    param_names = [arg.arg for arg in cli_function.args.args]
    
    expected_params = [
        "version", "create", "dbtype", "host", "port", "user", "pw", 
        "name", "schema", "vocab", "input_path", "eunomia_dataset", "eunomia_path"
    ]
    
    for param in expected_params:
        assert param in param_names, f"Missing parameter: {param}"
    
    print(f"✅ CLI function has correct signature with {len(param_names)} parameters")


if __name__ == "__main__":
    print("Running CLI integration tests...")
    print("=" * 40)
    
    test_cli_syntax()
    test_cli_has_eunomia_options()
    test_cli_function_signature()
    
    print("\n✅ All CLI integration tests passed!")