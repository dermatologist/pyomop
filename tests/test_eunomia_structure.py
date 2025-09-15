"""Simple syntax and structure test for Eunomia module.

This test validates the module structure without requiring external dependencies.
"""

import ast
import os
import sys
from pathlib import Path


def test_eunomia_module_syntax():
    """Test that the eunomia module has valid Python syntax."""
    module_path = Path(__file__).parent.parent / "src" / "pyomop" / "eunomia.py"
    
    with open(module_path, 'r') as f:
        source_code = f.read()
    
    try:
        ast.parse(source_code)
        print("✅ Eunomia module syntax is valid")
    except SyntaxError as e:
        print(f"❌ Syntax error in eunomia module: {e}")
        raise


def test_eunomia_module_structure():
    """Test that the eunomia module has expected classes and functions."""
    module_path = Path(__file__).parent.parent / "src" / "pyomop" / "eunomia.py"
    
    with open(module_path, 'r') as f:
        source_code = f.read()
    
    tree = ast.parse(source_code)
    
    # Find classes and functions
    classes = []
    functions = []
    
    for node in tree.body:  # Only look at top-level definitions
        if isinstance(node, ast.ClassDef):
            classes.append(node.name)
        elif isinstance(node, ast.FunctionDef):
            functions.append(node.name)
        elif isinstance(node, ast.AsyncFunctionDef):
            functions.append(node.name)
    
    # Check for expected classes
    expected_classes = ["EunomiaData"]
    for expected_class in expected_classes:
        assert expected_class in classes, f"Missing class: {expected_class}"
        print(f"✅ Found class: {expected_class}")
    
    # Check for expected functions
    expected_functions = [
        "download_eunomia_data",
        "extract_load_data", 
        "load_data_files",
        "export_data_files"
    ]
    for expected_function in expected_functions:
        assert expected_function in functions, f"Missing function: {expected_function}"
        print(f"✅ Found function: {expected_function}")
    
    print(f"ℹ️  Found {len(classes)} classes and {len(functions)} top-level functions")


def test_init_file_updated():
    """Test that __init__.py includes the new imports."""
    init_path = Path(__file__).parent.parent / "src" / "pyomop" / "__init__.py"
    
    with open(init_path, 'r') as f:
        content = f.read()
    
    # Check for eunomia imports
    assert "from .eunomia import" in content, "Missing eunomia imports in __init__.py"
    assert "EunomiaData" in content, "Missing EunomiaData import"
    assert "download_eunomia_data" in content, "Missing download_eunomia_data import"
    print("✅ __init__.py has been properly updated")


if __name__ == "__main__":
    print("Running Eunomia module structure tests...")
    print("=" * 40)
    
    test_eunomia_module_syntax()
    test_eunomia_module_structure()
    test_init_file_updated()
    
    print("\n✅ All structure tests passed!")