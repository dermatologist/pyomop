"""Example usage of pyomop Eunomia data functionality.

This example demonstrates how to download, load, and work with OMOP CDM
sample datasets using the Eunomia functionality in pyomop.
"""

import asyncio
import tempfile
from pathlib import Path

from pyomop import CdmEngineFactory, CdmVector
from pyomop.eunomia import EunomiaData, download_eunomia_data


async def main():
    """Demonstrate Eunomia data functionality."""
    print("🔬 pyomop Eunomia Data Example")
    print("=" * 40)
    
    # Example 1: Download a dataset
    print("\n1. Downloading Eunomia dataset...")
    try:
        # Download GiBleed dataset (small sample dataset)
        # Note: This will try to download from the actual repository
        zip_path = download_eunomia_data(
            dataset_name="GiBleed",
            cdm_version="5.3",
            verbose=True
        )
        print(f"✅ Downloaded dataset to: {zip_path}")
    except Exception as e:
        print(f"❌ Download failed (this is expected in CI): {e}")
        # Create a mock dataset for demonstration
        with tempfile.TemporaryDirectory() as temp_dir:
            zip_path = await create_mock_dataset(temp_dir)
            print(f"📝 Created mock dataset at: {zip_path}")
    
    # Example 2: Load dataset into database
    print("\n2. Loading dataset into database...")
    
    # Create CDM engine (SQLite for this example)
    cdm = CdmEngineFactory()  # Default is SQLite
    eunomia = EunomiaData(cdm)
    
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp_db:
        db_path = tmp_db.name
    
    try:
        await eunomia.extract_load_data(
            from_path=zip_path,
            to_path=db_path,
            verbose=True
        )
        print(f"✅ Loaded dataset into database: {db_path}")
        
        # Example 3: Query the loaded data
        print("\n3. Querying the loaded data...")
        
        # Update the CDM engine to use our new database
        cdm_with_data = CdmEngineFactory(db='sqlite', name=db_path)
        vector = CdmVector()
        
        # Execute a simple query
        result = await vector.execute(
            cdm_with_data, 
            query="SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"
        )
        
        tables = [row[0] for row in result]
        print(f"📊 Found {len(tables)} tables in database:")
        for table in tables[:10]:  # Show first 10 tables
            print(f"  - {table}")
        if len(tables) > 10:
            print(f"  ... and {len(tables) - 10} more")
        
        # Count records in person table if it exists
        if 'person' in tables:
            result = await vector.execute(
                cdm_with_data,
                query="SELECT COUNT(*) as person_count FROM person;"
            )
            person_count = result[0][0] if result else 0
            print(f"👥 Person table contains {person_count} records")
        
        # Example 4: Export data back to CSV
        print("\n4. Exporting data to CSV files...")
        
        with tempfile.TemporaryDirectory() as export_dir:
            await eunomia.export_data_files(
                db_path=db_path,
                output_folder=export_dir,
                output_format="csv",
                verbose=True
            )
            
            exported_files = list(Path(export_dir).glob("*.csv"))
            print(f"📁 Exported {len(exported_files)} CSV files:")
            for file_path in exported_files[:5]:  # Show first 5 files
                print(f"  - {file_path.name}")
            if len(exported_files) > 5:
                print(f"  ... and {len(exported_files) - 5} more")
        
        await cdm_with_data.engine.dispose()
        
    except Exception as e:
        print(f"❌ Error processing dataset: {e}")
        raise
    finally:
        # Clean up
        import os
        if os.path.exists(db_path):
            os.unlink(db_path)
    
    print("\n✨ Example completed successfully!")


async def create_mock_dataset(temp_dir: str) -> str:
    """Create a mock Eunomia dataset for demonstration purposes."""
    import pandas as pd
    import zipfile
    
    # Create sample data files
    csv_dir = Path(temp_dir) / "csv_data"
    csv_dir.mkdir()
    
    # Person table
    person_data = {
        'person_id': [1, 2, 3, 4, 5],
        'gender_concept_id': [8532, 8507, 8532, 8507, 8532],
        'year_of_birth': [1980, 1990, 1985, 1975, 1995],
        'month_of_birth': [1, 5, 3, 12, 8],
        'day_of_birth': [15, 22, 8, 3, 17]
    }
    pd.DataFrame(person_data).to_csv(csv_dir / "person.csv", index=False)
    
    # Concept table (sample concepts)
    concept_data = {
        'concept_id': [8532, 8507, 4013886, 4013887],
        'concept_name': ['FEMALE', 'MALE', 'Temperature', 'Blood pressure'],
        'domain_id': ['Gender', 'Gender', 'Measurement', 'Measurement'],
        'vocabulary_id': ['Gender', 'Gender', 'SNOMED', 'SNOMED'],
        'concept_class_id': ['Gender', 'Gender', 'Clinical Finding', 'Clinical Finding'],
        'standard_concept': ['S', 'S', 'S', 'S'],
        'concept_code': ['F', 'M', '386725007', '75367002']
    }
    pd.DataFrame(concept_data).to_csv(csv_dir / "concept.csv", index=False)
    
    # Observation table
    observation_data = {
        'observation_id': [1, 2, 3, 4, 5],
        'person_id': [1, 2, 3, 1, 2],
        'observation_concept_id': [4013886, 4013887, 4013886, 4013887, 4013886],
        'observation_date': ['2023-01-15', '2023-01-16', '2023-01-17', '2023-01-18', '2023-01-19'],
        'value_as_number': [98.6, 120, 99.1, 80, 97.8]
    }
    pd.DataFrame(observation_data).to_csv(csv_dir / "observation.csv", index=False)
    
    # Create ZIP file
    zip_path = Path(temp_dir) / "GiBleed_5.3.zip"
    with zipfile.ZipFile(zip_path, 'w') as zipf:
        for csv_file in csv_dir.glob("*.csv"):
            zipf.write(csv_file, csv_file.name)
    
    return str(zip_path)


if __name__ == "__main__":
    # Run the example
    asyncio.run(main())