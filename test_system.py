#!/usr/bin/env python3
"""
Test script to validate ETL system components.
"""

import sys
import os
sys.path.append('/opt/airflow/src')

import logging
import time
from datetime import datetime

from config import config
from utils import validate_config, get_system_stats, setup_logging
from processors.data_generator import DataGenerator
from processors.cdc_processor import CDCProcessor
from processors.batch_processor import BatchProcessor

def test_configuration():
    """Test system configuration."""
    print("🔧 Testing configuration...")
    try:
        validate_config()
        print("✅ Configuration validation passed")
        return True
    except Exception as e:
        print(f"❌ Configuration validation failed: {e}")
        return False

def test_data_generation():
    """Test data generation."""
    print("📊 Testing data generation...")
    try:
        generator = DataGenerator()
        generator.generate_batch_data(customers=5, products=10, orders=20)
        stats = generator.stats.to_dict()
        print(f"✅ Data generation successful: {stats}")
        return True
    except Exception as e:
        print(f"❌ Data generation failed: {e}")
        return False

def test_cdc_processor():
    """Test CDC processor health."""
    print("🔄 Testing CDC processor...")
    try:
        processor = CDCProcessor()
        health = processor.health_check()
        if health["status"] in ["healthy", "degraded"]:
            print(f"✅ CDC processor health: {health['status']}")
            return True
        else:
            print(f"❌ CDC processor unhealthy: {health}")
            return False
    except Exception as e:
        print(f"❌ CDC processor test failed: {e}")
        return False

def test_batch_processor():
    """Test batch processor."""
    print("📈 Testing batch processor...")
    try:
        processor = BatchProcessor()
        results = processor.process_batch()
        print(f"✅ Batch processing successful: {results}")
        return True
    except Exception as e:
        print(f"❌ Batch processing failed: {e}")
        return False

def test_system_stats():
    """Test system statistics."""
    print("📋 Testing system statistics...")
    try:
        stats = get_system_stats()
        print(f"✅ System stats retrieved: {stats}")
        return True
    except Exception as e:
        print(f"❌ System stats failed: {e}")
        return False

def main():
    """Main test function."""
    setup_logging("INFO")
    
    print("=== ETL System Validation ===")
    print(f"Timestamp: {datetime.now()}")
    print()
    
    tests = [
        ("Configuration", test_configuration),
        ("Data Generation", test_data_generation),
        ("CDC Processor", test_cdc_processor),
        ("Batch Processor", test_batch_processor),
        ("System Statistics", test_system_stats)
    ]
    
    results = {}
    for test_name, test_func in tests:
        try:
            results[test_name] = test_func()
        except Exception as e:
            print(f"❌ {test_name} test crashed: {e}")
            results[test_name] = False
        print()
    
    # Summary
    print("=== Test Summary ===")
    passed = sum(1 for result in results.values() if result)
    total = len(results)
    
    for test_name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{test_name}: {status}")
    
    print(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! ETL system is ready.")
        return 0
    else:
        print("⚠️  Some tests failed. Please check the configuration and services.")
        return 1

if __name__ == "__main__":
    sys.exit(main())