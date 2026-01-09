# test_imports.py - FIXED VERSION
import os
import sys

# Add project root to path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

# ALSO add src/modules to path - THIS IS THE FIX!
src_modules_path = os.path.join(project_root, 'src', 'modules')
sys.path.insert(0, src_modules_path)

print(f"Project root: {project_root}")
print(f"Python path includes:")
for i, path in enumerate(sys.path[:5], 1):
    if os.path.exists(path):
        print(f"  {i}. ✅ {path}")
    else:
        print(f"  {i}. ❌ {path} (does not exist)")

# Test importing each module
modules_to_test = [
    'bkam_forex',
    'bkam_treasury_official', 
    'investing_masi',
    'trading_economics',
    'yahoo_markets'
]

print("\n" + "="*60)
print("TESTING MODULE IMPORTS")
print("="*60)

for module_name in modules_to_test:
    module_path = os.path.join(src_modules_path, f'{module_name}.py')
    
    if os.path.exists(module_path):
        print(f"\n📁 {module_name}.py exists at: {module_path}")
        
        try:
            # Try importing
            module = __import__(module_name)
            print(f"✅ SUCCESS: Imported {module_name}")
            
            # Check functions
            if hasattr(module, 'collect_data'):
                print(f"   Has 'collect_data()' function: ✅")
            if hasattr(module, 'main'):
                print(f"   Has 'main()' function: ✅")
            if hasattr(module, 'run'):
                print(f"   Has 'run()' function: ✅")
                
        except ImportError as e:
            print(f"❌ FAILED: Import error: {e}")
            print(f"   Trying alternative import method...")
            
            # Try with different import method
            try:
                import importlib.util
                spec = importlib.util.spec_from_file_location(module_name, module_path)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                print(f"✅ SUCCESS: Loaded via importlib")
            except Exception as e2:
                print(f"❌ Alternative method also failed: {e2}")
    else:
        print(f"\n❌ {module_name}.py NOT FOUND at: {module_path}")