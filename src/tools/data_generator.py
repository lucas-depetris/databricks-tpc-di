# Databricks notebook source
# DBTITLE 1,Widget Parameters
# Allow calling this notebook with dbutils.notebook.run() and passing parameters
dbutils.widgets.text("scale_factor", "10", "Scale Factor")
dbutils.widgets.text("catalog", "tpcdi", "Catalog")
dbutils.widgets.text("tpcdi_directory", "", "TPC-DI Directory")
dbutils.widgets.text("workspace_src_path", "", "Workspace Source Path")
dbutils.widgets.text("UC_enabled", "True", "Unity Catalog Enabled")
dbutils.widgets.text("lighthouse", "False", "Lighthouse Flag")

# Read widget values - all widgets return strings
widget_scale_factor = dbutils.widgets.get("scale_factor")
widget_catalog = dbutils.widgets.get("catalog")
widget_tpcdi_directory = dbutils.widgets.get("tpcdi_directory")
widget_workspace_src_path = dbutils.widgets.get("workspace_src_path")
widget_UC_enabled_str = dbutils.widgets.get("UC_enabled")
widget_lighthouse_str = dbutils.widgets.get("lighthouse")

# Set global variables - always assign from widgets if they have values
if widget_scale_factor:
    scale_factor = widget_scale_factor
if widget_catalog:
    catalog = widget_catalog
if widget_tpcdi_directory:
    tpcdi_directory = widget_tpcdi_directory
if widget_workspace_src_path:
    workspace_src_path = widget_workspace_src_path

# Boolean conversions - always set these even if False
UC_enabled = widget_UC_enabled_str.lower() == "true"
lighthouse = widget_lighthouse_str.lower() == "true"

print(f"📋 Variables loaded:")
print(f"  scale_factor: {scale_factor if 'scale_factor' in dir() else 'NOT SET'}")
print(f"  catalog: {catalog if 'catalog' in dir() else 'NOT SET'}")
print(f"  tpcdi_directory: {tpcdi_directory if 'tpcdi_directory' in dir() else 'NOT SET'}")
print(f"  workspace_src_path: {workspace_src_path if 'workspace_src_path' in dir() else 'NOT SET'}")
print(f"  UC_enabled: {UC_enabled}")
print(f"  lighthouse: {lighthouse}")

# COMMAND ----------
import os
import concurrent.futures
import requests
import shutil
import subprocess
import shlex

# COMMAND ----------

def move_file(source_location, target_location):
  shutil.copyfile(source_location, target_location)
  return f"Finished moving {source_location} to {target_location}"

def copy_directory(source_dir, target_dir, overwrite):
  if os.path.exists(target_dir) and overwrite:
    print(f"Overwrite set to true. Deleting: {target_dir}.")
    shutil.rmtree(target_dir)
    print(f"Deleted {target_dir}.")
  try:
    dst = shutil.copytree(source_dir, target_dir)
    print(f"Copied {source_dir} to {target_dir} succesfully!")
    return dst
  except FileExistsError:
    print(f"The folder you're trying to write to exists. Please delete it or set overwrite=True.")
  except FileNotFoundError:
    print(f"The folder you're trying to copy doesn't exist: {source_dir}")
    
def generate_data():
  print("=" * 80)
  print("Starting Data Generation Process")
  print("=" * 80)
  
  # Check for required variables (should be set via widgets or global scope)
  required_vars = {
    'scale_factor': 'Scale factor for data generation',
    'tpcdi_directory': 'TPC-DI directory path',
    'UC_enabled': 'Unity Catalog enabled flag',
    'workspace_src_path': 'Workspace source path',
    'catalog': 'Catalog name',
    'lighthouse': 'Lighthouse flag'
  }
  
  missing_vars = []
  for var_name, description in required_vars.items():
    # Check if variable exists in globals - don't check truthiness because False is valid
    if var_name not in globals():
      missing_vars.append(f"{var_name} ({description})")
    # For string variables, also check if they're empty
    elif var_name in ['scale_factor', 'tpcdi_directory', 'workspace_src_path', 'catalog']:
      if not globals()[var_name]:
        missing_vars.append(f"{var_name} ({description})")
  
  if missing_vars:
    error_msg = "\n⚠ ERROR: Required variables are not defined!\n"
    error_msg += "Missing variables:\n"
    for var in missing_vars:
      error_msg += f"  - {var}\n"
    error_msg += "\nThis notebook can be called in two ways:\n"
    error_msg += "1. Via dbutils.notebook.run() with parameters (recommended)\n"
    error_msg += "2. Via 'TPC-DI Driver.py' which executes './tools/setup'\n"
    error_msg += "\nExample dbutils.notebook.run() call:\n"
    error_msg += '  dbutils.notebook.run(\n'
    error_msg += '    "path/to/data_generator",\n'
    error_msg += '    timeout_seconds=3600,\n'
    error_msg += '    arguments={\n'
    error_msg += '      "scale_factor": "10",\n'
    error_msg += '      "catalog": "tpcdi_benchmark",\n'
    error_msg += '      "tpcdi_directory": "/Volumes/catalog/schema/volume/",\n'
    error_msg += '      "workspace_src_path": "/Repos/user/repo/src",\n'
    error_msg += '      "UC_enabled": "True",\n'
    error_msg += '      "lighthouse": "False"\n'
    error_msg += '    }\n'
    error_msg += '  )\n'
    error_msg += "=" * 80
    print(error_msg)
    dbutils.notebook.exit(error_msg)
    raise Exception(error_msg)
  
  DRIVER_ROOT      = "/local_disk0"
  tpcdi_tmp_path   = "/tmp/tpcdi/"
  driver_tmp_path  = f"{DRIVER_ROOT}{tpcdi_tmp_path}datagen/"
  driver_out_path  = f"{DRIVER_ROOT}{tpcdi_tmp_path}sf={scale_factor}"
  blob_out_path    = f"{tpcdi_directory}sf={scale_factor}"
  print(f"Configuration:")
  print(f"  - Scale Factor: {scale_factor}")
  print(f"  - Driver Output Path: {driver_out_path}")
  print(f"  - Target Blob Path: {blob_out_path}")
  print(f"  - Unity Catalog Enabled: {UC_enabled}")
  if UC_enabled:  # Unity Catalog enabled so use VOLUMES to store raw files
    os_blob_out_path = blob_out_path
  else: # No Unity Catalog enabled so use DBFS to store raw files instead of volumes
    os_blob_out_path = f"/dbfs{blob_out_path}"
    blob_out_path = f"dbfs:{blob_out_path}"
  print(f"  - OS Blob Path: {os_blob_out_path}") 

  print("\n" + "-" * 80)
  print("Checking if data already exists...")
  
  # Check if directory exists AND contains files
  data_exists = False
  if os.path.exists(os_blob_out_path):
    try:
      # Check if directory has any files (recursively)
      has_files = any(os.path.isfile(os.path.join(root, f)) 
                     for root, dirs, files in os.walk(os_blob_out_path) 
                     for f in files)
      if has_files:
        # Count files to verify
        file_count = sum(len(files) for _, _, files in os.walk(os_blob_out_path))
        print(f"  Directory exists: {os_blob_out_path}")
        print(f"  Found {file_count} existing file(s)")
        data_exists = True
      else:
        print(f"  Directory exists but is empty: {os_blob_out_path}")
        print(f"  Will regenerate data...")
    except Exception as e:
      print(f"  ⚠ Error checking directory contents: {e}")
      print(f"  Will regenerate data...")
  else:
    print(f"  Directory does not exist: {os_blob_out_path}")
  
  if data_exists:
    print(f"✓ Data generation skipped since the raw data/directory {blob_out_path} already exists for this scale factor.")
  else:
    print(f"✗ Raw Data Directory {blob_out_path} does not exist yet.")
    print(f"  Proceeding to generate data for scale factor={scale_factor} into this directory")
    print("\n" + "-" * 80)
    print("Step 1: Copying DIGen tool to driver...")
    copy_directory(f"{workspace_src_path}/tools/datagen", driver_tmp_path, overwrite=True)
    print("\n" + "-" * 80)
    print("Step 2: Generating data with DIGen...")
    print(f"  Starting data generation for scale factor={scale_factor}")
    print(f"  Output directory: {driver_out_path}")
    DIGen(driver_tmp_path, scale_factor, driver_out_path)
    print(f"✓ Data generation for scale factor={scale_factor} has completed in directory: {driver_out_path}")
    print("\n" + "-" * 80)
    print("Step 3: Preparing to move files to storage...")
    print(f"  Source: {driver_out_path}")
    print(f"  Destination: {blob_out_path}")
    if catalog != 'hive_metastore':
      print("\n  Setting up Unity Catalog resources...")
      catalog_exists = spark.sql(f"SELECT count(*) FROM system.information_schema.tables WHERE table_catalog = '{catalog}'").first()[0] > 0
      if not catalog_exists:
        print(f"    Creating catalog: {catalog}")
        spark.sql(f"""CREATE CATALOG IF NOT EXISTS {catalog}""")
        spark.sql(f"""GRANT ALL PRIVILEGES ON CATALOG {catalog} TO `account users`""")
      else:
        print(f"    Catalog {catalog} already exists")
      print(f"    Creating database: {catalog}.tpcdi_raw_data")
      spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.tpcdi_raw_data COMMENT 'Schema for TPC-DI Raw Files Volume'")
      print(f"    Creating volume: {catalog}.tpcdi_raw_data.tpcdi_volume")
      spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.tpcdi_raw_data.tpcdi_volume COMMENT 'TPC-DI Raw Files'")
      print("    ✓ Unity Catalog resources ready")
    
    print("\n  Scanning generated files...")
    filenames = [os.path.join(root, name) for root, dirs, files in os.walk(top=driver_out_path , topdown=True) for name in files]
    print(f"    Found {len(filenames)} file(s) to move")
    
    if len(filenames) == 0:
      print("    ⚠ WARNING: No files found in driver output path!")
      print(f"    Please check if DIGen generated data in: {driver_out_path}")
      if os.path.exists(driver_out_path):
        print(f"    Directory exists but is empty or contains only subdirectories")
        all_items = os.listdir(driver_out_path)
        print(f"    Items in directory: {all_items if all_items else 'none'}")
      else:
        print(f"    ⚠ ERROR: Directory does not exist: {driver_out_path}")
    
    print("\n  Creating target directories...")
    print(f"    Creating root: {blob_out_path}")
    dbutils.fs.mkdirs(blob_out_path)
    try:
      walk_result = next(os.walk(driver_out_path))
      subdirs = walk_result[1]
      if subdirs:
        print(f"    Found {len(subdirs)} subdirectory(ies): {subdirs}")
        for dir in subdirs:
          target_dir = f"{blob_out_path}/{dir}"
          print(f"      Creating: {target_dir}")
          dbutils.fs.mkdirs(target_dir)
      else:
        print(f"    No subdirectories found (files in root only)")
    except StopIteration:
      print(f"    ⚠ Warning: Could not enumerate directories in {driver_out_path}")
    if lighthouse: threads = 8
    else: threads = sc.defaultParallelism
    
    if len(filenames) > 0:
      print(f"\n  Starting parallel file transfer with {threads} threads...")
      with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        futures = []
        for filename in filenames:
          futures.append(executor.submit(move_file, source_location=filename, target_location=filename.replace(driver_out_path, os_blob_out_path)))
        
        completed_count = 0
        for future in concurrent.futures.as_completed(futures):
          try:
            result = future.result()
            completed_count += 1
            if completed_count % 10 == 0 or completed_count == len(filenames):
              print(f"    Progress: {completed_count}/{len(filenames)} files moved")
            # Print individual file moves for debugging (can be commented out if too verbose)
            # print(f"    {result}")
          except requests.ConnectTimeout:
            print("    ⚠ ConnectTimeout occurred during file transfer")
          except Exception as e:
            print(f"    ⚠ Error moving file: {e}")
      
      print(f"\n✓ File transfer complete: {completed_count}/{len(filenames)} files successfully moved")
      print("=" * 80)
      print(f"Data generation completed successfully!")
      print(f"Data is now available at: {blob_out_path}")
      print("=" * 80)
    else:
      print("\n✗ Skipping file transfer - no files to move")
      print("=" * 80)
      print("Data generation completed with warnings!")
      print("Please review the logs above for issues.")
      print("=" * 80)
        
def DIGen(digen_path, scale_factor, output_path):
  # Check Java version first
  try:
    java_version = subprocess.run(['java', '-version'], capture_output=True, text=True, timeout=5)
    print(f"  Java Version Check:")
    # Java version info goes to stderr
    version_output = java_version.stderr.split('\n')[0] if java_version.stderr else "Unknown"
    print(f"    {version_output}")
  except Exception as e:
    print(f"  ⚠ WARNING: Could not check Java version: {e}")
  
  # Check if DIGen.jar exists and is accessible
  digen_jar = f"{digen_path}DIGen.jar"
  if not os.path.exists(digen_jar):
    print(f"  ⚠ ERROR: DIGen.jar not found at: {digen_jar}")
    return
  
  print(f"  DIGen.jar size: {os.path.getsize(digen_jar)} bytes")
  
  # Try with increased heap memory which might help with jar loading
  cmd = f"java -Xmx2g -jar {digen_path}DIGen.jar -sf {scale_factor} -o {output_path}"
  print(f"  DIGen Command: {cmd}")
  print(f"  Working Directory: {digen_path}")
  print(f"  Output Path: {output_path}")
  print("\n  DIGen Output:")
  print("  " + "-" * 76)
  
  args = shlex.split(cmd)
  p3 = subprocess.Popen(
    args,
    cwd=digen_path,
    universal_newlines=True,
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE, 
    stderr=subprocess.PIPE)
  p3.stdin.write("\n")
  p3.stdin.flush()
  p3.stdin.write("YES\n")
  p3.stdin.flush()
  
  line_count = 0
  while True:
    output = p3.stdout.readline()
    if p3.poll() is not None and output == '':
      break
    if output:
      print(f"  {output.strip()}")
      line_count += 1
  
  # Check for any errors
  stderr_output = p3.stderr.read()
  if stderr_output:
    print("\n  ⚠ DIGen Errors/Warnings:")
    for line in stderr_output.split('\n'):
      if line.strip():
        print(f"    {line}")
    
    # Check for specific known errors
    if "NoClassDefFoundError" in stderr_output or "ClassNotFoundException" in stderr_output:
      print("\n  ⚠ DIAGNOSTIC: Java classpath/dependency issue detected!")
      print("    This usually means DIGen.jar is missing required libraries.")
      print("    Possible solutions:")
      print("    1. The DIGen.jar file may be corrupted - try re-downloading")
      print("    2. The jar may need to be rebuilt with all dependencies included")
      print("    3. Check if there's an alternative DIGen distribution (e.g., DIGen_Build.zip)")
  
  return_code = p3.wait()
  print("  " + "-" * 76)
  print(f"  DIGen Process Exit Code: {return_code}")
  print(f"  Output Lines: {line_count}")
  
  if return_code != 0:
    print(f"  ⚠ WARNING: DIGen exited with non-zero code: {return_code}")
    print(f"  ⚠ Data generation FAILED - no files will be created")
  else:
    print(f"  ✓ DIGen completed successfully")

# COMMAND ----------

generate_data()
