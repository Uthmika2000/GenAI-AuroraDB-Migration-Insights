"""
Use Case 7: Automated Script Generation for Source Settings
Analyzes SQL Server config and generates Aurora PostgreSQL equivalent scripts
No Aurora connection needed - just generates scripts from SQL Server config
"""

import pyodbc
import json
from typing import Dict, List, Any
import os
from dotenv import load_dotenv

load_dotenv()

class AutomatedScriptGenerator:
    def __init__(self):
        # Initialize with SQL Server connection only
        self.sqlserver_conn = self._connect_sqlserver()
        
        # Parameter mapping rules (SQL Server -> PostgreSQL)
        self.parameter_mappings = {
            'max degree of parallelism': {
                'pg_param': 'max_parallel_workers_per_gather',
                'converter': lambda x: min(int(x), 32),
                'notes': 'PostgreSQL uses parallel workers per gather'
            },
            'cost threshold for parallelism': {
                'pg_param': 'parallel_setup_cost',
                'converter': lambda x: int(x) * 10,
                'notes': 'PostgreSQL uses different cost model'
            },
            'max server memory (mb)': {
                'pg_param': 'shared_buffers',
                'converter': lambda x: f"{int(int(x) * 0.25)}MB",
                'notes': 'shared_buffers typically 25% of SQL Server max memory'
            },
            'recovery interval (min)': {
                'pg_param': 'checkpoint_timeout',
                'converter': lambda x: f"{int(x) * 60}s",
                'notes': 'Converted from minutes to seconds'
            },
        }
        
        # Collation mappings
        self.collation_mappings = {
            'SQL_Latin1_General_CP1_CI_AS': 'en_US.utf8',
            'Latin1_General_CI_AS': 'en_US.utf8',
            'SQL_Latin1_General_CP1_CS_AS': 'C',
        }
    
    def _connect_sqlserver(self):
        # Connect to SQL Server
        try:
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={os.getenv('SQL_SERVER')};"
                f"DATABASE={os.getenv('SQL_DATABASE')};"
                f"UID={os.getenv('SQL_USERNAME')};"
                f"PWD={os.getenv('SQL_PASSWORD')}"
            )
            conn = pyodbc.connect(conn_str)
            print(" Connected to SQL Server")
            return conn
        except Exception as e:
            print(f" SQL Server connection failed: {str(e)}")
            raise
    
    def extract_sqlserver_config(self) -> Dict[str, Any]:
        config = {}
        
        try:
            cursor = self.sqlserver_conn.cursor()
            
            print("  Extracting server configuration...")
            server_config_query = """
                SELECT 
                    CAST(name AS NVARCHAR(255)) AS param_name,
                    CAST(value AS BIGINT) AS value,
                    CAST(value_in_use AS BIGINT) AS value_in_use
                FROM sys.configurations
                WHERE CAST(value_in_use AS BIGINT) <> 0
                ORDER BY name;
            """
            cursor.execute(server_config_query)
            config['server_config'] = []
            for row in cursor.fetchall():
                config['server_config'].append({
                    'name': (row.param_name or '').lower(),
                    'value': int(row.value) if row.value is not None else 0,
                    'value_in_use': int(row.value_in_use) if row.value_in_use is not None else 0,
                })
            print(f"    ✓ Found {len(config['server_config'])} server parameters")
            
            # 2. Database options
            print("  Extracting database options...")
            db_name = os.getenv("SQL_DATABASE")
            db_options_query = f"""
                SELECT 
                    CAST(name AS NVARCHAR(255)) AS db_name,
                    CAST(recovery_model_desc AS NVARCHAR(60)) AS recovery_model,
                    CAST(collation_name AS NVARCHAR(255)) AS collation,
                    compatibility_level,
                    is_auto_close_on,
                    is_auto_shrink_on,
                    is_auto_create_stats_on,
                    is_auto_update_stats_on
                FROM sys.databases
                WHERE name = ?
            """
            cursor.execute(db_options_query, (db_name,))
            row = cursor.fetchone()
            if row:
                config['database_options'] = {
                    'name': row.db_name,
                    'recovery_model': row.recovery_model,
                    'collation': row.collation,
                    'compatibility_level': row.compatibility_level,
                    'auto_close': row.is_auto_close_on,
                    'auto_shrink': row.is_auto_shrink_on,
                    'auto_create_stats': row.is_auto_create_stats_on,
                    'auto_update_stats': row.is_auto_update_stats_on
                }
                print(f"    ✓ Database: {row.db_name}")
            
            # 3. Get database size
            print("  Calculating database size...")
            size_query = """
                SELECT 
                    SUM(size) * 8 / 1024 AS size_mb
                FROM sys.master_files
                WHERE database_id = DB_ID();
            """
            cursor.execute(size_query)
            size_row = cursor.fetchone()
            if size_row and size_row.size_mb is not None:
                config['database_size_mb'] = int(size_row.size_mb)
                print(f"    ✓ Size: {int(size_row.size_mb)} MB")
            else:
                config['database_size_mb'] = 0
            
            # 4. Linked servers
            print("  Checking for linked servers...")
            linked_query = """
                SELECT 
                    CAST(name AS NVARCHAR(255)) AS server_name,
                    CAST(ISNULL(product, 'Unknown') AS NVARCHAR(255)) AS product,
                    CAST(ISNULL(data_source, '') AS NVARCHAR(4000)) AS data_source
                FROM sys.servers
                WHERE is_linked = 1;
            """
            cursor.execute(linked_query)
            config['linked_servers'] = []
            for row in cursor.fetchall():
                config['linked_servers'].append({
                    'name': row.server_name,
                    'product': row.product,
                    'data_source': row.data_source
                })
            print(f"    Found {len(config['linked_servers'])} linked servers")
            
            # 5. User logins
            print("  Extracting security principals...")
            security_query = """
                SELECT 
                    CAST(name AS NVARCHAR(255)) AS login_name,
                    CAST(type_desc AS NVARCHAR(60)) AS login_type,
                    is_disabled
                FROM sys.server_principals
                WHERE type IN ('S', 'U')
                    AND name NOT LIKE '##%%'
                    AND name NOT LIKE 'NT %%'
                ORDER BY name;
            """
            cursor.execute(security_query)
            config['security'] = []
            for row in cursor.fetchall():
                config['security'].append({
                    'name': row.login_name,
                    'type': row.login_type,
                    'disabled': bool(row.is_disabled)
                })
            print(f"    Found {len(config['security'])} user principals")
            
            cursor.close()
            
        except Exception as e:
            print(f" Error extracting config: {str(e)}")
            config['error'] = str(e)
        
        return config
    
    def generate_parameter_script(self, server_config: List[Dict]) -> str:
        lines = [
            "-- Aurora PostgreSQL Parameter Group Configuration",
            "-- Generated from SQL Server configuration",
            "-- ",
            "-- INSTRUCTIONS:",
            "-- 1. Create custom DB parameter group in AWS RDS Console",
            "-- 2. Modify these parameters in the parameter group",
            "-- 3. Associate parameter group with your Aurora cluster",
            "-- 4. Reboot cluster to apply changes",
            "--",
            ""
        ]
        
        mapped_count = 0
        for config_item in server_config:
            param_name = config_item['name'].lower()
            value = config_item['value_in_use']
            
            if param_name in self.parameter_mappings:
                mapping = self.parameter_mappings[param_name]
                pg_param = mapping['pg_param']
                pg_value = mapping['converter'](value)
                notes = mapping['notes']
                
                lines.append(f"-- SQL Server: {param_name} = {value}")
                lines.append(f"-- Note: {notes}")
                lines.append(f"-- PostgreSQL equivalent:")
                lines.append(f"{pg_param} = {pg_value}")
                lines.append("")
                mapped_count += 1
        
        lines.append(f"-- Total mapped parameters: {mapped_count}")
        lines.append(f"-- Parameters without direct mapping: {len(server_config) - mapped_count}")
        
        return "\n".join(lines)
    
    def generate_database_script(self, db_options: Dict) -> str:
        db_name = db_options.get('name', 'mydb').lower().replace(' ', '_')
        collation = db_options.get('collation', 'SQL_Latin1_General_CP1_CI_AS')
        pg_collation = self.collation_mappings.get(collation, 'en_US.utf8')
        
        lines = [
            "-- Database Creation Script for Aurora PostgreSQL",
            "-- ",
            f"-- Source Database: {db_options.get('name')}",
            f"-- Source Collation: {collation}",
            f"-- Target Collation: {pg_collation}",
            "--",
            "",
            f"-- Create database",
            f"CREATE DATABASE {db_name}",
            f"    WITH",
            f"    OWNER = postgres",
            f"    ENCODING = 'UTF8'",
            f"    LC_COLLATE = '{pg_collation}'",
            f"    LC_CTYPE = '{pg_collation}'",
            f"    TABLESPACE = pg_default",
            f"    CONNECTION LIMIT = -1;",
            "",
            f"-- Connect to database",
            f"\\c {db_name}",
            "",
            "-- Enable recommended extensions",
            "CREATE EXTENSION IF NOT EXISTS pg_stat_statements;",
            "CREATE EXTENSION IF NOT EXISTS pg_trgm;",
            "CREATE EXTENSION IF NOT EXISTS btree_gin;",
            "",
            "-- Aurora PostgreSQL specific settings",
            "-- Note: Some parameters can only be set at cluster level",
            "",
        ]
        
        if db_options.get('auto_create_stats'):
            lines.append("-- Auto-create statistics: Enabled by default in PostgreSQL")
        if db_options.get('auto_update_stats'):
            lines.append("-- Auto-update statistics: Enabled by default in PostgreSQL")
        
        return "\n".join(lines)
    
    def generate_security_script(self, security_settings: List[Dict]) -> str:
        lines = [
            "-- Security Configuration for Aurora PostgreSQL",
            "-- User and role creation",
            "--",
            "-- IMPORTANT: ",
            "-- 1. Change all passwords (marked as CHANGEME)",
            "-- 2. Use AWS IAM authentication for better security",
            "-- 3. Windows authentication requires AWS Directory Service",
            "--",
            ""
        ]
        
        for user in security_settings:
            if user['disabled']:
                continue
            
            username = user['name'].lower().replace(' ', '_').replace('\\', '_')
            user_type = user['type']
            
            lines.append(f"-- SQL Server login: {user['name']} ({user_type})")
            lines.append(f"CREATE USER {username} WITH PASSWORD 'CHANGEME';")
            lines.append(f"GRANT CONNECT ON DATABASE CURRENT TO {username};")
            lines.append(f"-- TODO: Grant specific permissions based on role")
            lines.append("")
        
        return "\n".join(lines)
    
    def generate_fdw_script(self, linked_servers: List[Dict]) -> str:
        """Generate Foreign Data Wrapper script for linked servers"""
        if not linked_servers:
            return "-- No linked servers found\n-- No FDW configuration needed"
        
        lines = [
            "-- Foreign Data Wrapper Configuration",
            "-- PostgreSQL equivalent of SQL Server linked servers",
            "--",
            "-- INSTRUCTIONS:",
            "-- 1. Install postgres_fdw extension",
            "-- 2. Update server addresses and credentials",
            "-- 3. Test connectivity",
            "--",
            "",
            "-- Enable FDW extension",
            "CREATE EXTENSION IF NOT EXISTS postgres_fdw;",
            ""
        ]
        
        for server in linked_servers:
            server_name = server['name'].lower().replace(' ', '_')
            fdw_name = f"fdw_{server_name}"
            
            lines.append(f"-- Linked Server: {server['name']}")
            lines.append(f"-- Product: {server['product']}")
            lines.append(f"CREATE SERVER {fdw_name}")
            lines.append(f"    FOREIGN DATA WRAPPER postgres_fdw")
            lines.append(f"    OPTIONS (")
            lines.append(f"        host '{server['data_source'] or 'HOSTNAME'}',")
            lines.append(f"        port '5432',")
            lines.append(f"        dbname 'database_name'")
            lines.append(f"    );")
            lines.append("")
            lines.append(f"-- User mapping for {fdw_name}")
            lines.append(f"CREATE USER MAPPING FOR CURRENT_USER")
            lines.append(f"    SERVER {fdw_name}")
            lines.append(f"    OPTIONS (")
            lines.append(f"        user 'remote_user',")
            lines.append(f"        password 'remote_password'")
            lines.append(f"    );")
            lines.append("")
        
        return "\n".join(lines)
    
    def generate_migration_notes(self, config: Dict) -> str:
        """Generate migration notes"""
        lines = [
            "=" * 80,
            "MIGRATION NOTES AND MANUAL STEPS",
            "=" * 80,
            "",
            "1. PARAMETER CONFIGURATION:",
            "   - Review parameter_group.sql",
            "   - Create custom parameter group in AWS RDS Console",
            "   - Apply to Aurora cluster",
            "   - Reboot cluster if required",
            "",
            "2. DATABASE SETUP:",
            "   - Execute database_setup.sql on Aurora",
            "   - Verify collation behavior",
            "   - Test character encoding",
            "",
            "3. SECURITY:",
            "   - Update all passwords in security.sql",
            "   - Consider AWS IAM authentication",
            "   - Map Windows auth to LDAP/AD if needed",
            "",
        ]
        
        if config.get('linked_servers'):
            lines.extend([
                "4. LINKED SERVERS / FDW:",
                "    IMPORTANT: Linked servers detected!",
                f"   - {len(config['linked_servers'])} linked server(s) found",
                "   - Update connection strings in fdw_setup.sql",
                "   - Test network connectivity",
                "   - Foreign Data Wrappers have different performance",
                "",
            ])
        
        lines.extend([
            "5. NOT SUPPORTED IN AURORA POSTGRESQL:",
            "   - SQL Server Agent -> Use AWS Lambda + EventBridge",
            "   - SQL Profiler -> Use pg_stat_statements",
            "   - SSRS/SSIS -> Use AWS Glue or other ETL tools",
            "",
            "6. NEXT STEPS:",
            "   [ ] Review all generated scripts",
            "   [ ] Create Aurora cluster",
            "   [ ] Apply parameter group",
            "   [ ] Execute database_setup.sql",
            "   [ ] Execute security.sql (after updating passwords)",
            "   [ ] Use AWS SCT for schema conversion",
            "   [ ] Use AWS DMS for data migration",
            "   [ ] Test application compatibility",
            "",
        ])
        
        if config.get('database_size_mb'):
            size_gb = config['database_size_mb'] / 1024
            lines.append(f"DATABASE SIZE: {size_gb:.2f} GB")
            lines.append(f"Estimated migration time: {size_gb * 10:.0f} - {size_gb * 30:.0f} minutes")
            lines.append("")
        
        lines.extend([
            "=" * 80,
            "For questions, refer to:",
            "- AWS Aurora PostgreSQL docs: https://docs.aws.amazon.com/aurora/",
            "- SQL Server migration guide: https://docs.aws.amazon.com/dms/",
            "=" * 80,
        ])
        
        return "\n".join(lines)
    
    def generate_all_scripts(self, config: Dict[str, Any]) -> Dict[str, str]:
        """Generate all migration scripts"""
        scripts = {}
        
        scripts['parameter_group'] = self.generate_parameter_script(
            config.get('server_config', [])
        )
        
        scripts['database_setup'] = self.generate_database_script(
            config.get('database_options', {})
        )
        
        scripts['security'] = self.generate_security_script(
            config.get('security', [])
        )
        
        scripts['fdw_setup'] = self.generate_fdw_script(
            config.get('linked_servers', [])
        )
        
        scripts['migration_notes'] = self.generate_migration_notes(config)
        
        return scripts
    
    def save_scripts(self, scripts: Dict[str, str], output_dir: str = "./migration_scripts"):
        os.makedirs(output_dir, exist_ok=True)
        
        for script_name, content in scripts.items():
            ext = '.txt' if script_name == 'migration_notes' else '.sql'
            filepath = os.path.join(output_dir, f"{script_name}{ext}")
            
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
        
        # Create README
        readme = """# SQL Server to Aurora PostgreSQL Migration Scripts

## Generated Scripts

1. **parameter_group.sql** - Aurora parameter group settings
2. **database_setup.sql** - Database creation and initialization  
3. **security.sql** - User and role creation
4. **fdw_setup.sql** - Foreign data wrapper configuration
5. **migration_notes.txt** - Important notes and checklist

## How to Use

1. Read `migration_notes.txt` first
2. Create Aurora PostgreSQL cluster in AWS
3. Create custom parameter group with settings from `parameter_group.sql`
4. Execute `database_setup.sql`
5. Update passwords in `security.sql` and execute
6. If you have linked servers, configure `fdw_setup.sql`
7. Use AWS SCT for schema conversion
8. Use AWS DMS for data migration

## Important

 These scripts are templates - review and customize before use!
 Always test in non-production environment first!
 Update all passwords and connection strings!
"""
        
        with open(os.path.join(output_dir, 'README.md'), 'w', encoding='utf-8') as f:
            f.write(readme)
        
        print(f"\n All scripts saved to: {output_dir}/")
        print(f"  - {len(scripts)} script files")
        print(f"  - 1 README file")

def main():
    print("=" * 80)
    print("SQL Server to Aurora PostgreSQL - Script Generator")
    print("Rule-Based Conversion (No LLM, No Aurora Connection Needed)")
    print("=" * 80)
    
    try:
        # Initialize generator
        generator = AutomatedScriptGenerator()
        
        # Extract configuration
        print("\n[1/3] Extracting SQL Server configuration...")
        config = generator.extract_sqlserver_config()
        
        if 'error' in config:
            print(f"\n✗ Configuration extraction failed")
            return
        
        print(f"\n✓ Configuration extracted successfully")
        
        # Generate scripts
        print("\n[2/3] Generating Aurora PostgreSQL scripts...")
        scripts = generator.generate_all_scripts(config)
        print(f" Generated {len(scripts)} script files")
        
        # Save scripts
        print("\n[3/3] Saving scripts...")
        generator.save_scripts(scripts)
        
        # Display summary
        print("\n" + "=" * 80)
        print("SUMMARY")
        print("=" * 80)
        print(f"Database: {config.get('database_options', {}).get('name', 'N/A')}")
        print(f"Size: {config.get('database_size_mb', 0)} MB")
        print(f"Parameters: {len(config.get('server_config', []))}")
        print(f"Users: {len(config.get('security', []))}")
        print(f"Linked Servers: {len(config.get('linked_servers', []))}")
        
        print("\n" + "=" * 80)
        print(" Script generation completed!")
        print("\nNext steps:")
        print("1. Review scripts in ./migration_scripts/")
        print("2. Read migration_notes.txt carefully")
        print("3. Create Aurora cluster")
        print("4. Execute scripts in order")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
