"""
Use Case 9: Data Migration Script Generator
Generates AWS DMS task configurations, data validation scripts, and migration monitoring queries
"""

import pyodbc
import pandas as pd
import json
import os
import sys
import argparse
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class DataMigrationScriptGenerator:
    def __init__(self, connection_string: str):
        self.conn_str = connection_string
        self.connection = None
        self.output_dir = "data_migration_scripts"
        
    def connect(self):
        try:
            self.connection = pyodbc.connect(self.conn_str)
            print(" Connected to SQL Server successfully")
            return True
        except Exception as e:
            print(f" Connection failed: {e}")
            return False
    
    def create_output_directory(self):
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        print(f" Output directory created: {self.output_dir}/")

    def _use_database(self, database_name: str):
        cursor = self.connection.cursor()
        cursor.execute(f"USE [{database_name}]")
    
    def get_table_metadata(self, database_name: str) -> pd.DataFrame:
        self._use_database(database_name)

        query = """
        SELECT 
            SCHEMA_NAME(t.schema_id) AS schema_name,
            t.name AS table_name,
            t.object_id,
            p.rows AS row_count,
            CAST(SUM(a.total_pages) * 8.0 / 1024 AS DECIMAL(18,2)) AS size_mb,
            (SELECT COUNT(*) FROM sys.columns c WHERE c.object_id = t.object_id) AS column_count,
            (SELECT COUNT(*) FROM sys.indexes i WHERE i.object_id = t.object_id AND i.index_id > 0) AS index_count,
            (SELECT TOP 1 c.name 
             FROM sys.indexes i
             INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
             INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
             WHERE i.object_id = t.object_id AND i.is_primary_key = 1
             ORDER BY ic.key_ordinal) AS primary_key_column,
            CASE 
                WHEN EXISTS (SELECT 1 FROM sys.foreign_keys WHERE parent_object_id = t.object_id) THEN 1
                ELSE 0
            END AS has_foreign_keys,
            CASE 
                WHEN EXISTS (SELECT 1 FROM sys.foreign_keys WHERE referenced_object_id = t.object_id) THEN 1
                ELSE 0
            END AS is_referenced
        FROM sys.tables t
        INNER JOIN sys.indexes i ON t.object_id = i.object_id
        INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
        INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
        WHERE t.is_ms_shipped = 0
            AND i.index_id IN (0,1)
        GROUP BY 
            t.schema_id, t.name, t.object_id, p.rows
        ORDER BY p.rows DESC
        """
        tables_df = pd.read_sql(query, self.connection)

        tables_df["row_count"] = (
            pd.to_numeric(tables_df["row_count"], errors="coerce")
            .fillna(0)
            .astype(int)
        )
        tables_df["size_mb"] = (
            pd.to_numeric(tables_df["size_mb"], errors="coerce")
            .fillna(0)
            .astype(float)
        )

        return tables_df
    
    def get_table_row_count_queries(self, database_name: str) -> str:
        tables_df = self.get_table_metadata(database_name)
        
        scripts = []
        scripts.append("-- ========================================")
        scripts.append("-- Row Count Validation Queries")
        scripts.append(f"-- Database: {database_name}")
        scripts.append(f"-- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        scripts.append("-- ========================================")
        scripts.append("-- Execute these queries on BOTH source and target databases")
        scripts.append("-- Compare results to validate data migration")
        scripts.append("-- ========================================\n")
        
        scripts.append("-- SQL Server (Source) Queries:")
        scripts.append("-- ========================================\n")
        
        for _, row in tables_df.iterrows():
            scripts.append(
                f"-- Table: {row['schema_name']}.{row['table_name']} "
                f"(Expected: {row['row_count']:,} rows)"
            )
            scripts.append(
                f"SELECT '{row['schema_name']}.{row['table_name']}' AS TableName, "
                f"COUNT(*) AS RowCount FROM [{row['schema_name']}].[{row['table_name']}];"
            )
            scripts.append("")
        
        scripts.append("\n-- PostgreSQL (Target) Queries:")
        scripts.append("-- ========================================\n")
        
        for _, row in tables_df.iterrows():
            scripts.append(
                f"-- Table: {row['schema_name']}.{row['table_name']} "
                f"(Expected: {row['row_count']:,} rows)"
            )
            scripts.append(
                f"SELECT '{row['schema_name']}.{row['table_name']}' AS table_name, "
                f"COUNT(*) AS row_count FROM {row['schema_name']}.{row['table_name']};"
            )
            scripts.append("")
        
        return '\n'.join(scripts)
    
    def generate_checksum_validation_queries(self, database_name: str) -> str:
        tables_df = self.get_table_metadata(database_name)
        
        scripts = []
        scripts.append("-- ========================================")
        scripts.append("-- Checksum Validation Queries")
        scripts.append(f"-- Database: {database_name}")
        scripts.append(f"-- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        scripts.append("-- ========================================")
        scripts.append("-- Use checksums to validate data integrity")
        scripts.append("-- NOTE: Checksums may differ due to data type conversions")
        scripts.append("-- ========================================\n")
        
        scripts.append("-- SQL Server Checksum Queries:")
        scripts.append("-- ========================================\n")
        
        for _, row in tables_df.iterrows():
            if row["row_count"] > 0:
                scripts.append(f"-- Table: {row['schema_name']}.{row['table_name']}")
                scripts.append("SELECT ")
                scripts.append(
                    f"    '{row['schema_name']}.{row['table_name']}' AS TableName,"
                )
                scripts.append("    COUNT(*) AS RowCount,")
                scripts.append("    CHECKSUM_AGG(BINARY_CHECKSUM(*)) AS ChecksumValue")
                scripts.append(
                    f"FROM [{row['schema_name']}].[{row['table_name']}];"
                )
                scripts.append("")
        
        scripts.append("\n-- PostgreSQL MD5 Hash Validation:")
        scripts.append("-- ========================================")
        scripts.append("-- Note: Use application-level checksums for complex validation\n")
        
        for _, row in tables_df.iterrows():
            if row["row_count"] > 0:
                scripts.append(f"-- Table: {row['schema_name']}.{row['table_name']}")
                scripts.append("SELECT ")
                scripts.append(
                    f"    '{row['schema_name']}.{row['table_name']}' AS table_name,"
                )
                scripts.append("    COUNT(*) AS row_count")
                scripts.append(f"FROM {row['schema_name']}.{row['table_name']};")
                scripts.append("")
        
        return '\n'.join(scripts)
    
    def generate_table_selection_rules(self, database_name: str) -> dict:
        tables_df = self.get_table_metadata(database_name)
        
        selection_rules = {
            "rules": []
        }
        
        schemas = tables_df["schema_name"].unique()
        for schema in schemas:
            selection_rules["rules"].append({
                "rule-type": "selection",
                "rule-id": f"select-schema-{schema}",
                "rule-name": f"select-schema-{schema}",
                "object-locator": {
                    "schema-name": schema,
                    "table-name": "%"
                },
                "rule-action": "include"
            })
        
        selection_rules["rules"].append({
            "rule-type": "transformation",
            "rule-id": "lowercase-schemas",
            "rule-name": "lowercase-schemas",
            "rule-target": "schema",
            "object-locator": {
                "schema-name": "%"
            },
            "rule-action": "convert-lowercase"
        })
        
        selection_rules["rules"].append({
            "rule-type": "transformation",
            "rule-id": "lowercase-tables",
            "rule-name": "lowercase-tables",
            "rule-target": "table",
            "object-locator": {
                "schema-name": "%",
                "table-name": "%"
            },
            "rule-action": "convert-lowercase"
        })
        
        return selection_rules
    
    def generate_migration_order(self, database_name: str) -> list:
        self._use_database(database_name)

        fk_query = """
        SELECT DISTINCT
            SCHEMA_NAME(fk_parent.schema_id) AS child_schema,
            OBJECT_NAME(fk.parent_object_id) AS child_table,
            SCHEMA_NAME(fk_ref.schema_id) AS parent_schema,
            OBJECT_NAME(fk.referenced_object_id) AS parent_table
        FROM sys.foreign_keys fk
        INNER JOIN sys.tables fk_parent ON fk.parent_object_id = fk_parent.object_id
        INNER JOIN sys.tables fk_ref ON fk.referenced_object_id = fk_ref.object_id
        WHERE fk_parent.is_ms_shipped = 0
        """
        
        fk_df = pd.read_sql(fk_query, self.connection)
        tables_df = self.get_table_metadata(database_name)
        
        # Build dependency graph
        dependencies = {}
        for _, row in tables_df.iterrows():
            table_name = f"{row['schema_name']}.{row['table_name']}"
            dependencies[table_name] = {
                "depends_on": [],
                "row_count": row["row_count"],
                "size_mb": row["size_mb"]
            }
        
        for _, fk in fk_df.iterrows():
            child = f"{fk['child_schema']}.{fk['child_table']}"
            parent = f"{fk['parent_schema']}.{fk['parent_table']}"
            if child in dependencies and parent in dependencies and child != parent:
                dependencies[child]["depends_on"].append(parent)
        
        migration_waves = []
        processed = set()
        
        while len(processed) < len(dependencies):
            current_wave = []
            for table, info in dependencies.items():
                if table not in processed:
                    if all(dep in processed for dep in info["depends_on"]):
                        current_wave.append({
                            "table": table,
                            "row_count": info["row_count"],
                            "size_mb": info["size_mb"],
                            "dependencies": len(info["depends_on"])
                        })
            
            if not current_wave:
                for table, info in dependencies.items():
                    if table not in processed:
                        current_wave.append({
                            "table": table,
                            "row_count": info["row_count"],
                            "size_mb": info["size_mb"],
                            "dependencies": len(info["depends_on"]),
                            "note": "Circular dependency - disable FK constraints"
                        })
            
            migration_waves.append(current_wave)
            processed.update([t["table"] for t in current_wave])
        
        return migration_waves
    
    def generate_aws_dms_config(self, database_name: str) -> dict:
        tables_df = self.get_table_metadata(database_name)
        
        config = {
            "metadata": {
                "database": database_name,
                "generated_date": datetime.now().isoformat(),
                "total_tables": len(tables_df),
                "total_rows": int(tables_df["row_count"].sum()),
                "total_size_mb": float(tables_df["size_mb"].sum())
            },
            "task_settings": {
                "TargetMetadata": {
                    "TargetSchema": "",
                    "SupportLobs": True,
                    "FullLobMode": False,
                    "LobChunkSize": 64,
                    "LimitedSizeLobMode": True,
                    "LobMaxSize": 32,
                    "InlineLobMaxSize": 0,
                    "LoadMaxFileSize": 0,
                    "ParallelLoadThreads": 0,
                    "ParallelLoadBufferSize": 0,
                    "BatchApplyEnabled": True,
                    "TaskRecoveryTableEnabled": False
                },
                "FullLoadSettings": {
                    "TargetTablePrepMode": "DROP_AND_CREATE",
                    "CreatePkAfterFullLoad": False,
                    "StopTaskCachedChangesApplied": False,
                    "StopTaskCachedChangesNotApplied": False,
                    "MaxFullLoadSubTasks": 8,
                    "TransactionConsistencyTimeout": 600,
                    "CommitRate": 10000
                },
                "Logging": {
                    "EnableLogging": True,
                    "LogComponents": [
                        {
                            "Id": "SOURCE_UNLOAD",
                            "Severity": "LOGGER_SEVERITY_DEFAULT"
                        },
                        {
                            "Id": "TARGET_LOAD",
                            "Severity": "LOGGER_SEVERITY_INFO"
                        },
                        {
                            "Id": "SOURCE_CAPTURE",
                            "Severity": "LOGGER_SEVERITY_INFO"
                        },
                        {
                            "Id": "TARGET_APPLY",
                            "Severity": "LOGGER_SEVERITY_INFO"
                        }
                    ]
                },
                "ControlTablesSettings": {
                    "ControlSchema": "dms_control",
                    "HistoryTimeslotInMinutes": 5,
                    "HistoryTableEnabled": True,
                    "SuspendedTablesTableEnabled": True,
                    "StatusTableEnabled": True
                },
                "ChangeProcessingDdlHandlingPolicy": {
                    "HandleSourceTableDropped": True,
                    "HandleSourceTableTruncated": True,
                    "HandleSourceTableAltered": True
                },
                "ChangeProcessingTuning": {
                    "BatchApplyPreserveTransaction": True,
                    "BatchApplyTimeoutMin": 1,
                    "BatchApplyTimeoutMax": 30,
                    "BatchApplyMemoryLimit": 500,
                    "BatchSplitSize": 0,
                    "MinTransactionSize": 1000,
                    "CommitTimeout": 1,
                    "MemoryLimitTotal": 1024,
                    "MemoryKeepTime": 60,
                    "StatementCacheSize": 50
                },
                "ValidationSettings": {
                    "EnableValidation": True,
                    "ValidationMode": "ROW_LEVEL",
                    "ThreadCount": 5,
                    "PartitionSize": 10000,
                    "FailureMaxCount": 10000,
                    "RecordFailureDelayInMinutes": 5,
                    "RecordSuspendDelayInMinutes": 30,
                    "MaxKeyColumnSize": 8096,
                    "TableFailureMaxCount": 1000,
                    "ValidationOnly": False,
                    "HandleCollationDiff": False,
                    "RecordFailureDelayLimitInMinutes": 0,
                    "SkipLobColumns": False,
                    "ValidationPartialLobSize": 0,
                    "ValidationQueryCdcDelaySeconds": 0
                }
            },
            "table_selection_rules": self.generate_table_selection_rules(database_name)
        }
        
        return config
    
    def generate_pre_migration_scripts(self, database_name: str) -> str:
        scripts = []
        scripts.append("-- ========================================")
        scripts.append("-- Pre-Migration Preparation Scripts")
        scripts.append(f"-- Database: {database_name}")
        scripts.append(f"-- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        scripts.append("-- ========================================\n")
        
        scripts.append("-- Step 1: Backup SQL Server Database")
        scripts.append("-- ========================================")
        scripts.append(f"BACKUP DATABASE [{database_name}]")
        scripts.append(
            f"TO DISK = 'C:\\Backup\\{database_name}_PreMigration_{datetime.now().strftime('%Y%m%d')}.bak'"
        )
        scripts.append("WITH FORMAT, COMPRESSION, STATS = 10;")
        scripts.append("GO\n")
        
        scripts.append("-- Step 2: Disable Foreign Key Constraints (if needed)")
        scripts.append("-- ========================================")
        scripts.append(f"USE [{database_name}];")
        scripts.append("GO\n")
        scripts.append("-- Generate disable FK scripts")
        scripts.append("SELECT 'ALTER TABLE [' + OBJECT_SCHEMA_NAME(parent_object_id) + '].[' + ")
        scripts.append("       OBJECT_NAME(parent_object_id) + '] NOCHECK CONSTRAINT [' + name + '];'")
        scripts.append("FROM sys.foreign_keys")
        scripts.append("WHERE parent_object_id IN (SELECT object_id FROM sys.tables WHERE is_ms_shipped = 0);")
        scripts.append("GO\n")
        
        scripts.append("-- Step 3: Disable Triggers (if needed)")
        scripts.append("-- ========================================")
        scripts.append("SELECT 'DISABLE TRIGGER [' + t.name + '] ON [' + ")
        scripts.append("       OBJECT_SCHEMA_NAME(t.parent_id) + '].[' + OBJECT_NAME(t.parent_id) + '];'")
        scripts.append("FROM sys.triggers t")
        scripts.append("WHERE t.is_ms_shipped = 0 AND t.parent_id > 0;")
        scripts.append("GO\n")
        
        scripts.append("-- Step 4: Update Statistics")
        scripts.append("-- ========================================")
        scripts.append(f"USE [{database_name}];")
        scripts.append("EXEC sp_updatestats;")
        scripts.append("GO\n")
        
        scripts.append("-- Step 5: Rebuild Fragmented Indexes")
        scripts.append("-- ========================================")
        scripts.append("-- Refer to index_maintenance_scripts.sql for detailed rebuild scripts\n")
        
        return '\n'.join(scripts)
    
    def generate_post_migration_scripts(self, database_name: str) -> str:
        scripts = []
        scripts.append("-- ========================================")
        scripts.append("-- Post-Migration Validation & Optimization Scripts")
        scripts.append(f"-- Database: {database_name}")
        scripts.append(f"-- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        scripts.append("-- ========================================")
        scripts.append("-- Execute these on PostgreSQL (Aurora) after migration")
        scripts.append("-- ========================================\n")
        
        scripts.append("-- Step 1: Validate Row Counts")
        scripts.append("-- ========================================")
        scripts.append("-- Run row count validation queries (see row_count_validation.sql)")
        scripts.append("-- Compare with source database\n")
        
        scripts.append("-- Step 2: Analyze Tables (PostgreSQL Statistics)")
        scripts.append("-- ========================================")
        scripts.append("-- Update PostgreSQL statistics for query optimizer")
        scripts.append("VACUUM ANALYZE;\n")
        
        scripts.append("-- Or analyze specific schema:")
        scripts.append("-- VACUUM ANALYZE schema_name.table_name;\n")
        
        scripts.append("-- Step 3: Create Missing Indexes")
        scripts.append("-- ========================================")
        scripts.append("-- Refer to missing_index_recommendations.sql\n")
        
        scripts.append("-- Step 4: Enable Constraints")
        scripts.append("-- ========================================")
        scripts.append("-- All constraints should be enabled by DMS")
        scripts.append("-- Verify with:")
        scripts.append("SELECT conrelid::regclass AS table_name,")
        scripts.append("       conname AS constraint_name,")
        scripts.append("       contype AS constraint_type,")
        scripts.append("       convalidated")
        scripts.append("FROM pg_constraint")
        scripts.append("WHERE connamespace = 'your_schema'::regnamespace")
        scripts.append("ORDER BY conrelid, conname;\n")
        
        scripts.append("-- Step 5: Performance Tuning")
        scripts.append("-- ========================================")
        scripts.append("-- Monitor slow queries")
        scripts.append("SELECT pid, usename, datname, state,")
        scripts.append("       query, query_start, state_change")
        scripts.append("FROM pg_stat_activity")
        scripts.append("WHERE state != 'idle'")
        scripts.append("ORDER BY query_start;")
        scripts.append("")
        scripts.append("-- Check table bloat")
        scripts.append("SELECT schemaname, tablename,")
        scripts.append("       pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size")
        scripts.append("FROM pg_tables")
        scripts.append("WHERE schemaname NOT IN ('pg_catalog', 'information_schema')")
        scripts.append("ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC")
        scripts.append("LIMIT 20;\n")
        
        scripts.append("-- Step 6: Application Connection String Update")
        scripts.append("-- ========================================")
        scripts.append("-- Update application connection strings to point to Aurora PostgreSQL")
        scripts.append("-- Old (SQL Server): Server=sqlserver.example.com;Database=dbname;...")
        scripts.append("-- New (PostgreSQL): Host=aurora-cluster.region.rds.amazonaws.com;Database=dbname;...\n")
        
        return '\n'.join(scripts)
    
    def generate_all_migration_scripts(self, database_name: str):
        print(f"\n Generating data migration scripts for: {database_name}")
        
        self.create_output_directory()
        
        print("\nGenerating migration scripts...")
        
        print("  • Row count validation queries...")
        row_count_script = self.get_table_row_count_queries(database_name)
        with open(f"{self.output_dir}/row_count_validation.sql", "w", encoding="utf-8") as f:
            f.write(row_count_script)
        
        print("  • Checksum validation queries...")
        checksum_script = self.generate_checksum_validation_queries(database_name)
        with open(f"{self.output_dir}/checksum_validation.sql", "w", encoding="utf-8") as f:
            f.write(checksum_script)

        print("  • AWS DMS configuration...")
        dms_config = self.generate_aws_dms_config(database_name)
        with open(f"{self.output_dir}/aws_dms_config.json", "w", encoding="utf-8") as f:
            json.dump(dms_config, f, indent=2, default=str)
        
        print("  • Table migration order...")
        migration_order = self.generate_migration_order(database_name)
        migration_order_doc = {
            "database": database_name,
            "generated_date": datetime.now().isoformat(),
            "total_waves": len(migration_order),
            "waves": [
                {
                    "wave_number": i + 1,
                    "table_count": len(wave),
                    "tables": wave
                }
                for i, wave in enumerate(migration_order)
            ]
        }
        with open(f"{self.output_dir}/table_migration_order.json", "w", encoding="utf-8") as f:
            json.dump(migration_order_doc, f, indent=2, default=str)
        
        print("  • Pre-migration preparation scripts...")
        pre_migration = self.generate_pre_migration_scripts(database_name)
        with open(f"{self.output_dir}/pre_migration_scripts.sql", "w", encoding="utf-8") as f:
            f.write(pre_migration)
        
        print("  • Post-migration validation scripts...")
        post_migration = self.generate_post_migration_scripts(database_name)
        with open(f"{self.output_dir}/post_migration_scripts.sql", "w", encoding="utf-8") as f:
            f.write(post_migration)
        
        print("  • Migration summary report...")
        tables_df = self.get_table_metadata(database_name)
        summary = {
            "database": database_name,
            "generated_date": datetime.now().isoformat(),
            "statistics": {
                "total_tables": len(tables_df),
                "total_rows": int(tables_df["row_count"].sum()),
                "total_size_mb": float(tables_df["size_mb"].sum()),
                "tables_with_fks": int(tables_df["has_foreign_keys"].sum()),
                "referenced_tables": int(tables_df["is_referenced"].sum()),
                "migration_waves": len(migration_order)
            },
            "largest_tables": tables_df.nlargest(10, "row_count")[
                ["schema_name", "table_name", "row_count", "size_mb"]
            ].to_dict("records"),
            "recommendations": [
                "Review and test all generated scripts before production use",
                "Execute pre-migration scripts to prepare source database",
                "Use AWS DMS for incremental data migration with CDC",
                "Validate data integrity using row count and checksum queries",
                "Execute post-migration scripts to optimize PostgreSQL",
                "Monitor application performance after cutover",
                "Keep source database as backup for 30 days minimum"
            ]
        }
        
        with open(f"{self.output_dir}/migration_summary.json", "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, default=str)
        
        print("\n All migration scripts generated successfully!")
        
        # Print summary
        print("\n" + "="*70)
        print("DATA MIGRATION SCRIPTS SUMMARY")
        print("="*70)
        print(f"Database: {database_name}")
        print(f"Total Tables: {summary['statistics']['total_tables']}")
        print(f"Total Rows: {summary['statistics']['total_rows']:,}")
        print(f"Total Size: {summary['statistics']['total_size_mb']:.2f} MB")
        print(f"Migration Waves: {summary['statistics']['migration_waves']}")
        print(f"\nGenerated Files in {self.output_dir}/:")
        print("  • row_count_validation.sql")
        print("  • checksum_validation.sql")
        print("  • aws_dms_config.json")
        print("  • table_migration_order.json")
        print("  • pre_migration_scripts.sql")
        print("  • post_migration_scripts.sql")
        print("  • migration_summary.json")
        
        return summary
    
    def close(self):
        if self.connection:
            self.connection.close()
            print("\n Connection closed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Data Migration Script Generator for SQL Server to PostgreSQL"
    )
    parser.add_argument("--database", type=str, help="Target database name to analyze")
    parser.add_argument("--server", type=str, help="SQL Server address (overrides env)")
    parser.add_argument("--port", type=str, help="SQL Server port (overrides env)")
    parser.add_argument("--username", type=str, help="SQL Server username (overrides env)")
    parser.add_argument("--password", type=str, help="SQL Server password (overrides env)")
    
    args = parser.parse_args()
    
    SQL_SERVER = args.server or os.getenv("SQL_SERVER", "localhost")
    SQL_PORT = args.port or os.getenv("SQL_PORT", "1433")
    SQL_DATABASE = os.getenv("SQL_DATABASE", "master")
    SQL_USERNAME = args.username or os.getenv("SQL_USERNAME", "sa")
    SQL_PASSWORD = args.password or os.getenv("SQL_PASSWORD", "")
    

    database_name = args.database or SQL_DATABASE
    
    conn_string = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SQL_SERVER},{SQL_PORT};"
        f"DATABASE={SQL_DATABASE};"
        f"UID={SQL_USERNAME};"
        f"PWD={SQL_PASSWORD}"
    )
    
    print("="*70)
    print("DATA MIGRATION SCRIPT GENERATOR")
    print("="*70)
    print(f"Server: {SQL_SERVER}:{SQL_PORT}")
    print(f"Target Database: {database_name}")
    print("="*70)
    
    generator = DataMigrationScriptGenerator(conn_string)
    
    if generator.connect():
        try:
            summary = generator.generate_all_migration_scripts(database_name)
            print("\n Script generation completed successfully!")
        except Exception as e:
            print(f"\n Error during script generation: {e}")
            import traceback
            traceback.print_exc()
        finally:
            generator.close()
    else:
        print("\n Failed to connect to SQL Server")
        print("\nTroubleshooting:")
        print("  1. Check your .env file or environment variables")
        print("  2. Verify SQL Server is running and accessible")
        print("  3. Confirm username and password are correct")
        print("  4. Ensure ODBC Driver 17 for SQL Server is installed")
        sys.exit(1)