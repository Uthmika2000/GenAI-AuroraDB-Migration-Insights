"""
Use Case 1: Automated Data Collection & Normalization (Enhanced)
Captures comprehensive database metadata, performance metrics, configuration, and fragmentation
"""
import pyodbc
import json
from datetime import datetime
import os

class DatabaseInventory:
    def __init__(self, connection_string):
        self.conn = pyodbc.connect(connection_string)
        self.cursor = self.conn.cursor()
        
        if not os.path.exists('outputs'):
            os.makedirs('outputs')
            print(" Created 'outputs' folder")
        
    def collect_database_metadata(self):
        metadata = {
            'timestamp': datetime.now().isoformat(),
            'databases': []
        }
        
        self.cursor.execute("""
            SELECT 
                name, 
                database_id,
                create_date,
                compatibility_level,
                collation_name,
                user_access_desc,
                state_desc,
                recovery_model_desc
            FROM sys.databases
            WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb')
        """)
        
        for row in self.cursor.fetchall():
            dbname = row.name
            print(f"Processing database: {dbname}")
            db_info = {
                'name': dbname,
                'id': row.database_id,
                'created': str(row.create_date),
                'compatibility_level': row.compatibility_level,
                'collation': row.collation_name,
                'access': row.user_access_desc,
                'state': row.state_desc,
                'recovery_model': row.recovery_model_desc,
                'schemas': self.get_schemas(dbname),
                'size': self.get_database_size(dbname),
                'tables': self.get_table_info(dbname),
                'indexes': self.get_index_info(dbname),
                'stored_procedures': self.get_stored_procedure_info(dbname),
                'constraints': self.get_constraint_info(dbname),
                'fragmentation': self.get_fragmentation_info(dbname)  # NEW
            }
            metadata['databases'].append(db_info)
            
        return metadata
    
    def get_schemas(self, database_name):
        try:
            self.cursor.execute(f"""
                SELECT name 
                FROM [{database_name}].sys.schemas 
                WHERE name NOT IN ('sys', 'INFORMATION_SCHEMA')
            """)
            return [row.name for row in self.cursor.fetchall()]
        except Exception as e:
            print(f"  Warning: Could not get schemas for {database_name}: {e}")
            return []
    
    def get_database_size(self, database_name):
        try:
            self.cursor.execute(f"""
                SELECT 
                    SUM(size * 8.0 / 1024) as SizeMB,
                    SUM(CASE WHEN type_desc = 'ROWS' THEN size * 8.0 / 1024 END) as DataSizeMB,
                    SUM(CASE WHEN type_desc = 'LOG' THEN size * 8.0 / 1024 END) as LogSizeMB
                FROM sys.master_files
                WHERE database_id = DB_ID('{database_name}')
            """)
            row = self.cursor.fetchone()
            return {
                'total_mb': float(row.SizeMB) if row and row.SizeMB else 0.0,
                'data_mb': float(row.DataSizeMB) if row and row.DataSizeMB else 0.0,
                'log_mb': float(row.LogSizeMB) if row and row.LogSizeMB else 0.0
            }
        except Exception as e:
            print(f"  Warning: Could not get size for {database_name}: {e}")
            return {'total_mb': 0.0, 'data_mb': 0.0, 'log_mb': 0.0}
    
    def get_table_info(self, database_name):
        try:
            self.cursor.execute(f"""
                SELECT 
                    t.name as TableName,
                    s.name as SchemaName,
                    MAX(p.[rows]) as TableRowCount,
                    SUM(a.total_pages) * 8 / 1024.0 as TotalSpaceMB,
                    SUM(a.used_pages) * 8 / 1024.0 as UsedSpaceMB
                FROM [{database_name}].sys.tables t
                INNER JOIN [{database_name}].sys.schemas s ON t.schema_id = s.schema_id
                INNER JOIN [{database_name}].sys.indexes i ON t.object_id = i.object_id
                INNER JOIN [{database_name}].sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
                INNER JOIN [{database_name}].sys.allocation_units a ON p.partition_id = a.container_id
                WHERE i.index_id < 2
                GROUP BY t.name, s.name
                ORDER BY SUM(a.total_pages) DESC
            """)
            
            tables = []
            for row in self.cursor.fetchall():
                tables.append({
                    'name': row.TableName,
                    'schema': row.SchemaName,
                    'row_count': row.TableRowCount if row.TableRowCount else 0,
                    'total_space_mb': float(row.TotalSpaceMB) if row.TotalSpaceMB else 0.0,
                    'used_space_mb': float(row.UsedSpaceMB) if row.UsedSpaceMB else 0.0
                })
            return tables
        except Exception as e:
            print(f"  Warning: Could not get table info for {database_name}: {e}")
            return []

    def get_index_info(self, database_name):
        try:
            self.cursor.execute(f"""
                SELECT 
                    s.name as SchemaName,
                    t.name as TableName,
                    i.name as IndexName,
                    i.type_desc as IndexType,
                    i.is_unique,
                    i.is_primary_key,
                    STUFF((
                        SELECT ', ' + c.name
                        FROM [{database_name}].sys.index_columns ic
                        JOIN [{database_name}].sys.columns c 
                          ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                        WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id
                        ORDER BY ic.key_ordinal
                        FOR XML PATH('')
                    ), 1, 2, '') as IndexColumns
                FROM [{database_name}].sys.indexes i
                JOIN [{database_name}].sys.tables t ON i.object_id = t.object_id
                JOIN [{database_name}].sys.schemas s ON t.schema_id = s.schema_id
                WHERE i.name IS NOT NULL
            """)
            
            indexes = []
            for row in self.cursor.fetchall():
                indexes.append({
                    'schema': row.SchemaName,
                    'table': row.TableName,
                    'name': row.IndexName,
                    'type': row.IndexType,
                    'is_unique': bool(row.is_unique),
                    'is_primary_key': bool(row.is_primary_key),
                    'columns': row.IndexColumns if row.IndexColumns else ''
                })
            return indexes
        except Exception as e:
            print(f"  Warning: Could not get index info for {database_name}: {e}")
            return []
    
    def get_stored_procedure_info(self, database_name):
        try:
            self.cursor.execute(f"""
                SELECT 
                    s.name as SchemaName,
                    p.name as ProcedureName,
                    p.create_date,
                    p.modify_date
                FROM [{database_name}].sys.procedures p
                JOIN [{database_name}].sys.schemas s ON p.schema_id = s.schema_id
            """)
            
            procs = []
            for row in self.cursor.fetchall():
                procs.append({
                    'schema': row.SchemaName,
                    'name': row.ProcedureName,
                    'created': str(row.create_date),
                    'modified': str(row.modify_date)
                })
            return procs
        except Exception as e:
            print(f"  Warning: Could not get stored procedure info for {database_name}: {e}")
            return []
    
    def get_constraint_info(self, database_name):
        try:
            self.cursor.execute(f"""
                SELECT 
                    s.name as SchemaName,
                    t.name as TableName,
                    c.name as ConstraintName,
                    c.type_desc as ConstraintType
                FROM [{database_name}].sys.check_constraints c
                JOIN [{database_name}].sys.tables t ON c.parent_object_id = t.object_id
                JOIN [{database_name}].sys.schemas s ON t.schema_id = s.schema_id
                
                UNION ALL
                
                SELECT 
                    s.name,
                    t.name,
                    fk.name,
                    'FOREIGN_KEY'
                FROM [{database_name}].sys.foreign_keys fk
                JOIN [{database_name}].sys.tables t ON fk.parent_object_id = t.object_id
                JOIN [{database_name}].sys.schemas s ON t.schema_id = s.schema_id
            """)
            
            constraints = []
            for row in self.cursor.fetchall():
                constraints.append({
                    'schema': row.SchemaName,
                    'table': row.TableName,
                    'name': row.ConstraintName,
                    'type': row.ConstraintType
                })
            return constraints
        except Exception as e:
            print(f"  Warning: Could not get constraint info for {database_name}: {e}")
            return []
    
    def get_fragmentation_info(self, database_name):
        try:
            self.cursor.execute(f"""
                SELECT 
                    OBJECT_SCHEMA_NAME(ips.object_id, DB_ID('{database_name}')) AS SchemaName,
                    OBJECT_NAME(ips.object_id, DB_ID('{database_name}')) AS TableName,
                    i.name AS IndexName,
                    i.type_desc AS IndexType,
                    ips.index_id,
                    ROUND(ips.avg_fragmentation_in_percent, 2) AS avg_fragmentation_in_percent,
                    ips.fragment_count,
                    ips.page_count,
                    ROUND(ips.avg_page_space_used_in_percent, 2) AS avg_page_space_used_in_percent,
                    ips.record_count
                FROM sys.dm_db_index_physical_stats(
                    DB_ID('{database_name}'), 
                    NULL, 
                    NULL, 
                    NULL, 
                    'LIMITED'
                ) AS ips
                INNER JOIN [{database_name}].sys.indexes AS i 
                    ON ips.object_id = i.object_id 
                    AND ips.index_id = i.index_id
                WHERE ips.index_id > 0  -- Exclude heaps
                    AND OBJECT_NAME(ips.object_id, DB_ID('{database_name}')) IS NOT NULL
                ORDER BY ips.avg_fragmentation_in_percent DESC
            """)
            
            fragmentation_data = []
            high_frag_count = 0
            moderate_frag_count = 0
            
            for row in self.cursor.fetchall():
                frag_pct = float(row.avg_fragmentation_in_percent) if row.avg_fragmentation_in_percent else 0.0
                
                if frag_pct > 30:
                    frag_level = 'HIGH'
                    high_frag_count += 1
                elif frag_pct > 10:
                    frag_level = 'MODERATE'
                    moderate_frag_count += 1
                else:
                    frag_level = 'LOW'
                
                if frag_pct > 30:
                    recommendation = 'REBUILD index recommended'
                elif frag_pct > 10:
                    recommendation = 'REORGANIZE index recommended'
                else:
                    recommendation = 'No action needed'
                
                fragmentation_data.append({
                    'schema': row.SchemaName,
                    'table': row.TableName,
                    'index_name': row.IndexName,
                    'index_type': row.IndexType,
                    'fragmentation_percent': frag_pct,
                    'fragmentation_level': frag_level,
                    'fragment_count': row.fragment_count,
                    'page_count': row.page_count,
                    'avg_page_space_used_percent': float(row.avg_page_space_used_in_percent) if row.avg_page_space_used_in_percent else 0.0,
                    'record_count': row.record_count,
                    'recommendation': recommendation
                })
            
            return {
                'summary': {
                    'total_indexes': len(fragmentation_data),
                    'high_fragmentation': high_frag_count,
                    'moderate_fragmentation': moderate_frag_count,
                    'low_fragmentation': len(fragmentation_data) - high_frag_count - moderate_frag_count
                },
                'details': fragmentation_data
            }
            
        except Exception as e:
            print(f"  Warning: Could not get fragmentation info for {database_name}: {e}")
            return {'error': str(e)}
    
    def get_server_configurations(self):
        try:
            self.cursor.execute("""
                SELECT 
                    name,
                    CAST(value AS INT) as value,
                    CAST(value_in_use AS INT) as value_in_use,
                    description
                FROM sys.configurations
                ORDER BY name
            """)
            
            configs = []
            for row in self.cursor.fetchall():
                configs.append({
                    'name': row.name,
                    'value': row.value,
                    'value_in_use': row.value_in_use,
                    'description': row.description,
                    'requires_restart': row.value != row.value_in_use
                })
            
            return {
                'count': len(configs),
                'configurations': configs,
                'pending_restart': any(c['requires_restart'] for c in configs)
            }
            
        except Exception as e:
            print(f"  Warning: Could not get server configurations: {e}")
            return {'error': str(e)}
    
    def collect_performance_metrics(self):
        metrics = {}
        
        try:
            # CPU Usage
            self.cursor.execute("""
                SELECT TOP 1
                    SQLProcessUtilization AS SQL_CPU_Usage,
                    SystemIdle AS System_Idle,
                    100 - SystemIdle - SQLProcessUtilization AS Other_Process_CPU
                FROM (
                    SELECT 
                        record.value('(./Record/@id)[1]', 'int') AS record_id,
                        record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int') AS SystemIdle,
                        record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int') AS SQLProcessUtilization,
                        timestamp
                    FROM (
                        SELECT timestamp, CONVERT(xml, record) AS record 
                        FROM sys.dm_os_ring_buffers 
                        WHERE ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR' 
                        AND record LIKE '%<SystemHealth>%'
                    ) AS x
                ) AS y 
                ORDER BY record_id DESC
            """)
            row = self.cursor.fetchone()
            if row:
                metrics['cpu'] = {
                    'sql_cpu_usage': row.SQL_CPU_Usage,
                    'system_idle': row.System_Idle,
                    'other_cpu': row.Other_Process_CPU
                }
        except Exception as e:
            print(f"  Warning: Could not collect CPU metrics: {e}")
            metrics['cpu'] = {'error': str(e)}
        
        try:
            # Memory Usage
            self.cursor.execute("""
                SELECT 
                    (physical_memory_in_use_kb/1024) AS Memory_Used_MB,
                    (locked_page_allocations_kb/1024) AS Locked_Pages_MB,
                    (total_virtual_address_space_kb/1024) AS Total_VAS_MB,
                    (virtual_address_space_committed_kb/1024) AS VAS_Committed_MB,
                    (virtual_address_space_available_kb/1024) AS VAS_Available_MB
                FROM sys.dm_os_process_memory
            """)
            row = self.cursor.fetchone()
            if row:
                metrics['memory'] = {
                    'memory_used_mb': row.Memory_Used_MB,
                    'locked_pages_mb': row.Locked_Pages_MB,
                    'total_vas_mb': row.Total_VAS_MB,
                    'vas_committed_mb': row.VAS_Committed_MB,
                    'vas_available_mb': row.VAS_Available_MB
                }
        except Exception as e:
            print(f"  Warning: Could not collect memory metrics: {e}")
            metrics['memory'] = {'error': str(e)}
        
        try:
            # IO Statistics
            self.cursor.execute("""
                SELECT 
                    DB_NAME(database_id) AS DatabaseName,
                    SUM(num_of_reads) AS Total_Reads,
                    SUM(num_of_writes) AS Total_Writes,
                    SUM(num_of_bytes_read)/1024/1024 AS Total_MB_Read,
                    SUM(num_of_bytes_written)/1024/1024 AS Total_MB_Written
                FROM sys.dm_io_virtual_file_stats(NULL, NULL)
                GROUP BY database_id
                HAVING DB_NAME(database_id) IS NOT NULL
            """)
            
            io_stats = []
            for row in self.cursor.fetchall():
                io_stats.append({
                    'database': row.DatabaseName,
                    'total_reads': row.Total_Reads,
                    'total_writes': row.Total_Writes,
                    'total_mb_read': row.Total_MB_Read if row.Total_MB_Read else 0,
                    'total_mb_written': row.Total_MB_Written if row.Total_MB_Written else 0
                })
            metrics['io'] = io_stats
        except Exception as e:
            print(f"  Warning: Could not collect IO metrics: {e}")
            metrics['io'] = {'error': str(e)}
        
        return metrics
    
    def generate_inventory_report(self, output_file='outputs/sql_server_inventory.json'):
        print("Collecting server information...")
        server_info = self.get_server_info()
        
        print("Collecting server configurations...")
        server_configs = self.get_server_configurations()
        
        print("Collecting database metadata...")
        metadata = self.collect_database_metadata()
        
        print("Collecting performance metrics...")
        performance = self.collect_performance_metrics()
        
        report = {
            'server_info': server_info,
            'server_configurations': server_configs,
            'metadata': metadata,
            'performance': performance,
            'generated_at': datetime.now().isoformat()
        }
        
        with open(output_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        print(f"\n Inventory report generated: {output_file}")
        return report
    
    def get_server_info(self):
        try:
            self.cursor.execute("""
                SELECT 
                    CAST(SERVERPROPERTY('ServerName') AS NVARCHAR(128)) AS ServerName,
                    CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(128)) AS Version,
                    CAST(SERVERPROPERTY('ProductLevel') AS NVARCHAR(128)) AS ProductLevel,
                    CAST(SERVERPROPERTY('Edition') AS NVARCHAR(128)) AS Edition,
                    CAST(SERVERPROPERTY('EngineEdition') AS INT) AS EngineEdition
            """)
            row = self.cursor.fetchone()
            return {
                'server_name': row.ServerName,
                'version': row.Version,
                'product_level': row.ProductLevel,
                'edition': row.Edition,
                'engine_edition': row.EngineEdition
            }
        except Exception as e:
            print(f"  Warning: Could not get server info: {e}")
            return {'error': str(e)}
    
    def generate_summary(self, report, output_file):
        print("\n" + "="*60)
        print("DATABASE INVENTORY SUMMARY")
        print("="*60)
        
        # Server Info
        server = report.get('server_info', {})
        print(f"\nServer: {server.get('server_name', 'Unknown')}")
        print(f"Version: {server.get('version', 'Unknown')}")
        print(f"Edition: {server.get('edition', 'Unknown')}")
        
        # Configuration Summary
        configs = report.get('server_configurations', {})
        if 'configurations' in configs:
            print(f"\n  Server Configurations: {configs.get('count', 0)}")
            if configs.get('pending_restart'):
                print("     Warning: Some configuration changes require restart")
        
        databases = report.get('metadata', {}).get('databases', [])
        print(f"\n Total Databases: {len(databases)}")
        
        total_size = sum(db.get('size', {}).get('total_mb', 0) for db in databases)
        print(f" Total Storage: {total_size:.2f} MB ({total_size/1024:.2f} GB)")
        
        total_tables = sum(len(db.get('tables', [])) for db in databases)
        print(f" Total Tables: {total_tables}")
        
        total_indexes = sum(len(db.get('indexes', [])) for db in databases)
        print(f" Total Indexes: {total_indexes}")
        
        total_sps = sum(len(db.get('stored_procedures', [])) for db in databases)
        print(f"  Total Stored Procedures: {total_sps}")
        
        total_high_frag = sum(
            db.get('fragmentation', {}).get('summary', {}).get('high_fragmentation', 0) 
            for db in databases
        )
        total_mod_frag = sum(
            db.get('fragmentation', {}).get('summary', {}).get('moderate_fragmentation', 0) 
            for db in databases
        )
        
        if total_high_frag > 0 or total_mod_frag > 0:
            print(f"\n Fragmentation Status:")
            if total_high_frag > 0:
                print(f"    High fragmentation (>30%): {total_high_frag} indexes")
            if total_mod_frag > 0:
                print(f"    Moderate fragmentation (>10%): {total_mod_frag} indexes")
        
        perf = report.get('performance', {})
        cpu = perf.get('cpu', {})
        if 'sql_cpu_usage' in cpu:
            print(f"\n  SQL CPU Usage: {cpu['sql_cpu_usage']}%")
        
        memory = perf.get('memory', {})
        if 'memory_used_mb' in memory:
            print(f" Memory Used: {memory['memory_used_mb']:.2f} MB")
        
        if databases:
            print("\n Top 5 Largest Databases:")
            sorted_dbs = sorted(databases, 
                              key=lambda x: x.get('size', {}).get('total_mb', 0), 
                              reverse=True)
            for i, db in enumerate(sorted_dbs[:5], 1):
                size = db.get('size', {}).get('total_mb', 0)
                tables = len(db.get('tables', []))
                print(f"  {i}. {db['name']}: {size:.2f} MB ({tables} tables)")
        
        print("\n" + "="*60)
        print(f"Report saved to: {output_file}")
        print("="*60 + "\n")
    
    def close(self):
        self.cursor.close()
        self.conn.close()

if __name__ == "__main__":
    print("\n" + "="*60)
    print("SQL SERVER DATABASE INVENTORY COLLECTOR (ENHANCED)")
    print("="*60 + "\n")
    
    SQL_SERVER = os.getenv("SQL_SERVER", "127.0.0.1")
    SQL_DATABASE = os.getenv("SQL_DATABASE", "master")
    SQL_USERNAME = os.getenv("SQL_USERNAME", "sa")
    SQL_PASSWORD = os.getenv("SQL_PASSWORD", "Mysql_User123!")
    SQL_PORT = os.getenv("SQL_PORT", "1433")
    
    conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SQL_SERVER},{SQL_PORT};DATABASE={SQL_DATABASE};UID={SQL_USERNAME};PWD={SQL_PASSWORD}"
    
    try:
        print("Connecting to SQL Server...")
        inventory = DatabaseInventory(conn_str)
        print(" Connected successfully!\n")
        
        output_file = 'outputs/sql_server_inventory.json'
        report = inventory.generate_inventory_report(output_file)
        
        inventory.generate_summary(report, output_file)
        
        inventory.close()
        
        print("\n Next Steps:")
        print("1. Review outputs/sql_server_inventory.json")
        print("2. Check server_configurations section for all SQL Server settings")
        print("3. Review fragmentation details for each database")
        print("4. Use GitHub Copilot to analyze: 'Analyze sql_server_inventory.json and suggest Aurora PostgreSQL instance sizing'")
        print("5. Run migration readiness assessment (UC5)")
        
    except pyodbc.Error as e:
        print(f"\n Database Error: {e}")
        print("\nTroubleshooting:")
        print("1. Verify SQL Server is running")
        print("2. Check connection string (server name, instance)")
        print("3. Verify credentials (username/password)")
        print("4. Ensure SQL Server authentication is enabled")
    except Exception as e:
        print(f"\n Error: {e}")
        import traceback
        traceback.print_exc()