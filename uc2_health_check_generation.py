"""
Use Case 2: Intelligent Health Check Report Generation
Analyzes database health metrics and uses GenAI for recommendations
"""
import pyodbc
import json
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class HealthCheckAnalyzer:
    def __init__(self, connection_string):
        self.conn = pyodbc.connect(connection_string)
        self.cursor = self.conn.cursor()
        self.metrics = {}
        
    def collect_cpu_metrics(self):
        # Collect CPU utilization metrics
        self.cursor.execute("""
            SELECT TOP 10
                record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int') AS SQL_CPU_Usage,
                record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int') AS System_Idle,
                DATEADD(ms, -1 * ((SELECT ms_ticks FROM sys.dm_os_sys_info) - [timestamp]), GETDATE()) AS Event_Time
            FROM (
                SELECT timestamp, CONVERT(xml, record) AS record 
                FROM sys.dm_os_ring_buffers 
                WHERE ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR' 
                AND record LIKE '%<SystemHealth>%'
            ) AS x
            ORDER BY Event_Time DESC
        """)
        
        cpu_data = []
        for row in self.cursor.fetchall():
            cpu_data.append({
                'sql_cpu': row.SQL_CPU_Usage,
                'system_idle': row.System_Idle,
                'timestamp': str(row.Event_Time)
            })
        
        avg_cpu = sum(d['sql_cpu'] for d in cpu_data) / len(cpu_data) if cpu_data else 0
        
        self.metrics['cpu'] = {
            'current': cpu_data[0] if cpu_data else None,
            'average': avg_cpu,
            'status': 'CRITICAL' if avg_cpu > 90 else 'WARNING' if avg_cpu > 70 else 'OK',
            'history': cpu_data
        }
        
    def collect_memory_metrics(self):
        # Collect memory pressure indicators
        self.cursor.execute("""
            SELECT 
                (physical_memory_in_use_kb/1024.0) AS Memory_Used_MB,
                (large_page_allocations_kb/1024.0) AS Large_Page_MB,
                (locked_page_allocations_kb/1024.0) AS Locked_Pages_MB,
                (virtual_address_space_committed_kb/1024.0) AS VAS_Committed_MB,
                process_physical_memory_low,
                process_virtual_memory_low
            FROM sys.dm_os_process_memory
        """)
        
        row = self.cursor.fetchone()
        memory_used = row.Memory_Used_MB
        
        self.cursor.execute("""
            SELECT 
                (COUNT(*) * 8.0 / 1024) AS Buffer_Pool_MB,
                SUM(CASE WHEN is_modified = 1 THEN 1 ELSE 0 END) * 8.0 / 1024 AS Dirty_Pages_MB
            FROM sys.dm_os_buffer_descriptors
        """)
        
        buffer_row = self.cursor.fetchone()
        
        self.metrics['memory'] = {
            'used_mb': memory_used,
            'large_page_mb': row.Large_Page_MB,
            'locked_pages_mb': row.Locked_Pages_MB,
            'buffer_pool_mb': buffer_row.Buffer_Pool_MB,
            'dirty_pages_mb': buffer_row.Dirty_Pages_MB,
            'low_memory_signal': bool(row.process_physical_memory_low),
            'status': 'CRITICAL' if row.process_physical_memory_low else 'OK'
        }
        
    def collect_query_performance(self):
        # Collect slow query statistics
        self.cursor.execute("""
            SELECT TOP 20
                qs.execution_count,
                qs.total_elapsed_time / 1000.0 / qs.execution_count AS avg_elapsed_ms,
                qs.total_worker_time / 1000.0 / qs.execution_count AS avg_cpu_ms,
                qs.total_logical_reads / qs.execution_count AS avg_logical_reads,
                qs.total_logical_writes / qs.execution_count AS avg_logical_writes,
                SUBSTRING(qt.text, (qs.statement_start_offset/2)+1,
                    ((CASE qs.statement_end_offset
                        WHEN -1 THEN DATALENGTH(qt.text)
                        ELSE qs.statement_end_offset
                    END - qs.statement_start_offset)/2) + 1) AS query_text
            FROM sys.dm_exec_query_stats qs
            CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) qt
            ORDER BY qs.total_elapsed_time / qs.execution_count DESC
        """)
        
        slow_queries = []
        for row in self.cursor.fetchall():
            if row.avg_elapsed_ms > 1000:
                slow_queries.append({
                    'execution_count': row.execution_count,
                    'avg_elapsed_ms': row.avg_elapsed_ms,
                    'avg_cpu_ms': row.avg_cpu_ms,
                    'avg_logical_reads': row.avg_logical_reads,
                    'avg_logical_writes': row.avg_logical_writes,
                    'query_text': row.query_text[:200]
                })
        
        self.metrics['query_performance'] = {
            'slow_queries_count': len(slow_queries),
            'top_slow_queries': slow_queries[:10],
            'status': 'WARNING' if len(slow_queries) > 5 else 'OK'
        }
        
    def collect_replication_metrics(self):
        """Collect replication lag if applicable"""
        try:
            self.cursor.execute("""
                SELECT 
                    name AS database_name,
                    log_reuse_wait_desc,
                    CASE 
                        WHEN log_reuse_wait_desc IN ('REPLICATION', 'AVAILABILITY_REPLICA') 
                        THEN 'REPLICATION_ACTIVE'
                        ELSE 'NO_REPLICATION'
                    END AS replication_status
                FROM sys.databases
                WHERE database_id > 4
            """)
            
            repl_status = []
            for row in self.cursor.fetchall():
                repl_status.append({
                    'database': row.database_name,
                    'log_reuse_wait': row.log_reuse_wait_desc,
                    'status': row.replication_status
                })
            
            self.metrics['replication'] = {
                'databases': repl_status,
                'has_replication': any(db['status'] == 'REPLICATION_ACTIVE' for db in repl_status)
            }
        except Exception as e:
            self.metrics['replication'] = {'error': str(e)}
            
    def collect_io_statistics(self):
        # Collect I/O performance metrics
        self.cursor.execute("""
            SELECT 
                DB_NAME(database_id) AS database_name,
                file_id,
                io_stall_read_ms,
                num_of_reads,
                CASE WHEN num_of_reads = 0 THEN 0 
                    ELSE io_stall_read_ms / num_of_reads 
                END AS avg_read_latency_ms,
                io_stall_write_ms,
                num_of_writes,
                CASE WHEN num_of_writes = 0 THEN 0 
                    ELSE io_stall_write_ms / num_of_writes 
                END AS avg_write_latency_ms
            FROM sys.dm_io_virtual_file_stats(NULL, NULL)
            WHERE database_id > 4
            ORDER BY avg_read_latency_ms DESC
        """)
        
        io_stats = []
        high_latency_count = 0
        
        for row in self.cursor.fetchall():
            if row.avg_read_latency_ms > 20 or row.avg_write_latency_ms > 20:
                high_latency_count += 1
            
            io_stats.append({
                'database': row.database_name,
                'file_id': row.file_id,
                'avg_read_latency_ms': row.avg_read_latency_ms,
                'avg_write_latency_ms': row.avg_write_latency_ms,
                'total_reads': row.num_of_reads,
                'total_writes': row.num_of_writes
            })
        
        self.metrics['io'] = {
            'high_latency_files': high_latency_count,
            'file_stats': io_stats[:20],
            'status': 'WARNING' if high_latency_count > 0 else 'OK'
        }
        
    def generate_health_report(self):
        # Generate health report
        self.collect_cpu_metrics()
        self.collect_memory_metrics()
        self.collect_query_performance()
        self.collect_replication_metrics()
        self.collect_io_statistics()
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'metrics': self.metrics,
            'overall_status': self.calculate_overall_status(),
            'recommendations': self.generate_recommendations()
        }
        return report
    
    def calculate_overall_status(self):
        statuses = [d['status'] for d in self.metrics.values() if isinstance(d, dict) and 'status' in d]
        if 'CRITICAL' in statuses:
            return 'CRITICAL'
        elif 'WARNING' in statuses:
            return 'WARNING'
        return 'OK'
    
    def generate_recommendations(self):
        recs = []
        if self.metrics.get('cpu', {}).get('status') == 'CRITICAL':
            recs.append({
                'category': 'CPU', 'severity': 'HIGH',
                'issue': f"High CPU utilization: {self.metrics['cpu']['average']:.1f}%",
                'recommendation': "Optimize queries, add indexes, or scale up compute.",
                'migration_note': "Aurora PostgreSQL auto-scaling handles CPU spikes efficiently."
            })
        if self.metrics.get('memory', {}).get('low_memory_signal'):
            recs.append({
                'category': 'Memory', 'severity': 'HIGH',
                'issue': "Low memory condition detected",
                'recommendation': "Review buffer pool and increase RAM.",
                'migration_note': "Aurora PostgreSQL memory management differs; tune accordingly."
            })
        slow = self.metrics.get('query_performance', {}).get('slow_queries_count', 0)
        if slow > 5:
            recs.append({
                'category': 'Query Performance', 'severity': 'MEDIUM',
                'issue': f"{slow} queries with avg exec time >1s",
                'recommendation': "Optimize slow queries or add missing indexes.",
                'migration_note': "Use PostgreSQL EXPLAIN ANALYZE for tuning post-migration."
            })
        if self.metrics.get('io', {}).get('high_latency_files', 0) > 0:
            recs.append({
                'category': 'I/O Performance', 'severity': 'MEDIUM',
                'issue': "High I/O latency on some files",
                'recommendation': "Use SSD storage or optimize I/O.",
                'migration_note': "Aurora uses SSD-backed distributed storage."
            })
        return recs
    
    def export_report(self, filename='health_report.json'):
        report = self.generate_health_report()
        os.makedirs("outputs", exist_ok=True)
        filepath = os.path.join("outputs", filename)
        with open(filepath, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        print(f"Health report exported to {filepath}")
        return report
    
    def generate_markdown_report(self, filename='health_report.md'):
        report = self.generate_health_report()
        md_content = f"""# Database Health Check Report

**Generated:** {report['timestamp']}  
**Overall Status:** {report['overall_status']}

## Summary
- CPU Status: {self.metrics.get('cpu', {}).get('status', 'N/A')}
- Memory Status: {self.metrics.get('memory', {}).get('status', 'N/A')}
- Query Performance: {self.metrics.get('query_performance', {}).get('status', 'N/A')}
- I/O Performance: {self.metrics.get('io', {}).get('status', 'N/A')}

## Recommendations
"""
        for rec in report['recommendations']:
            md_content += f"""### {rec['category']} - {rec['severity']}
**Issue:** {rec['issue']}  
**Recommendation:** {rec['recommendation']}  
**Migration Note:** {rec['migration_note']}

"""
        os.makedirs("outputs", exist_ok=True)
        filepath = os.path.join("outputs", filename)
        with open(filepath, 'w') as f:
            f.write(md_content)
        print(f"Markdown report generated: {filepath}")
        return md_content
    
    def close(self):
        self.cursor.close()
        self.conn.close()



if __name__ == "__main__":
    SQL_SERVER = os.getenv("SQL_SERVER", "localhost")
    SQL_DATABASE = os.getenv("SQL_DATABASE", "master")
    SQL_USERNAME = os.getenv("SQL_USERNAME", "sa")
    SQL_PASSWORD = os.getenv("SQL_PASSWORD", "")
    SQL_PORT = os.getenv("SQL_PORT", "1433")

    conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SQL_SERVER},{SQL_PORT};DATABASE={SQL_DATABASE};UID={SQL_USERNAME};PWD={SQL_PASSWORD}"

    analyzer = HealthCheckAnalyzer(conn_str)
    analyzer.export_report("health_report.json")
    analyzer.generate_markdown_report("health_report.md")
    analyzer.close()

    print("\n=== GenAI Integration ===")
    print("Next: 'Analyze this health report and suggest Aurora PostgreSQL optimization strategies'")
