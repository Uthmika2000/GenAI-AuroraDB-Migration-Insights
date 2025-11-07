"""
Use Case 2: Intelligent Health Check Report Generation
Analyzes database health metrics and uses GenAI for recommendations
"""
import pyodbc
import json
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

class HealthCheckAnalyzer:
    def __init__(self, connection_string):
        self.conn = pyodbc.connect(connection_string)
        self.cursor = self.conn.cursor()
        self.metrics = {}
        
        # Create outputs folder
        if not os.path.exists('outputs'):
            os.makedirs('outputs')
            print("✓ Created 'outputs' folder")
        
    def collect_cpu_metrics(self):
        """Collect CPU utilization metrics"""
        try:
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
            max_cpu = max([d['sql_cpu'] for d in cpu_data]) if cpu_data else 0
            
            # More nuanced status determination
            status = 'OK'
            if avg_cpu > 80 or max_cpu > 95:
                status = 'CRITICAL'
            elif avg_cpu > 60 or max_cpu > 80:
                status = 'WARNING'
            elif avg_cpu > 40:
                status = 'ATTENTION'
            
            self.metrics['cpu'] = {
                'current': cpu_data[0] if cpu_data else None,
                'average': round(avg_cpu, 2),
                'max': max_cpu,
                'min': min([d['sql_cpu'] for d in cpu_data]) if cpu_data else 0,
                'status': status,
                'history': cpu_data
            }
        except Exception as e:
            print(f"  Warning: Could not collect CPU metrics: {e}")
            self.metrics['cpu'] = {'status': 'UNKNOWN', 'error': str(e)}
        
    def collect_memory_metrics(self):
        """Collect memory pressure indicators"""
        try:
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
            
            # Get buffer pool info
            self.cursor.execute("""
                SELECT 
                    (COUNT(*) * 8.0 / 1024) AS Buffer_Pool_MB,
                    SUM(CASE WHEN is_modified = 1 THEN 1 ELSE 0 END) * 8.0 / 1024 AS Dirty_Pages_MB
                FROM sys.dm_os_buffer_descriptors
            """)
            
            buffer_row = self.cursor.fetchone()
            
            # Calculate memory pressure
            buffer_pool_mb = buffer_row.Buffer_Pool_MB if buffer_row.Buffer_Pool_MB else 0
            dirty_pages_mb = buffer_row.Dirty_Pages_MB if buffer_row.Dirty_Pages_MB else 0
            dirty_ratio = (dirty_pages_mb / buffer_pool_mb * 100) if buffer_pool_mb > 0 else 0
            
            # Determine status based on multiple factors
            status = 'OK'
            if row.process_physical_memory_low or row.process_virtual_memory_low:
                status = 'CRITICAL'
            elif dirty_ratio > 50:
                status = 'WARNING'
            elif dirty_ratio > 30 or memory_used > 1000:
                status = 'ATTENTION'
            
            self.metrics['memory'] = {
                'used_mb': round(memory_used, 2),
                'large_page_mb': round(row.Large_Page_MB, 2),
                'locked_pages_mb': round(row.Locked_Pages_MB, 2),
                'buffer_pool_mb': round(buffer_pool_mb, 2),
                'dirty_pages_mb': round(dirty_pages_mb, 2),
                'dirty_ratio_percent': round(dirty_ratio, 2),
                'low_memory_signal': bool(row.process_physical_memory_low),
                'low_virtual_memory': bool(row.process_virtual_memory_low),
                'status': status
            }
        except Exception as e:
            print(f"  Warning: Could not collect memory metrics: {e}")
            self.metrics['memory'] = {'status': 'UNKNOWN', 'error': str(e)}
        
    def collect_query_performance(self):
        """Collect slow query statistics"""
        try:
            self.cursor.execute("""
                SELECT TOP 20
                    qs.execution_count,
                    qs.total_elapsed_time / 1000.0 / qs.execution_count AS avg_elapsed_ms,
                    qs.total_worker_time / 1000.0 / qs.execution_count AS avg_cpu_ms,
                    qs.total_logical_reads / qs.execution_count AS avg_logical_reads,
                    qs.total_logical_writes / qs.execution_count AS avg_logical_writes,
                    qs.last_execution_time,
                    SUBSTRING(qt.text, (qs.statement_start_offset/2)+1,
                        ((CASE qs.statement_end_offset
                            WHEN -1 THEN DATALENGTH(qt.text)
                            ELSE qs.statement_end_offset
                        END - qs.statement_start_offset)/2) + 1) AS query_text
                FROM sys.dm_exec_query_stats qs
                CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) qt
                ORDER BY qs.total_elapsed_time / qs.execution_count DESC
            """)
            
            all_queries = []
            slow_queries = []  # > 1000ms
            moderate_queries = []  # > 500ms
            
            for row in self.cursor.fetchall():
                query_info = {
                    'execution_count': row.execution_count,
                    'avg_elapsed_ms': round(row.avg_elapsed_ms, 2),
                    'avg_cpu_ms': round(row.avg_cpu_ms, 2),
                    'avg_logical_reads': row.avg_logical_reads,
                    'avg_logical_writes': row.avg_logical_writes,
                    'last_execution': str(row.last_execution_time),
                    'query_text': row.query_text[:200]
                }
                all_queries.append(query_info)
                
                if row.avg_elapsed_ms > 1000:
                    slow_queries.append(query_info)
                elif row.avg_elapsed_ms > 500:
                    moderate_queries.append(query_info)
            
            # Determine status
            status = 'OK'
            if len(slow_queries) > 5:
                status = 'CRITICAL'
            elif len(slow_queries) > 2 or len(moderate_queries) > 5:
                status = 'WARNING'
            elif len(slow_queries) > 0 or len(moderate_queries) > 0:
                status = 'ATTENTION'
            
            self.metrics['query_performance'] = {
                'total_queries_analyzed': len(all_queries),
                'slow_queries_count': len(slow_queries),
                'moderate_queries_count': len(moderate_queries),
                'top_slow_queries': slow_queries[:10],
                'top_moderate_queries': moderate_queries[:5],
                'status': status
            }
        except Exception as e:
            print(f"  Warning: Could not collect query performance: {e}")
            self.metrics['query_performance'] = {'status': 'UNKNOWN', 'error': str(e)}
        
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
                'has_replication': any(db['status'] == 'REPLICATION_ACTIVE' for db in repl_status),
                'status': 'OK'
            }
        except Exception as e:
            self.metrics['replication'] = {'error': str(e), 'status': 'UNKNOWN'}
            
    def collect_io_statistics(self):
        """Collect I/O performance metrics"""
        try:
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
            critical_latency = 0
            high_latency_count = 0
            
            for row in self.cursor.fetchall():
                avg_read = row.avg_read_latency_ms
                avg_write = row.avg_write_latency_ms
                
                if avg_read > 50 or avg_write > 50:
                    critical_latency += 1
                elif avg_read > 20 or avg_write > 20:
                    high_latency_count += 1
                
                io_stats.append({
                    'database': row.database_name,
                    'file_id': row.file_id,
                    'avg_read_latency_ms': round(avg_read, 2),
                    'avg_write_latency_ms': round(avg_write, 2),
                    'total_reads': row.num_of_reads,
                    'total_writes': row.num_of_writes
                })
            
            # Determine status
            status = 'OK'
            if critical_latency > 0:
                status = 'CRITICAL'
            elif high_latency_count > 2:
                status = 'WARNING'
            elif high_latency_count > 0:
                status = 'ATTENTION'
            
            self.metrics['io'] = {
                'critical_latency_files': critical_latency,
                'high_latency_files': high_latency_count,
                'file_stats': io_stats[:20],
                'status': status
            }
        except Exception as e:
            print(f"  Warning: Could not collect I/O statistics: {e}")
            self.metrics['io'] = {'status': 'UNKNOWN', 'error': str(e)}
    
    def collect_database_info(self):
        # Collect general database information
        try:
            self.cursor.execute("""
                SELECT 
                    COUNT(*) as total_databases,
                    SUM(CASE WHEN state_desc = 'ONLINE' THEN 1 ELSE 0 END) as online_dbs,
                    SUM(CASE WHEN recovery_model_desc = 'FULL' THEN 1 ELSE 0 END) as full_recovery_dbs
                FROM sys.databases
                WHERE database_id > 4
            """)
            
            row = self.cursor.fetchone()
            
            self.metrics['database_info'] = {
                'total_databases': row.total_databases,
                'online_databases': row.online_dbs,
                'full_recovery_databases': row.full_recovery_dbs,
                'status': 'OK'
            }
        except Exception as e:
            self.metrics['database_info'] = {'error': str(e), 'status': 'UNKNOWN'}
    
    def collect_wait_statistics(self):
        # Collect wait statistics to identify bottlenecks
        try:
            self.cursor.execute("""
                SELECT TOP 10
                    wait_type,
                    waiting_tasks_count,
                    wait_time_ms,
                    max_wait_time_ms,
                    signal_wait_time_ms
                FROM sys.dm_os_wait_stats
                WHERE wait_type NOT IN (
                    'CLR_SEMAPHORE', 'LAZYWRITER_SLEEP', 'RESOURCE_QUEUE', 'SLEEP_TASK',
                    'SLEEP_SYSTEMTASK', 'SQLTRACE_BUFFER_FLUSH', 'WAITFOR', 'LOGMGR_QUEUE',
                    'CHECKPOINT_QUEUE', 'REQUEST_FOR_DEADLOCK_SEARCH', 'XE_TIMER_EVENT',
                    'BROKER_TO_FLUSH', 'BROKER_TASK_STOP', 'CLR_MANUAL_EVENT',
                    'CLR_AUTO_EVENT', 'DISPATCHER_QUEUE_SEMAPHORE', 'FT_IFTS_SCHEDULER_IDLE_WAIT',
                    'XE_DISPATCHER_WAIT', 'XE_DISPATCHER_JOIN', 'BROKER_EVENTHANDLER',
                    'TRACEWRITE', 'FT_IFTSHC_MUTEX', 'SQLTRACE_INCREMENTAL_FLUSH_SLEEP',
                    'DIRTY_PAGE_POLL', 'HADR_FILESTREAM_IOMGR_IOCOMPLETION'
                )
                AND wait_time_ms > 0
                ORDER BY wait_time_ms DESC
            """)
            
            wait_stats = []
            for row in self.cursor.fetchall():
                wait_stats.append({
                    'wait_type': row.wait_type,
                    'waiting_tasks': row.waiting_tasks_count,
                    'wait_time_ms': row.wait_time_ms,
                    'avg_wait_ms': round(row.wait_time_ms / row.waiting_tasks_count, 2) if row.waiting_tasks_count > 0 else 0
                })
            
            # Identify problematic waits
            status = 'OK'
            if wait_stats and any(w['wait_type'] in ['PAGEIOLATCH_SH', 'PAGEIOLATCH_EX', 'IO_COMPLETION'] for w in wait_stats[:3]):
                status = 'WARNING'
            
            self.metrics['wait_statistics'] = {
                'top_waits': wait_stats,
                'status': status
            }
        except Exception as e:
            self.metrics['wait_statistics'] = {'error': str(e), 'status': 'UNKNOWN'}
        
    def generate_health_report(self):
        print("\nCollecting health metrics...")
        
        print("  CPU metrics...")
        self.collect_cpu_metrics()
        
        print("  Memory metrics...")
        self.collect_memory_metrics()
        
        print("  Query performance...")
        self.collect_query_performance()
        
        print("  Replication status...")
        self.collect_replication_metrics()
        
        print("  I/O statistics...")
        self.collect_io_statistics()
        
        print("  Database information...")
        self.collect_database_info()
        
        print("  Wait statistics...")
        self.collect_wait_statistics()
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'server_name': self.get_server_name(),
            'metrics': self.metrics,
            'overall_status': self.calculate_overall_status(),
            'recommendations': self.generate_recommendations()
        }
        
        return report
    
    def get_server_name(self):
        try:
            self.cursor.execute("SELECT @@SERVERNAME")
            return self.cursor.fetchone()[0]
        except:
            return "Unknown"
    
    def calculate_overall_status(self):
        statuses = []
        for metric_name, metric_data in self.metrics.items():
            if isinstance(metric_data, dict) and 'status' in metric_data:
                statuses.append(metric_data['status'])
        
        if 'CRITICAL' in statuses:
            return 'CRITICAL'
        elif 'WARNING' in statuses:
            return 'WARNING'
        elif 'ATTENTION' in statuses:
            return 'ATTENTION'
        elif 'UNKNOWN' in statuses:
            return 'NEEDS_REVIEW'
        else:
            return 'OK'
    
    def generate_recommendations(self):
        recommendations = []
        
        # CPU recommendations
        cpu_status = self.metrics.get('cpu', {}).get('status', 'UNKNOWN')
        cpu_avg = self.metrics.get('cpu', {}).get('average', 0)
        cpu_max = self.metrics.get('cpu', {}).get('max', 0)
        
        if cpu_status in ['CRITICAL', 'WARNING', 'ATTENTION']:
            severity = 'HIGH' if cpu_status == 'CRITICAL' else 'MEDIUM'
            recommendations.append({
                'category': 'CPU',
                'severity': severity,
                'issue': f"CPU utilization - Average: {cpu_avg}%, Max: {cpu_max}%",
                'recommendation': "Review top CPU-consuming queries, optimize query plans, add missing indexes, or scale up compute resources",
                'migration_note': "Aurora PostgreSQL auto-scaling can handle CPU spikes better. Consider Aurora Serverless v2 for variable workloads.",
                'priority': 1 if cpu_status == 'CRITICAL' else 2
            })
        
        # Memory recommendations
        memory_status = self.metrics.get('memory', {}).get('status', 'UNKNOWN')
        memory_used = self.metrics.get('memory', {}).get('used_mb', 0)
        dirty_ratio = self.metrics.get('memory', {}).get('dirty_ratio_percent', 0)
        
        if memory_status in ['CRITICAL', 'WARNING', 'ATTENTION']:
            severity = 'HIGH' if memory_status == 'CRITICAL' else 'MEDIUM'
            issue_parts = [f"Memory used: {memory_used:.0f} MB"]
            if dirty_ratio > 30:
                issue_parts.append(f"Dirty pages: {dirty_ratio:.1f}%")
            
            recommendations.append({
                'category': 'Memory',
                'severity': severity,
                'issue': ", ".join(issue_parts),
                'recommendation': "Review buffer pool configuration, check for memory leaks, consider adding more RAM, or review query memory grants",
                'migration_note': "Aurora PostgreSQL has different memory management. Evaluate instance memory requirements based on workload patterns.",
                'priority': 1 if memory_status == 'CRITICAL' else 3
            })
        
        # Query performance recommendations
        query_status = self.metrics.get('query_performance', {}).get('status', 'UNKNOWN')
        slow_count = self.metrics.get('query_performance', {}).get('slow_queries_count', 0)
        moderate_count = self.metrics.get('query_performance', {}).get('moderate_queries_count', 0)
        
        if query_status in ['CRITICAL', 'WARNING', 'ATTENTION']:
            severity = 'HIGH' if slow_count > 5 else 'MEDIUM'
            recommendations.append({
                'category': 'Query Performance',
                'severity': severity,
                'issue': f"{slow_count} slow queries (>1s), {moderate_count} moderate queries (>500ms)",
                'recommendation': "Optimize slow queries using execution plans, add missing indexes, update statistics, or refactor problematic queries",
                'migration_note': "Use PostgreSQL EXPLAIN ANALYZE for query tuning. Consider pg_stat_statements extension for ongoing monitoring.",
                'priority': 2
            })
        
        # I/O recommendations
        io_status = self.metrics.get('io', {}).get('status', 'UNKNOWN')
        critical_io = self.metrics.get('io', {}).get('critical_latency_files', 0)
        high_io = self.metrics.get('io', {}).get('high_latency_files', 0)
        
        if io_status in ['CRITICAL', 'WARNING', 'ATTENTION']:
            severity = 'HIGH' if critical_io > 0 else 'MEDIUM'
            recommendations.append({
                'category': 'I/O Performance',
                'severity': severity,
                'issue': f"{critical_io} files with critical latency (>50ms), {high_io} files with high latency (>20ms)",
                'recommendation': "Move to SSD storage, optimize queries to reduce I/O, implement proper indexing strategy, or consider partitioning large tables",
                'migration_note': "Aurora uses SSD-backed distributed storage with 6-way replication and automatic scaling. Expected I/O latency <5ms.",
                'priority': 2 if critical_io > 0 else 4
            })
        
        # Wait statistics recommendations
        wait_stats = self.metrics.get('wait_statistics', {})
        if wait_stats.get('status') in ['WARNING', 'ATTENTION']:
            top_waits = wait_stats.get('top_waits', [])
            if top_waits:
                top_wait_types = [w['wait_type'] for w in top_waits[:3]]
                recommendations.append({
                    'category': 'Wait Statistics',
                    'severity': 'MEDIUM',
                    'issue': f"Top wait types: {', '.join(top_wait_types)}",
                    'recommendation': "Investigate wait types - PAGEIOLATCH indicates disk I/O issues, LCK_ waits indicate blocking, CXPACKET suggests parallelism tuning needed",
                    'migration_note': "PostgreSQL has different wait events. Monitor pg_stat_activity for lock waits and pg_stat_bgwriter for checkpoint issues.",
                    'priority': 3
                })
        
        # Always add general recommendations
        recommendations.append({
            'category': 'General',
            'severity': 'INFO',
            'issue': "Regular maintenance",
            'recommendation': "Ensure regular index maintenance, statistics updates, and backup verification. Monitor disk space growth.",
            'migration_note': "Aurora automatically manages backups, snapshots, and point-in-time recovery. Vacuum/analyze is automatic in Aurora PostgreSQL.",
            'priority': 5
        })
        
        # Add database-specific recommendations
        db_info = self.metrics.get('database_info', {})
        total_dbs = db_info.get('total_databases', 0)
        if total_dbs > 10:
            recommendations.append({
                'category': 'Database Management',
                'severity': 'INFO',
                'issue': f"{total_dbs} user databases detected",
                'recommendation': "Review database consolidation opportunities. Consider separating databases by workload type (OLTP vs Analytics).",
                'migration_note': "Aurora supports multiple databases per cluster. Consider using separate clusters for different SLA requirements.",
                'priority': 6
            })
        
        # Sort by priority
        recommendations.sort(key=lambda x: x['priority'])
        
        return recommendations
    
    def export_report(self, filename='health_report.json'):
        report = self.generate_health_report()
        filepath = os.path.join("outputs", filename)
        
        with open(filepath, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        print(f"\n✓ Health report exported to {filepath}")
        return report
    
    def generate_markdown_report(self, filename='health_report.md'):
        report = self.generate_health_report()
        
        md_content = f"""# Database Health Check Report

**Generated:** {report['timestamp']}  
**Server:** {report['server_name']}  
**Overall Status:** {report['overall_status']}

## Summary

- **CPU Status:** {self.metrics.get('cpu', {}).get('status', 'N/A')} (Avg: {self.metrics.get('cpu', {}).get('average', 0):.1f}%)
- **Memory Status:** {self.metrics.get('memory', {}).get('status', 'N/A')} (Used: {self.metrics.get('memory', {}).get('used_mb', 0):.0f} MB)
- **Query Performance:** {self.metrics.get('query_performance', {}).get('status', 'N/A')} ({self.metrics.get('query_performance', {}).get('slow_queries_count', 0)} slow queries)
- **I/O Performance:** {self.metrics.get('io', {}).get('status', 'N/A')} ({self.metrics.get('io', {}).get('high_latency_files', 0)} high latency files)

## Detailed Metrics

### CPU Utilization
- Average: {self.metrics.get('cpu', {}).get('average', 0):.2f}%
- Maximum: {self.metrics.get('cpu', {}).get('max', 0):.2f}%
- Minimum: {self.metrics.get('cpu', {}).get('min', 0):.2f}%
- Status: {self.metrics.get('cpu', {}).get('status', 'N/A')}

### Memory Usage
- Used: {self.metrics.get('memory', {}).get('used_mb', 0):.2f} MB
- Buffer Pool: {self.metrics.get('memory', {}).get('buffer_pool_mb', 0):.2f} MB
- Dirty Pages: {self.metrics.get('memory', {}).get('dirty_ratio_percent', 0):.2f}%
- Status: {self.metrics.get('memory', {}).get('status', 'N/A')}

### Query Performance
- Total Queries Analyzed: {self.metrics.get('query_performance', {}).get('total_queries_analyzed', 0)}
- Slow Queries (>1s): {self.metrics.get('query_performance', {}).get('slow_queries_count', 0)}
- Moderate Queries (>500ms): {self.metrics.get('query_performance', {}).get('moderate_queries_count', 0)}
- Status: {self.metrics.get('query_performance', {}).get('status', 'N/A')}

### I/O Performance
- Critical Latency Files (>50ms): {self.metrics.get('io', {}).get('critical_latency_files', 0)}
- High Latency Files (>20ms): {self.metrics.get('io', {}).get('high_latency_files', 0)}
- Status: {self.metrics.get('io', {}).get('status', 'N/A')}

## Recommendations ({len(report['recommendations'])} total)

"""
        
        for i, rec in enumerate(report['recommendations'], 1):
            md_content += f"""### {i}. {rec['category']} - {rec['severity']}

**Issue:** {rec['issue']}  
**Recommendation:** {rec['recommendation']}  
**Migration Note:** {rec['migration_note']}

"""
        
        filepath = os.path.join("outputs", filename)
        with open(filepath, 'w') as f:
            f.write(md_content)
        
        print(f" Markdown report generated: {filepath}")
        return md_content
    
    def generate_summary(self, report):

        print("\n" + "="*60)
        print("DATABASE HEALTH CHECK SUMMARY")
        print("="*60)
        
        print(f"\nServer: {report['server_name']}")
        print(f"Overall Status: {report['overall_status']}")
        
        print(f"\n Metrics Status:")
        for metric_name, metric_data in self.metrics.items():
            if isinstance(metric_data, dict) and 'status' in metric_data:
                status = metric_data['status']
                emoji = 'alert' if status == 'CRITICAL' else 'warn' if status == 'WARNING' else 'none' if status == 'ATTENTION' else 'check'
                print(f"  {emoji} {metric_name.replace('_', ' ').title()}: {status}")
        
        print(f"\n Recommendations: {len(report['recommendations'])}")
        high_priority = [r for r in report['recommendations'] if r['severity'] == 'HIGH']
        if high_priority:
            print(f"    High Priority: {len(high_priority)}")
            for rec in high_priority[:3]:
                print(f"      - {rec['category']}: {rec['issue'][:60]}...")
        
        print("\n" + "="*60)
    
    def close(self):
        self.cursor.close()
        self.conn.close()

# Usage
if __name__ == "__main__":
    print("\n" + "="*60)
    print("SQL SERVER HEALTH CHECK ANALYZER")
    print("="*60 + "\n")
    
    SQL_SERVER = os.getenv("SQL_SERVER", "localhost")
    SQL_DATABASE = os.getenv("SQL_DATABASE", "master")
    SQL_USERNAME = os.getenv("SQL_USERNAME", "sa")
    SQL_PASSWORD = os.getenv("SQL_PASSWORD", "")
    SQL_PORT = os.getenv("SQL_PORT", "1433")

    conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SQL_SERVER},{SQL_PORT};DATABASE={SQL_DATABASE};UID={SQL_USERNAME};PWD={SQL_PASSWORD}"
    
    try:
        print("Connecting to SQL Server...")
        analyzer = HealthCheckAnalyzer(conn_str)
        print(" Connected successfully!\n")
        
        # Generate reports
        json_report = analyzer.export_report("health_report.json")
        md_report = analyzer.generate_markdown_report("health_report.md")
        
        # Display summary
        analyzer.generate_summary(json_report)
        
        analyzer.close()
        
        print("\n Next Steps:")
        print("1. Review outputs/health_report.json for detailed metrics")
        print("2. Review outputs/health_report.md for human-readable analysis")
        print("3. Ask GitHub Copilot/Claude:")
        print("   'Analyze this health report and suggest Aurora PostgreSQL optimization strategies'")
        print("4. Implement high-priority recommendations first")
        
    except Exception as e:
        print(f"\n Error: {e}")
        import traceback
        traceback.print_exc()