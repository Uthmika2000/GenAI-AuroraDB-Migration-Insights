"""
Use Case 3: Predictive Anomaly Detection & Alerting
Collects time-series performance data for ML training and anomaly detection
"""
import pyodbc
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import json
from dotenv import load_dotenv

load_dotenv()

class PerformanceDataCollector:
    def __init__(self, connection_string):
        self.conn = pyodbc.connect(connection_string)
        self.cursor = self.conn.cursor()
        # Ensure outputs directory exists for all writes
        os.makedirs("outputs", exist_ok=True)
        
    def collect_time_series_metrics(self, days=30):
        """Collect historical performance metrics for ML training"""
        
        # Collect CPU time series
        cpu_data = self.collect_cpu_time_series(days)
        
        # Collect query execution time series
        query_data = self.collect_query_time_series(days)
        
        # Collect wait statistics time series
        wait_data = self.collect_wait_statistics(days)
        
        # Collect database size growth
        size_data = self.collect_size_metrics()
        
        return {
            'cpu_metrics': cpu_data,
            'query_metrics': query_data,
            'wait_statistics': wait_data,
            'size_metrics': size_data,
            'collection_timestamp': datetime.now().isoformat()
        }
    
    def collect_cpu_time_series(self, days):
        """Collect CPU utilization over time"""
        # Note: This queries the ring buffer which has limited history
        self.cursor.execute("""
            SELECT 
                DATEADD(ms, -1 * ((SELECT ms_ticks FROM sys.dm_os_sys_info) - [timestamp]), GETDATE()) AS sample_time,
                record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int') AS sql_cpu,
                record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int') AS system_idle,
                100 - record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int') - 
                    record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int') AS other_cpu
            FROM (
                SELECT timestamp, CONVERT(xml, record) AS record 
                FROM sys.dm_os_ring_buffers 
                WHERE ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR' 
                AND record LIKE '%<SystemHealth>%'
            ) AS x
            ORDER BY sample_time DESC
        """)
        
        cpu_samples = []
        for row in self.cursor.fetchall():
            cpu_samples.append({
                'timestamp': str(row.sample_time),
                'sql_cpu': row.sql_cpu,
                'system_idle': row.system_idle,
                'other_cpu': row.other_cpu
            })
        
        return cpu_samples
    
    def collect_query_time_series(self, days):
        """Collect query execution patterns"""
        self.cursor.execute("""
            SELECT 
                qs.creation_time,
                qs.last_execution_time,
                qs.execution_count,
                qs.total_elapsed_time,
                qs.total_worker_time,
                qs.total_logical_reads,
                qs.total_logical_writes,
                qs.total_rows,
                (qs.total_elapsed_time / qs.execution_count) AS avg_elapsed_time,
                (qs.total_worker_time / qs.execution_count) AS avg_worker_time,
                DB_NAME(qt.dbid) AS database_name,
                SUBSTRING(qt.text, 1, 100) AS query_sample
            FROM sys.dm_exec_query_stats qs
            CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) qt
            WHERE qs.last_execution_time >= DATEADD(day, -30, GETDATE())
            ORDER BY qs.total_elapsed_time DESC
        """)
        
        query_patterns = []
        for row in self.cursor.fetchall():
            query_patterns.append({
                'creation_time': str(row.creation_time),
                'last_execution_time': str(row.last_execution_time),
                'execution_count': row.execution_count,
                'total_elapsed_time_ms': row.total_elapsed_time / 1000.0,
                'avg_elapsed_time_ms': row.avg_elapsed_time / 1000.0,
                'total_logical_reads': row.total_logical_reads,
                'total_rows': row.total_rows,
                'database': row.database_name,
                'query_sample': row.query_sample
            })
        
        return query_patterns
    
    def collect_wait_statistics(self, days):
        """Collect wait statistics for identifying bottlenecks"""
        self.cursor.execute("""
            SELECT 
                wait_type,
                waiting_tasks_count,
                wait_time_ms,
                max_wait_time_ms,
                signal_wait_time_ms,
                (wait_time_ms - signal_wait_time_ms) AS resource_wait_time_ms,
                CASE 
                    WHEN waiting_tasks_count = 0 THEN 0
                    ELSE wait_time_ms / waiting_tasks_count
                END AS avg_wait_time_ms
            FROM sys.dm_os_wait_stats
            WHERE wait_type NOT IN (
                SELECT wait_type 
                FROM sys.dm_os_wait_stats 
                WHERE wait_type LIKE 'SLEEP%'
                   OR wait_type LIKE 'BROKER%'
                   OR wait_type LIKE 'XE%'
                   OR wait_type LIKE 'SQLTRACE%'
            )
            AND wait_time_ms > 0
            ORDER BY wait_time_ms DESC
        """)
        
        wait_stats = []
        for row in self.cursor.fetchall():
            wait_stats.append({
                'wait_type': row.wait_type,
                'waiting_tasks': row.waiting_tasks_count,
                'total_wait_ms': row.wait_time_ms,
                'max_wait_ms': row.max_wait_time_ms,
                'avg_wait_ms': row.avg_wait_time_ms,
                'resource_wait_ms': row.resource_wait_time_ms
            })
        
        return wait_stats
    
    def collect_size_metrics(self):
        """Collect database size and growth metrics"""
        self.cursor.execute("""
            SELECT 
                d.name AS database_name,
                SUM(mf.size * 8 / 1024.0) AS size_mb,
                SUM(CASE WHEN mf.type_desc = 'ROWS' THEN mf.size * 8 / 1024.0 ELSE 0 END) AS data_size_mb,
                SUM(CASE WHEN mf.type_desc = 'LOG' THEN mf.size * 8 / 1024.0 ELSE 0 END) AS log_size_mb,
                SUM(mf.growth * 8 / 1024.0) AS growth_increment_mb,
                MAX(CASE WHEN mf.is_percent_growth = 1 THEN mf.growth ELSE 0 END) AS growth_percent
            FROM sys.databases d
            JOIN sys.master_files mf ON d.database_id = mf.database_id
            WHERE d.database_id > 4
            GROUP BY d.name
            ORDER BY size_mb DESC
        """)
        
        size_metrics = []
        for row in self.cursor.fetchall():
            size_metrics.append({
                'database': row.database_name,
                'total_size_mb': row.size_mb,
                'data_size_mb': row.data_size_mb,
                'log_size_mb': row.log_size_mb,
                'growth_increment_mb': row.growth_increment_mb,
                'growth_percent': row.growth_percent
            })
        
        return size_metrics
    
    def prepare_ml_dataset(self, output_file='ml_training_data.csv'):
        
        data = self.collect_time_series_metrics(30)
        
        cpu_df = pd.DataFrame(data['cpu_metrics'])
        query_df = pd.DataFrame(data['query_metrics'])
        
        os.makedirs("outputs", exist_ok=True)
        cpu_path = os.path.join("outputs", f'cpu_{output_file}')
        query_path = os.path.join("outputs", f'query_{output_file}')
        cpu_df.to_csv(cpu_path, index=False)
        query_df.to_csv(query_path, index=False)
        
        print(f"ML training datasets created:")
        print(f"  - {cpu_path}: {len(cpu_df)} samples")
        print(f"  - {query_path}: {len(query_df)} samples")
        
        return {
            'cpu_data': cpu_df,
            'query_data': query_df
        }
    
    def detect_anomalies_simple(self):
        data = self.collect_time_series_metrics(30)
        
        anomalies = []
        
        # Check CPU anomalies
        cpu_values = [m['sql_cpu'] for m in data['cpu_metrics']]
        if cpu_values:
            cpu_mean = np.mean(cpu_values)
            cpu_std = np.std(cpu_values)
            
            for metric in data['cpu_metrics']:
                if abs(metric['sql_cpu'] - cpu_mean) > 2 * cpu_std:
                    anomalies.append({
                        'type': 'CPU_SPIKE',
                        'timestamp': metric['timestamp'],
                        'value': metric['sql_cpu'],
                        'threshold': cpu_mean + 2 * cpu_std,
                        'severity': 'HIGH' if metric['sql_cpu'] > 90 else 'MEDIUM'
                    })
        
        # Check query anomalies
        query_times = [q['avg_elapsed_time_ms'] for q in data['query_metrics']]
        if query_times:
            query_mean = np.mean(query_times)
            query_std = np.std(query_times)
            
            for query in data['query_metrics']:
                if query['avg_elapsed_time_ms'] > query_mean + 3 * query_std:
                    anomalies.append({
                        'type': 'SLOW_QUERY',
                        'timestamp': query['last_execution_time'],
                        'value': query['avg_elapsed_time_ms'],
                        'threshold': query_mean + 3 * query_std,
                        'query_sample': query['query_sample'],
                        'severity': 'HIGH'
                    })
        
        return {
            'anomalies': anomalies,
            'detection_time': datetime.now().isoformat(),
            'summary': {
                'total_anomalies': len(anomalies),
                'cpu_spikes': sum(1 for a in anomalies if a['type'] == 'CPU_SPIKE'),
                'slow_queries': sum(1 for a in anomalies if a['type'] == 'SLOW_QUERY')
            }
        }
    
    def generate_alert_context(self):
        # Generate context for LLM to create alerts
        anomalies = self.detect_anomalies_simple()
        metrics = self.collect_time_series_metrics(7)
        
        context = {
            'anomalies': anomalies,
            'recent_metrics': {
                'cpu_avg': np.mean([m['sql_cpu'] for m in metrics['cpu_metrics']]) if metrics['cpu_metrics'] else 0,
                'top_wait_types': metrics['wait_statistics'][:5] if 'wait_statistics' in metrics else [],
                'slowest_queries': metrics['query_metrics'][:5] if 'query_metrics' in metrics else []
            },
            'timestamp': datetime.now().isoformat()
        }
        
        return context
    
    def export_for_bedrock(self, filename='anomaly_context.json'):
        context = self.generate_alert_context()
        
        os.makedirs("outputs", exist_ok=True)
        out_path = os.path.join("outputs", filename)
        with open(out_path, 'w') as f:
            json.dump(context, f, indent=2, default=str)
        
        print(f"Anomaly context exported for Claude/Bedrock: {out_path}")
        return context
    
    def close(self):
        self.cursor.close()
        self.conn.close()

# Usage
if __name__ == "__main__":
    SQL_SERVER = os.getenv("SQL_SERVER", "localhost")
    SQL_DATABASE = os.getenv("SQL_DATABASE", "master")
    SQL_USERNAME = os.getenv("SQL_USERNAME", "sa")
    SQL_PASSWORD = os.getenv("SQL_PASSWORD", "")
    SQL_PORT = os.getenv("SQL_PORT", "1433")

    conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SQL_SERVER},{SQL_PORT};DATABASE={SQL_DATABASE};UID={SQL_USERNAME};PWD={SQL_PASSWORD}"
    
    collector = PerformanceDataCollector(conn_str)
    
    # Collect data for ML training
    ml_data = collector.prepare_ml_dataset()
    
    # Detect anomalies
    anomalies = collector.detect_anomalies_simple()
    print(f"\nDetected {anomalies['summary']['total_anomalies']} anomalies")
    
    # Export for Bedrock
    context = collector.export_for_bedrock()
    
    collector.close()
    
    print("\n=== GenAI Integration ===")
    print("Upload outputs/anomaly_context.json to Claude/AWS Bedrock with prompt:")
    print("'Analyze these database anomalies and generate actionable alerts with root cause analysis'")