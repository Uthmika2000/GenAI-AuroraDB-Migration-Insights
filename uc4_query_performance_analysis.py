"""
Use Case 4: Automated Query Performance Analysis
Extracts and analyzes slow queries for optimization recommendations
"""
import pyodbc
import re
import json
import os
from datetime import datetime
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv()


class QueryPerformanceAnalyzer:
    def __init__(self, connection_string):
        self.conn = pyodbc.connect(connection_string)
        self.cursor = self.conn.cursor()
        
        # Create outputs folder if it doesn't exist
        if not os.path.exists('outputs'):
            os.makedirs('outputs')
            print("✓ Created 'outputs' folder")
        
    def extract_slow_queries(self, threshold_ms=1000, top_n=50):
        """Extract queries exceeding performance threshold"""
        try:
            self.cursor.execute(f"""
                SELECT TOP {top_n}
                    qs.execution_count,
                    qs.total_elapsed_time,
                    qs.total_worker_time,
                    qs.total_logical_reads,
                    qs.total_logical_writes,
                    qs.total_physical_reads,
                    (qs.total_elapsed_time / qs.execution_count) AS avg_elapsed_time,
                    (qs.total_worker_time / qs.execution_count) AS avg_worker_time,
                    (qs.total_logical_reads / qs.execution_count) AS avg_logical_reads,
                    qs.creation_time,
                    qs.last_execution_time,
                    DB_NAME(qt.dbid) AS database_name,
                    OBJECT_NAME(qt.objectid, qt.dbid) AS object_name,
                    qt.text AS query_text,
                    qp.query_plan
                FROM sys.dm_exec_query_stats qs
                CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) qt
                CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) qp
                WHERE (qs.total_elapsed_time / qs.execution_count) > ({threshold_ms} * 1000)
                ORDER BY avg_elapsed_time DESC
            """)
            
            slow_queries = []
            for row in self.cursor.fetchall():
                query_info = {
                    'execution_count': row.execution_count,
                    'avg_elapsed_ms': row.avg_elapsed_time / 1000.0,
                    'avg_worker_ms': row.avg_worker_time / 1000.0,
                    'avg_logical_reads': row.avg_logical_reads,
                    'total_elapsed_ms': row.total_elapsed_time / 1000.0,
                    'database': row.database_name,
                    'object_name': row.object_name,
                    'query_text': row.query_text,
                    'query_plan': str(row.query_plan) if row.query_plan else None,
                    'last_execution': str(row.last_execution_time)
                }
                slow_queries.append(query_info)
            
            return slow_queries
        except Exception as e:
            print(f"Warning: Could not extract slow queries: {e}")
            return []
    
    def analyze_execution_plans(self, slow_queries):
        """Analyze execution plans for common issues"""
        analysis_results = []
        
        for query in slow_queries:
            issues = []
            recommendations = []
            
            query_text = query['query_text'].upper()
            plan_text = (query['query_plan'] or '').upper() if query['query_plan'] else ''
            
            # Check for table scans
            if 'TABLE SCAN' in plan_text or 'CLUSTERED INDEX SCAN' in plan_text:
                issues.append('Table scan detected - may indicate missing index')
                recommendations.append('Consider adding appropriate index')
            
            # Check for key lookups
            if 'KEY LOOKUP' in plan_text or 'RID LOOKUP' in plan_text:
                issues.append('Key/RID lookup detected - possible covering index opportunity')
                recommendations.append('Consider creating covering index')
            
            # Check for implicit conversions
            if 'CONVERT_IMPLICIT' in plan_text:
                issues.append('Implicit conversion detected - may prevent index usage')
                recommendations.append('Review data types and add explicit CAST if needed')
            
            # Check for SELECT *
            if re.search(r'SELECT\s+\*', query_text):
                issues.append('SELECT * usage - retrieving unnecessary columns')
                recommendations.append('Specify only required columns')
            
            # Check for missing WHERE clause
            if 'WHERE' not in query_text and 'JOIN' not in query_text:
                issues.append('Missing WHERE clause - potential full table scan')
                recommendations.append('Add appropriate filtering conditions')
            
            # Check for OR conditions
            if ' OR ' in query_text:
                issues.append('OR conditions may prevent index usage')
                recommendations.append('Consider splitting into UNION queries')
            
            # Check for LIKE with leading wildcard
            if re.search(r"LIKE\s+['\"]%", query_text):
                issues.append('LIKE with leading wildcard - index not used')
                recommendations.append('Restructure query or use full-text search')
            
            # Check for scalar functions on columns
            if re.search(r'WHERE\s+\w+\(', query_text):
                issues.append('Function on column in WHERE clause - prevents index usage')
                recommendations.append('Move function to right side of comparison or use computed column')
            
            analysis_results.append({
                'query_id': hash(query['query_text']),
                'query_sample': query['query_text'][:200],
                'avg_elapsed_ms': query['avg_elapsed_ms'],
                'execution_count': query['execution_count'],
                'issues': issues,
                'recommendations': recommendations,
                'priority': 'HIGH' if query['avg_elapsed_ms'] > 5000 else 'MEDIUM'
            })
        
        return analysis_results
    
    def identify_missing_indexes(self):
        """Identify missing indexes based on system recommendations"""
        try:
            self.cursor.execute("""
                SELECT 
                    DB_NAME(mid.database_id) AS database_name,
                    OBJECT_NAME(mid.object_id, mid.database_id) AS table_name,
                    migs.avg_user_impact,
                    migs.user_seeks,
                    migs.user_scans,
                    mid.equality_columns,
                    mid.inequality_columns,
                    mid.included_columns,
                    migs.last_user_seek,
                    migs.last_user_scan
                FROM sys.dm_db_missing_index_details mid
                INNER JOIN sys.dm_db_missing_index_groups mig
                    ON mid.index_handle = mig.index_handle
                INNER JOIN sys.dm_db_missing_index_group_stats migs
                    ON mig.index_group_handle = migs.group_handle
                WHERE migs.avg_user_impact > 10
                ORDER BY migs.avg_user_impact DESC
            """)
            
            missing_indexes = []
            for row in self.cursor.fetchall():
                index_def = self.build_index_statement(
                    row.database_name,
                    row.table_name,
                    row.equality_columns,
                    row.inequality_columns,
                    row.included_columns
                )
                
                missing_indexes.append({
                    'database': row.database_name,
                    'table': row.table_name,
                    'avg_impact': row.avg_user_impact,
                    'user_seeks': row.user_seeks,
                    'user_scans': row.user_scans,
                    'equality_columns': row.equality_columns,
                    'inequality_columns': row.inequality_columns,
                    'included_columns': row.included_columns,
                    'create_statement': index_def,
                    'last_usage': str(row.last_user_seek or row.last_user_scan)
                })
            
            return missing_indexes
        except Exception as e:
            print(f"Warning: Could not identify missing indexes: {e}")
            return []
    
    def build_index_statement(self, database, table, equality_cols, inequality_cols, included_cols):
        key_columns = []
        
        if equality_cols:
            key_columns.extend([col.strip() for col in equality_cols.split(',')])
        if inequality_cols:
            key_columns.extend([col.strip() for col in inequality_cols.split(',')])
        
        if not key_columns:
            return None
        
        # Sanitize column names for index name
        safe_cols = [re.sub(r'[^\w]', '', col) for col in key_columns[:3]]
        index_name = f"IX_{table}_{'_'.join(safe_cols)}"
        
        statement = f"CREATE NONCLUSTERED INDEX [{index_name}]\n"
        statement += f"ON [{database}].[dbo].[{table}]\n"
        statement += f"({', '.join([f'[{col}]' for col in key_columns])})\n"
        
        if included_cols:
            inc_cols = [col.strip() for col in included_cols.split(',')]
            statement += f"INCLUDE ({', '.join([f'[{col}]' for col in inc_cols])})\n"
        
        return statement
    
    def generate_postgresql_conversion_notes(self, slow_queries):
        # Generate PostgreSQL-specific conversion notes
        conversion_notes = []
        
        for query in slow_queries:
            query_text = query['query_text'].upper()
            notes = []
            
            # T-SQL specific constructs
            if 'NOLOCK' in query_text:
                notes.append("NOLOCK hint → Use READ UNCOMMITTED isolation level in PostgreSQL")
            
            if 'TOP' in query_text:
                notes.append("TOP N → Use LIMIT N in PostgreSQL")
            
            if 'ISNULL(' in query_text:
                notes.append("ISNULL() → Use COALESCE() in PostgreSQL")
            
            if 'GETDATE()' in query_text:
                notes.append("GETDATE() → Use CURRENT_TIMESTAMP in PostgreSQL")
            
            if 'DATEADD' in query_text or 'DATEDIFF' in query_text:
                notes.append("Date functions → Use PostgreSQL interval arithmetic")
            
            if 'LEN(' in query_text:
                notes.append("LEN() → Use LENGTH() or CHAR_LENGTH() in PostgreSQL")
            
            if '[]' in query_text:
                notes.append("Square brackets → Use double quotes for identifiers in PostgreSQL")
            
            if notes:
                conversion_notes.append({
                    'query_sample': query['query_text'][:200],
                    'conversion_notes': notes
                })
        
        return conversion_notes
    
    def generate_optimization_report(self, output_file='outputs/query_optimization_report.json'):
        """Generate comprehensive optimization report"""
        print("\nAnalyzing query performance...")
        
        # Collect data
        print("  Extracting slow queries...")
        slow_queries = self.extract_slow_queries()
        
        print("  Analyzing execution plans...")
        analysis = self.analyze_execution_plans(slow_queries)
        
        print("  Identifying missing indexes...")
        missing_indexes = self.identify_missing_indexes()
        
        print("  Generating PostgreSQL conversion notes...")
        pg_conversions = self.generate_postgresql_conversion_notes(slow_queries)
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'summary': {
                'total_slow_queries': len(slow_queries),
                'queries_with_issues': len([a for a in analysis if a['issues']]),
                'missing_indexes': len(missing_indexes),
                'high_priority_queries': len([a for a in analysis if a['priority'] == 'HIGH'])
            },
            'slow_queries': slow_queries[:20],  # Top 20
            'query_analysis': analysis,
            'missing_indexes': missing_indexes,
            'postgresql_conversion_notes': pg_conversions
        }
        
        with open(output_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        print(f"\n✓ Optimization report generated: {output_file}")
        return report
    
    def generate_markdown_report(self, output_file='outputs/query_optimization_report.md'):
        """Generate human-readable markdown report"""
        report = self.generate_optimization_report()
        
        md_content = f"""# Query Performance Analysis Report

**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Executive Summary

- **Total Slow Queries:** {report['summary']['total_slow_queries']}
- **Queries with Issues:** {report['summary']['queries_with_issues']}
- **Missing Indexes:** {report['summary']['missing_indexes']}
- **High Priority Queries:** {report['summary']['high_priority_queries']}

## Top Slow Queries

"""
        
        for i, query in enumerate(report['slow_queries'][:10], 1):
            md_content += f"""### {i}. {query.get('object_name', 'Ad-hoc Query')}

**Average Execution Time:** {query['avg_elapsed_ms']:.2f} ms  
**Executions:** {query['execution_count']}  
**Database:** {query['database']}

```sql
{query['query_text'][:500]}
```

"""
        
        md_content += "## Query Analysis\n\n"
        
        for analysis in report['query_analysis'][:10]:
            if analysis['issues']:
                md_content += f"""### Query (First 200 chars)
```
{analysis['query_sample']}
```

**Issues Found:**
"""
                for issue in analysis['issues']:
                    md_content += f"- {issue}\n"
                
                md_content += "\n**Recommendations:**\n"
                for rec in analysis['recommendations']:
                    md_content += f"- {rec}\n"
                
                md_content += "\n"
        
        md_content += "## Recommended Missing Indexes\n\n"
        
        for idx in report['missing_indexes'][:10]:
            if idx['create_statement']:
                md_content += f"""### {idx['table']} (Impact: {idx['avg_impact']:.1f}%)

```sql
{idx['create_statement']}
```

**Usage:** {idx['user_seeks']} seeks, {idx['user_scans']} scans

"""
        
        md_content += "## PostgreSQL Conversion Notes\n\n"
        
        for note in report['postgresql_conversion_notes'][:10]:
            md_content += f"""### Query Sample
```sql
{note['query_sample']}
```

**Conversion Required:**
"""
            for conv_note in note['conversion_notes']:
                md_content += f"- {conv_note}\n"
            
            md_content += "\n"
        
        with open(output_file, 'w') as f:
            f.write(md_content)
        
        print(f"✓ Markdown report generated: {output_file}")
        return md_content
    
    def generate_summary(self, report):
        """Generate console summary"""
        print("\n" + "="*60)
        print("QUERY PERFORMANCE ANALYSIS SUMMARY")
        print("="*60)
        
        summary = report['summary']
        print(f"\n Total Slow Queries: {summary['total_slow_queries']}")
        print(f"  Queries with Issues: {summary['queries_with_issues']}")
        print(f" Missing Indexes: {summary['missing_indexes']}")
        print(f" High Priority Queries: {summary['high_priority_queries']}")
        
        if report['slow_queries']:
            print("\n Top 5 Slowest Queries:")
            for i, query in enumerate(report['slow_queries'][:5], 1):
                obj_name = query.get('object_name', 'Ad-hoc')
                print(f"  {i}. {obj_name}: {query['avg_elapsed_ms']:.2f} ms (Exec: {query['execution_count']}x)")
        
        if report['missing_indexes']:
            print("\n Top 5 Missing Indexes (by impact):")
            for i, idx in enumerate(report['missing_indexes'][:5], 1):
                print(f"  {i}. {idx['table']}: {idx['avg_impact']:.1f}% impact")
        
        print("\n" + "="*60)
    
    def close(self):
        self.cursor.close()
        self.conn.close()

# Usage
if __name__ == "__main__":
    print("\n" + "="*60)
    print("SQL SERVER QUERY PERFORMANCE ANALYZER")
    print("="*60 + "\n")
    
    SQL_SERVER = os.getenv("SQL_SERVER", "localhost")
    SQL_DATABASE = os.getenv("SQL_DATABASE", "master")
    SQL_USERNAME = os.getenv("SQL_USERNAME", "sa")
    SQL_PASSWORD = os.getenv("SQL_PASSWORD", "")
    SQL_PORT = os.getenv("SQL_PORT", "1433")
    
    conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SQL_SERVER},{SQL_PORT};DATABASE={SQL_DATABASE};UID={SQL_USERNAME};PWD={SQL_PASSWORD}"
    
    try:
        print("Connecting to SQL Server...")
        analyzer = QueryPerformanceAnalyzer(conn_str)
        print("✓ Connected successfully!\n")
        
        # Generate reports
        json_report = analyzer.generate_optimization_report()
        md_report = analyzer.generate_markdown_report()
        
        # Display summary
        analyzer.generate_summary(json_report)
        
        analyzer.close()
        
        print("\n Next Steps:")
        print("1. Review outputs/query_optimization_report.json")
        print("2. Review outputs/query_optimization_report.md for human-readable analysis")
        print("3. Ask GitHub Copilot/Claude:")
        print("   'Review these slow queries and generate optimized PostgreSQL equivalents'")
        print("4. Implement recommended indexes")
        print("5. Test query performance improvements")
        
    except pyodbc.Error as e:
        print(f"\n Database Error: {e}")
        print("\nTroubleshooting:")
        print("1. Verify SQL Server is running")
        print("2. Check connection settings")
        print("3. Ensure you have permissions to query DMVs")
        print("4. Run some queries first to populate query stats")
    except Exception as e:
        print(f"\n Error: {e}")
        import traceback
        traceback.print_exc()