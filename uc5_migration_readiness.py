"""
Use Case 5: Migration Readiness Assessment
Comprehensive assessment of migration complexity and readiness
"""
import pyodbc
import json
from datetime import datetime
from collections import Counter
import os
from dotenv import load_dotenv

load_dotenv()

class MigrationReadinessAssessor:
    def __init__(self, connection_string):
        self.conn = pyodbc.connect(connection_string)
        self.cursor = self.conn.cursor()
        self.assessment = {}

    def _db_exists(self, database_name: str) -> bool:
        self.cursor.execute("SELECT DB_ID(?)", database_name)
        row = self.cursor.fetchone()
        return row is not None and row[0] is not None

    def _safe_db(self, database_name: str) -> str:
        name = str(database_name).strip()
        if name.startswith("{") and name.endswith("}"):
            name = name[1:-1]
        if (name.startswith("'") and name.endswith("'")) or (name.startswith('"') and name.endswith('"')):
            name = name[1:-1]
        name = name.strip("[]")
        return name

    def assess_schema_complexity(self, database_name):
        complexity_score = 0

        db = self._safe_db(database_name)
        self.cursor.execute(f"""
            SELECT 
                (SELECT COUNT(*) FROM [{db}].sys.tables) AS table_count,
                (SELECT COUNT(*) FROM [{db}].sys.views) AS view_count,
                (SELECT COUNT(*) FROM [{db}].sys.procedures) AS sp_count,
                (SELECT COUNT(*) FROM [{db}].sys.triggers) AS trigger_count,
                (SELECT COUNT(*) FROM [{db}].sys.types WHERE is_user_defined = 1) AS udt_count,
                (SELECT COUNT(*) FROM [{db}].sys.xml_schema_collections) AS xml_schema_count,
                (SELECT COUNT(*) FROM [{db}].sys.assemblies WHERE is_user_defined = 1) AS clr_assembly_count
        """)
        row = self.cursor.fetchone()
        details = {
            'tables': row.table_count,
            'views': row.view_count,
            'stored_procedures': row.sp_count,
            'triggers': row.trigger_count,
            'user_defined_types': row.udt_count,
            'xml_schemas': row.xml_schema_count,
            'clr_assemblies': row.clr_assembly_count
        }

        complexity_score += min(details['tables'] / 10, 20)
        complexity_score += min(details['stored_procedures'] / 5, 25)
        complexity_score += min(details['triggers'] * 5, 15)
        complexity_score += min(details['user_defined_types'] * 10, 20)
        complexity_score += min(details['clr_assemblies'] * 20, 20)

        return {
            'complexity_score': min(complexity_score, 100),
            'complexity_level': self.categorize_complexity(complexity_score),
            'details': details
        }

    def categorize_complexity(self, score):
        if score < 30:
            return 'LOW'
        elif score < 60:
            return 'MEDIUM'
        elif score < 80:
            return 'HIGH'
        else:
            return 'VERY_HIGH'

    def analyze_stored_procedures(self, database_name):
        db = self._safe_db(database_name)
        self.cursor.execute(f"""
            SELECT 
                p.name,
                m.definition AS definition,
                p.create_date,
                p.modify_date
            FROM [{db}].sys.procedures p
            LEFT JOIN [{db}].sys.sql_modules m ON m.object_id = p.object_id
        """)

        sp_analysis = []
        incompatible_features = Counter()

        for row in self.cursor.fetchall():
            sp_def = (row.definition or '').upper()
            issues = []

            if 'CURSOR' in sp_def:
                issues.append('Uses cursors (may need refactoring)')
                incompatible_features['cursors'] += 1

            if 'RAISERROR' in sp_def:
                issues.append('Uses RAISERROR (PostgreSQL uses RAISE)')
                incompatible_features['raiserror'] += 1

            if 'TRY' in sp_def and 'CATCH' in sp_def:
                issues.append('Uses TRY-CATCH (PostgreSQL uses EXCEPTION blocks)')
                incompatible_features['try_catch'] += 1

            if 'EXEC(' in sp_def or 'EXECUTE(' in sp_def:
                issues.append('Uses dynamic SQL')
                incompatible_features['dynamic_sql'] += 1

            if ' OUTPUT ' in f" {sp_def} ":
                issues.append('Uses OUTPUT parameters')
                incompatible_features['output_params'] += 1

            if '@@' in sp_def:
                issues.append('Uses global variables (@@)')
                incompatible_features['global_vars'] += 1

            if issues:
                sp_analysis.append({
                    'name': row.name,
                    'created': str(row.create_date),
                    'modified': str(row.modify_date),
                    'issues': issues,
                    'complexity': 'HIGH' if len(issues) > 3 else 'MEDIUM'
                })

        return {
            'total_procedures': len(sp_analysis),
            'procedures_with_issues': len(sp_analysis),
            'incompatible_features': dict(incompatible_features),
            'detailed_analysis': sp_analysis
        }

    def analyze_data_types(self, database_name):
        db = self._safe_db(database_name)
        self.cursor.execute(f"""
            SELECT 
                t.name AS table_name,
                c.name AS column_name,
                ty.name AS data_type,
                c.max_length,
                c.precision,
                c.scale,
                c.is_nullable,
                c.is_identity
            FROM [{db}].sys.tables t
            JOIN [{db}].sys.columns c ON t.object_id = c.object_id
            JOIN [{db}].sys.types ty ON c.user_type_id = ty.user_type_id
            ORDER BY t.name, c.column_id
        """)

        type_mapping = {
            'NVARCHAR': ('VARCHAR', 'TEXT', 'Compatible with adjustments'),
            'VARCHAR': ('VARCHAR', 'TEXT', 'Direct compatibility'),
            'INT': ('INTEGER', 'INT', 'Direct compatibility'),
            'BIGINT': ('BIGINT', 'BIGINT', 'Direct compatibility'),
            'SMALLINT': ('SMALLINT', 'SMALLINT', 'Direct compatibility'),
            'TINYINT': ('SMALLINT', 'SMALLINT', 'PostgreSQL minimum is SMALLINT'),
            'BIT': ('BOOLEAN', 'BOOLEAN', 'Direct compatibility'),
            'DATETIME': ('TIMESTAMP', 'TIMESTAMP', 'Compatible'),
            'DATETIME2': ('TIMESTAMP', 'TIMESTAMP', 'Compatible'),
            'DATE': ('DATE', 'DATE', 'Direct compatibility'),
            'TIME': ('TIME', 'TIME', 'Direct compatibility'),
            'DECIMAL': ('DECIMAL', 'NUMERIC', 'Direct compatibility'),
            'NUMERIC': ('NUMERIC', 'NUMERIC', 'Direct compatibility'),
            'MONEY': ('DECIMAL(19,4)', 'NUMERIC', 'Requires explicit mapping'),
            'UNIQUEIDENTIFIER': ('UUID', 'UUID', 'Requires extension'),
            'XML': ('XML', 'XML', 'Direct compatibility'),
            'VARBINARY': ('BYTEA', 'BYTEA', 'Compatible'),
            'IMAGE': ('BYTEA', 'BYTEA', 'Deprecated in SQL Server, use BYTEA'),
            'TEXT': ('TEXT', 'TEXT', 'Deprecated in SQL Server'),
            'NTEXT': ('TEXT', 'TEXT', 'Deprecated in SQL Server')
        }

        type_usage = Counter()
        compatibility_issues = []

        for row in self.cursor.fetchall():
            data_type = row.data_type.upper()
            type_usage[data_type] += 1

            if data_type not in type_mapping:
                compatibility_issues.append({
                    'table': row.table_name,
                    'column': row.column_name,
                    'type': data_type,
                    'issue': 'Unknown type mapping',
                    'severity': 'HIGH'
                })
            elif 'Requires' in type_mapping[data_type][2]:
                compatibility_issues.append({
                    'table': row.table_name,
                    'column': row.column_name,
                    'type': data_type,
                    'postgresql_type': type_mapping[data_type][0],
                    'note': type_mapping[data_type][2],
                    'severity': 'MEDIUM'
                })

        return {
            'type_usage': dict(type_usage),
            'compatibility_issues': compatibility_issues,
            'total_columns_analyzed': sum(type_usage.values())
        }

    def analyze_indexes(self, database_name):
        """Analyze index structure and compatibility"""
        db = self._safe_db(database_name)
        self.cursor.execute(f"""
            WITH idx AS (
                SELECT 
                    t.object_id,
                    t.name AS table_name,
                    i.index_id,
                    i.name AS index_name,
                    i.type_desc,
                    i.is_unique,
                    i.is_primary_key,
                    i.fill_factor
                FROM [{db}].sys.indexes i
                JOIN [{db}].sys.tables t ON i.object_id = t.object_id
                WHERE i.type > 0  -- Exclude heaps
            )
            SELECT 
                idx.table_name,
                idx.index_name,
                idx.type_desc,
                idx.is_unique,
                idx.is_primary_key,
                idx.fill_factor,
                -- key columns with order
                STUFF((
                    SELECT ', ' + c.name + CASE WHEN ic.is_descending_key = 1 THEN ' DESC' ELSE ' ASC' END
                    FROM [{db}].sys.index_columns ic
                    JOIN [{db}].sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                    WHERE ic.object_id = idx.object_id AND ic.index_id = idx.index_id AND ic.is_included_column = 0
                    ORDER BY ic.key_ordinal
                    FOR XML PATH('')
                ), 1, 2, '') AS key_columns,
                -- included columns
                STUFF((
                    SELECT ', ' + c.name
                    FROM [{db}].sys.index_columns ic
                    JOIN [{db}].sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                    WHERE ic.object_id = idx.object_id AND ic.index_id = idx.index_id AND ic.is_included_column = 1
                    FOR XML PATH('')
                ), 1, 2, '') AS included_columns
            FROM idx
        """)

        index_analysis = []
        index_types = Counter()

        for row in self.cursor.fetchall():
            index_types[row.type_desc] += 1

            migration_notes = []
            if row.type_desc == 'CLUSTERED':
                migration_notes.append('Clustered index → Map to primary key or regular index in PostgreSQL')
            if row.type_desc == 'NONCLUSTERED COLUMNSTORE':
                migration_notes.append('Columnstore index → Consider PostgreSQL BRIN or regular B-tree')
            if row.included_columns:
                migration_notes.append('Has included columns → PostgreSQL supports INCLUDE in indexes (PG11+)')
            if row.fill_factor and row.fill_factor < 100:
                migration_notes.append(f'Fill factor {row.fill_factor} → Use FILLFACTOR in PostgreSQL')

            index_analysis.append({
                'table': row.table_name,
                'index_name': row.index_name,
                'type': row.type_desc,
                'is_unique': bool(row.is_unique),
                'is_primary_key': bool(row.is_primary_key),
                'key_columns': row.key_columns,
                'included_columns': row.included_columns,
                'migration_notes': migration_notes
            })

        return {
            'total_indexes': len(index_analysis),
            'index_types': dict(index_types),
            'detailed_analysis': index_analysis
        }

    def generate_migration_phases(self, assessment_results):
        complexity = assessment_results['schema_complexity']['complexity_level']

        if complexity == 'LOW':
            phases = [
                {
                    'phase': 1,
                    'name': 'Schema Migration',
                    'duration_estimate': '1-2 weeks',
                    'tasks': [
                        'Export schema using AWS SCT',
                        'Convert data types',
                        'Create tables in Aurora PostgreSQL',
                        'Migrate indexes'
                    ]
                },
                {
                    'phase': 2,
                    'name': 'Data Migration',
                    'duration_estimate': '1 week',
                    'tasks': [
                        'Setup AWS DMS replication',
                        'Full load migration',
                        'Validate data integrity'
                    ]
                },
                {
                    'phase': 3,
                    'name': 'Application Migration',
                    'duration_estimate': '1-2 weeks',
                    'tasks': [
                        'Update connection strings',
                        'Test application functionality',
                        'Performance tuning'
                    ]
                }
            ]
        elif complexity in ['MEDIUM', 'HIGH']:
            phases = [
                {
                    'phase': 1,
                    'name': 'Assessment & Planning',
                    'duration_estimate': '2-3 weeks',
                    'tasks': [
                        'Detailed compatibility analysis',
                        'Run Babelfish Compass',
                        'Identify high-risk objects',
                        'Plan mitigation strategies'
                    ]
                },
                {
                    'phase': 2,
                    'name': 'Schema Conversion',
                    'duration_estimate': '3-4 weeks',
                    'tasks': [
                        'Convert tables and views',
                        'Migrate indexes and constraints',
                        'Setup partitioning if needed',
                        'Create sequences'
                    ]
                },
                {
                    'phase': 3,
                    'name': 'Code Migration',
                    'duration_estimate': '4-6 weeks',
                    'tasks': [
                        'Convert stored procedures to PL/pgSQL',
                        'Rewrite incompatible constructs',
                        'Migrate triggers',
                        'Update dynamic SQL'
                    ]
                },
                {
                    'phase': 4,
                    'name': 'Data Migration',
                    'duration_estimate': '2-3 weeks',
                    'tasks': [
                        'Setup AWS DMS',
                        'Perform initial full load',
                        'Setup CDC for incremental sync',
                        'Data validation'
                    ]
                },
                {
                    'phase': 5,
                    'name': 'Testing & Validation',
                    'duration_estimate': '3-4 weeks',
                    'tasks': [
                        'Functional testing',
                        'Performance testing',
                        'User acceptance testing',
                        'Security validation'
                    ]
                },
                {
                    'phase': 6,
                    'name': 'Cutover',
                    'duration_estimate': '1 week',
                    'tasks': [
                        'Final data sync',
                        'DNS/connection string updates',
                        'Monitoring setup',
                        'Rollback plan ready'
                    ]
                }
            ]
        else:
            phases = [
                {
                    'phase': 0,
                    'name': 'Pre-Migration POC',
                    'duration_estimate': '4-6 weeks',
                    'tasks': [
                        'Select representative subset',
                        'Pilot migration',
                        'Identify major blockers',
                        'Refine approach'
                    ]
                }
            ]
        return phases

    def calculate_migration_score(self, assessment_results):
        """Calculate overall migration readiness score (0-100)"""
        score = 100

        # Deduct for schema complexity
        complexity_score = assessment_results['schema_complexity']['complexity_score']
        score -= complexity_score * 0.3

        # Deduct for stored procedure issues
        sp_issues = assessment_results['stored_procedure_analysis']['procedures_with_issues']
        total_sp = assessment_results['stored_procedure_analysis']['total_procedures']
        if total_sp > 0:
            sp_ratio = sp_issues / total_sp
            score -= sp_ratio * 20

        # Deduct for data type incompatibilities
        type_issues = len(assessment_results['data_type_analysis']['compatibility_issues'])
        total_cols = assessment_results['data_type_analysis']['total_columns_analyzed']
        if total_cols > 0:
            type_ratio = type_issues / total_cols
            score -= type_ratio * 15

        clr_count = assessment_results['schema_complexity']['details'].get('clr_assemblies', 0)
        if clr_count > 0:
            score -= min(clr_count * 10, 20)

        return max(score, 0)

    def generate_assessment_report(self, database_name, output_file='outputs/migration_readiness.json'):
        # Validate database first
        if not self._db_exists(str(database_name)):
            raise ValueError(f"Target database '{database_name}' does not exist on this server.")

        print(f"Assessing database: {database_name}")

        assessment_results = {
            'database': database_name,
            'assessment_date': datetime.now().isoformat(),
            'schema_complexity': self.assess_schema_complexity(database_name),
            'stored_procedure_analysis': self.analyze_stored_procedures(database_name),
            'data_type_analysis': self.analyze_data_types(database_name),
            'index_analysis': self.analyze_indexes(database_name)
        }

        # Calculate scores
        readiness_score = self.calculate_migration_score(assessment_results)

        # Generate phases
        migration_phases = self.generate_migration_phases(assessment_results)

        # Add to report
        assessment_results['readiness_score'] = readiness_score
        assessment_results['readiness_level'] = (
            'READY' if readiness_score > 70 else
            'MODERATE' if readiness_score > 50 else
            'CHALLENGING' if readiness_score > 30 else
            'REQUIRES_SIGNIFICANT_EFFORT'
        )
        assessment_results['migration_phases'] = migration_phases

        os.makedirs(os.path.dirname(output_file), exist_ok=True)

        # Export
        with open(output_file, 'w') as f:
            json.dump(assessment_results, f, indent=2, default=str)

        print(f"\nAssessment complete!")
        print(f"Readiness Score: {readiness_score:.1f}/100")
        print(f"Readiness Level: {assessment_results['readiness_level']}")
        print(f"Report saved: {output_file}")

        return assessment_results

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

    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SQL_SERVER},{SQL_PORT};"
        f"DATABASE={SQL_DATABASE};"
        f"UID={SQL_USERNAME};"
        f"PWD={SQL_PASSWORD}"
    )

    assessor = MigrationReadinessAssessor(conn_str)
    try:

        database_to_assess = SQL_DATABASE
        assessment = assessor.generate_assessment_report(
            database_to_assess,
            output_file='outputs/migration_readiness.json'
        )
    finally:
        assessor.close()

    print("\n=== GenAI Integration ===")
    print("Ask GitHub Copilot/Claude:")
    print("'Review outputs/migration_readiness.json and generate a detailed migration plan with risk mitigation strategies'")
