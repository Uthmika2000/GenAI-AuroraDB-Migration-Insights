"""
Use Case 10: Application Code Scanner
Scans application code files to identify SQL Server-specific dependencies, connection strings,
and queries that need modification for PostgreSQL migration
"""

import os
import re
import json
import sys
import argparse
from datetime import datetime
from pathlib import Path
import pandas as pd

class ApplicationCodeScanner:
    PATTERNS = {
        'connection_strings': [
            r'Server\s*=',
            r'Data Source\s*=',
            r'Initial Catalog\s*=',
            r'Integrated Security\s*=',
            r'SqlConnection',
            r'System\.Data\.SqlClient',
            r'Microsoft\.Data\.SqlClient'
        ],
        'tsql_syntax': [
            r'\bTOP\s+\d+\b',
            r'\bGETDATE\(\)',
            r'\bDATEADD\(',
            r'\bDATEDIFF\(',
            r'\bISNULL\(',
            r'\bCHARINDEX\(',
            r'\bLEN\(',
            r'\[dbo\]\.',
            r'@@ROWCOUNT',
            r'@@IDENTITY',
            r'@@ERROR',
            r'\bNOLOCK\b',
            r'\bWITH\s*\(NOLOCK\)',
            r'\bSET NOCOUNT ON\b',
            r'\bRAISERROR\b',
            r'\bBEGIN TRAN',
            r'\bCONVERT\(',
            r'\bCASTing'
        ],
        'stored_procedures': [
            r'EXEC\s+\[?\w+\]?\.\[?\w+\]?',
            r'EXECUTE\s+\[?\w+\]?\.\[?\w+\]?',
            r'sp_executesql'
        ],
        'sql_server_types': [
            r'\bUNIQUEIDENTIFIER\b',
            r'\bDATETIME2\b',
            r'\bDATETIMEOFFSET\b',
            r'\bHIERARCHYID\b',
            r'\bGEOMETRY\b',
            r'\bGEOGRAPHY\b',
            r'\bXML\b'
        ]
    }
    
    SUPPORTED_EXTENSIONS = {
        '.cs', '.vb', '.java', '.py', '.js', '.ts', 
        '.php', '.rb', '.go', '.sql', '.xml', '.config',
        '.json', '.properties', '.yaml', '.yml'
    }
    
    def __init__(self, scan_directory: str):
        self.scan_dir = scan_directory
        self.output_dir = "code_scan_results"
        self.scan_results = []
        
    def create_output_directory(self):
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        print(f" Output directory created: {self.output_dir}/")
    
    def scan_file(self, file_path: str) -> dict:
        findings = {
            'file_path': file_path,
            'file_name': os.path.basename(file_path),
            'file_extension': os.path.splitext(file_path)[1],
            'file_size': os.path.getsize(file_path),
            'issues': [],
            'connection_strings_found': 0,
            'tsql_syntax_found': 0,
            'stored_procedures_found': 0,
            'sql_server_types_found': 0,
            'total_issues': 0
        }
        
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
                line_number = 0
                
                for line in content.split('\n'):
                    line_number += 1
                    
                    for pattern in self.PATTERNS['connection_strings']:
                        if re.search(pattern, line, re.IGNORECASE):
                            findings['issues'].append({
                                'line': line_number,
                                'category': 'CONNECTION_STRING',
                                'severity': 'HIGH',
                                'pattern': pattern,
                                'code_snippet': line.strip()[:100],
                                'recommendation': 'Update connection string for PostgreSQL (Host, Port, Database, Username, Password)'
                            })
                            findings['connection_strings_found'] += 1
                    
                    for pattern in self.PATTERNS['tsql_syntax']:
                        matches = re.finditer(pattern, line, re.IGNORECASE)
                        for match in matches:
                            findings['issues'].append({
                                'line': line_number,
                                'category': 'TSQL_SYNTAX',
                                'severity': 'MEDIUM',
                                'pattern': pattern,
                                'matched_text': match.group(),
                                'code_snippet': line.strip()[:100],
                                'recommendation': self.get_postgresql_alternative(match.group())
                            })
                            findings['tsql_syntax_found'] += 1
                    
                    for pattern in self.PATTERNS['stored_procedures']:
                        matches = re.finditer(pattern, line, re.IGNORECASE)
                        for match in matches:
                            findings['issues'].append({
                                'line': line_number,
                                'category': 'STORED_PROCEDURE',
                                'severity': 'HIGH',
                                'pattern': pattern,
                                'matched_text': match.group(),
                                'code_snippet': line.strip()[:100],
                                'recommendation': 'Verify stored procedure exists in PostgreSQL and update call syntax if needed'
                            })
                            findings['stored_procedures_found'] += 1
                    
                    for pattern in self.PATTERNS['sql_server_types']:
                        matches = re.finditer(pattern, line, re.IGNORECASE)
                        for match in matches:
                            findings['issues'].append({
                                'line': line_number,
                                'category': 'DATA_TYPE',
                                'severity': 'MEDIUM',
                                'pattern': pattern,
                                'matched_text': match.group(),
                                'code_snippet': line.strip()[:100],
                                'recommendation': self.get_postgresql_type_mapping(match.group())
                            })
                            findings['sql_server_types_found'] += 1
                
                findings['total_issues'] = len(findings['issues'])
                
        except Exception as e:
            findings['scan_error'] = str(e)
        
        return findings
    
    def get_postgresql_alternative(self, tsql_element: str) -> str:
        alternatives = {
            'GETDATE()': 'Use CURRENT_TIMESTAMP or NOW()',
            'DATEADD': 'Use date/time arithmetic (e.g., timestamp + INTERVAL \'1 day\')',
            'DATEDIFF': 'Use AGE() function or date subtraction',
            'ISNULL': 'Use COALESCE() function',
            'CHARINDEX': 'Use POSITION() or STRPOS()',
            'LEN': 'Use LENGTH() or CHAR_LENGTH()',
            'TOP': 'Use LIMIT clause',
            '@@ROWCOUNT': 'Use GET DIAGNOSTICS row_count = ROW_COUNT',
            '@@IDENTITY': 'Use RETURNING clause or LASTVAL()',
            '@@ERROR': 'Use exception handling with SQLSTATE',
            'NOLOCK': 'Review isolation level requirements',
            'SET NOCOUNT ON': 'Not needed in PostgreSQL functions',
            'RAISERROR': 'Use RAISE EXCEPTION',
            'BEGIN TRAN': 'Use BEGIN (PostgreSQL)',
            'CONVERT': 'Use CAST() or :: operator'
        }
        
        for key, value in alternatives.items():
            if key.upper() in tsql_element.upper():
                return value
        
        return 'Consult PostgreSQL documentation for equivalent syntax'
    
    def get_postgresql_type_mapping(self, sql_type: str) -> str:
        type_map = {
            'UNIQUEIDENTIFIER': 'Use UUID type (requires uuid-ossp extension)',
            'DATETIME2': 'Use TIMESTAMP',
            'DATETIMEOFFSET': 'Use TIMESTAMP WITH TIME ZONE',
            'HIERARCHYID': 'Use ltree extension or redesign hierarchy',
            'GEOMETRY': 'Use PostGIS extension',
            'GEOGRAPHY': 'Use PostGIS extension',
            'XML': 'Use XML type (native support available)'
        }
        
        for key, value in type_map.items():
            if key.upper() in sql_type.upper():
                return value
        
        return 'Review data type compatibility'
    
    def scan_directory(self, directory: str = None, recursive: bool = True) -> list:
        if directory is None:
            directory = self.scan_dir
        
        print(f"\n🔍 Scanning directory: {directory}")
        print(f"Recursive: {recursive}")
        print(f"Supported extensions: {', '.join(sorted(self.SUPPORTED_EXTENSIONS))}\n")
        
        files_scanned = 0
        files_with_issues = 0
        
        if recursive:
            for root, dirs, files in os.walk(directory):
                dirs[:] = [d for d in dirs if d not in [
                    'node_modules', '.git', '.svn', 'bin', 'obj', 
                    '__pycache__', 'venv', '.venv', 'dist', 'build'
                ]]
                
                for file in files:
                    file_path = os.path.join(root, file)
                    ext = os.path.splitext(file)[1].lower()
                    
                    if ext in self.SUPPORTED_EXTENSIONS:
                        result = self.scan_file(file_path)
                        if result['total_issues'] > 0:
                            self.scan_results.append(result)
                            files_with_issues += 1
                        files_scanned += 1
                        
                        if files_scanned % 100 == 0:
                            print(f"  Scanned {files_scanned} files...")
        else:
            for file in os.listdir(directory):
                file_path = os.path.join(directory, file)
                if os.path.isfile(file_path):
                    ext = os.path.splitext(file)[1].lower()
                    if ext in self.SUPPORTED_EXTENSIONS:
                        result = self.scan_file(file_path)
                        if result['total_issues'] > 0:
                            self.scan_results.append(result)
                            files_with_issues += 1
                        files_scanned += 1
        
        print(f"\n Scan complete!")
        print(f"  Files scanned: {files_scanned}")
        print(f"  Files with issues: {files_with_issues}")
        
        return self.scan_results
    
    def generate_summary_report(self) -> dict:
        if not self.scan_results:
            return {
                'status': 'NO_ISSUES',
                'message': 'No SQL Server dependencies found'
            }
        
        summary = {
            'scan_date': datetime.now().isoformat(),
            'scan_directory': self.scan_dir,
            'total_files_scanned': len(self.scan_results),
            'statistics': {
                'total_issues': sum(r['total_issues'] for r in self.scan_results),
                'connection_strings': sum(r['connection_strings_found'] for r in self.scan_results),
                'tsql_syntax': sum(r['tsql_syntax_found'] for r in self.scan_results),
                'stored_procedures': sum(r['stored_procedures_found'] for r in self.scan_results),
                'sql_server_types': sum(r['sql_server_types_found'] for r in self.scan_results)
            },
            'files_by_extension': {},
            'severity_breakdown': {'HIGH': 0, 'MEDIUM': 0, 'LOW': 0},
            'category_breakdown': {
                'CONNECTION_STRING': 0,
                'TSQL_SYNTAX': 0,
                'STORED_PROCEDURE': 0,
                'DATA_TYPE': 0
            },
            'top_files_with_issues': []
        }
        
        for result in self.scan_results:
            ext = result['file_extension']
            if ext not in summary['files_by_extension']:
                summary['files_by_extension'][ext] = 0
            summary['files_by_extension'][ext] += 1
            
            for issue in result['issues']:
                summary['severity_breakdown'][issue['severity']] += 1
                summary['category_breakdown'][issue['category']] += 1
        
        sorted_results = sorted(self.scan_results, key=lambda x: x['total_issues'], reverse=True)
        summary['top_files_with_issues'] = [
            {
                'file_path': r['file_path'],
                'file_name': r['file_name'],
                'total_issues': r['total_issues'],
                'connection_strings': r['connection_strings_found'],
                'tsql_syntax': r['tsql_syntax_found'],
                'stored_procedures': r['stored_procedures_found']
            }
            for r in sorted_results[:20]
        ]
        
        return summary
    
    def generate_remediation_plan(self, summary: dict) -> dict:
        plan = {
            'priority_levels': [],
            'estimated_effort': {},
            'recommendations': []
        }
        
        if summary['statistics']['connection_strings'] > 0:
            plan['priority_levels'].append({
                'priority': 1,
                'category': 'Connection Strings',
                'count': summary['statistics']['connection_strings'],
                'urgency': 'CRITICAL',
                'description': 'Must be updated for database cutover',
                'tasks': [
                    'Identify all connection string locations',
                    'Update to PostgreSQL format',
                    'Test connections in staging environment',
                    'Update configuration management systems'
                ]
            })
        
        if summary['statistics']['stored_procedures'] > 0:
            plan['priority_levels'].append({
                'priority': 2,
                'category': 'Stored Procedure Calls',
                'count': summary['statistics']['stored_procedures'],
                'urgency': 'HIGH',
                'description': 'Verify converted procedures and update call syntax',
                'tasks': [
                    'Verify all called procedures are converted to PL/pgSQL',
                    'Update call syntax if needed',
                    'Test procedure functionality',
                    'Update error handling'
                ]
            })
        
        if summary['statistics']['tsql_syntax'] > 0:
            plan['priority_levels'].append({
                'priority': 3,
                'category': 'T-SQL Syntax',
                'count': summary['statistics']['tsql_syntax'],
                'urgency': 'MEDIUM',
                'description': 'Update SQL queries to PostgreSQL syntax',
                'tasks': [
                    'Review and update each query',
                    'Test query results',
                    'Optimize for PostgreSQL',
                    'Update documentation'
                ]
            })
        
        if summary['statistics']['sql_server_types'] > 0:
            plan['priority_levels'].append({
                'priority': 4,
                'category': 'SQL Server Data Types',
                'count': summary['statistics']['sql_server_types'],
                'urgency': 'MEDIUM',
                'description': 'Update data type handling in application code',
                'tasks': [
                    'Review data type conversions',
                    'Update ORM mappings',
                    'Test data serialization',
                    'Verify data integrity'
                ]
            })
        
        total_issues = summary['statistics']['total_issues']
        plan['estimated_effort'] = {
            'total_issues': total_issues,
            'estimated_hours': total_issues * 0.5,
            'estimated_days': round((total_issues * 0.5) / 8, 1),
            'recommended_team_size': '2-3 developers',
            'estimated_calendar_weeks': round((total_issues * 0.5) / (8 * 2), 1)
        }
        
        plan['recommendations'] = [
            'Create a dedicated branch for PostgreSQL compatibility changes',
            'Update one file at a time and test thoroughly',
            'Use automated testing to verify changes',
            'Document all changes for team knowledge sharing',
            'Consider using database abstraction layers (ORMs) for future portability',
            'Plan for regression testing across all affected modules',
            'Keep original SQL Server compatibility temporarily with feature flags'
        ]
        
        return plan
    
    def export_detailed_report(self, output_file: str = 'code_scan_detailed.json'):
        report = {
            'metadata': {
                'scan_date': datetime.now().isoformat(),
                'scan_directory': self.scan_dir,
                'total_files_scanned': len(self.scan_results)
            },
            'detailed_results': self.scan_results
        }
        
        output_path = os.path.join(self.output_dir, output_file)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, default=str)
        
        print(f" Detailed report saved: {output_path}")
    
    def export_excel_report(self, output_file: str = 'code_scan_report.xlsx'):
        if not self.scan_results:
            print(" No scan results to export")
            return
        
        issues_data = []
        for result in self.scan_results:
            for issue in result['issues']:
                issues_data.append({
                    'File': result['file_path'],
                    'Line': issue['line'],
                    'Category': issue['category'],
                    'Severity': issue['severity'],
                    'Pattern': issue['pattern'],
                    'Matched Text': issue.get('matched_text', ''),
                    'Code Snippet': issue['code_snippet'],
                    'Recommendation': issue['recommendation']
                })
        
        df = pd.DataFrame(issues_data)
        
        output_path = os.path.join(self.output_dir, output_file)
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Issues', index=False)
            
            summary_data = {
                'Metric': ['Total Files', 'Total Issues', 'Connection Strings', 
                          'T-SQL Syntax', 'Stored Procedures', 'Data Types'],
                'Count': [
                    len(self.scan_results),
                    sum(r['total_issues'] for r in self.scan_results),
                    sum(r['connection_strings_found'] for r in self.scan_results),
                    sum(r['tsql_syntax_found'] for r in self.scan_results),
                    sum(r['stored_procedures_found'] for r in self.scan_results),
                    sum(r['sql_server_types_found'] for r in self.scan_results)
                ]
            }
            pd.DataFrame(summary_data).to_excel(writer, sheet_name='Summary', index=False)
        
        print(f" Excel report saved: {output_path}")
    
    def generate_all_reports(self):
        print(f"\n Generating comprehensive code scan reports...")

        self.create_output_directory()

        print("\n  • Generating summary report...")
        summary = self.generate_summary_report()
        summary_path = os.path.join(self.output_dir, 'scan_summary.json')
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, default=str)
        
        print("  • Generating remediation plan...")
        remediation = self.generate_remediation_plan(summary)
        remediation_path = os.path.join(self.output_dir, 'remediation_plan.json')
        with open(remediation_path, 'w', encoding='utf-8') as f:
            json.dump(remediation, f, indent=2, default=str)
        
        print("  • Exporting detailed results...")
        self.export_detailed_report()
        
        print("  • Creating Excel report...")
        self.export_excel_report()
        
        print("\n All reports generated successfully!")
        
        print("\n" + "="*70)
        print("APPLICATION CODE SCAN SUMMARY")
        print("="*70)
        print(f"Scan Directory: {self.scan_dir}")
        print(f"Files Scanned: {summary['total_files_scanned']}")
        print(f"\nIssues Found:")
        print(f"  Total Issues: {summary['statistics']['total_issues']}")
        print(f"  Connection Strings: {summary['statistics']['connection_strings']}")
        print(f"  T-SQL Syntax: {summary['statistics']['tsql_syntax']}")
        print(f"  Stored Procedures: {summary['statistics']['stored_procedures']}")
        print(f"  SQL Server Types: {summary['statistics']['sql_server_types']}")
        print(f"\nSeverity Breakdown:")
        for severity, count in summary['severity_breakdown'].items():
            print(f"  {severity}: {count}")
        print(f"\nEstimated Remediation Effort:")
        print(f"  Total Hours: {remediation['estimated_effort']['estimated_hours']:.1f}")
        print(f"  Total Days: {remediation['estimated_effort']['estimated_days']}")
        print(f"  Estimated Weeks: {remediation['estimated_effort']['estimated_calendar_weeks']}")
        print(f"\nGenerated Files in {self.output_dir}/:")
        print("  • scan_summary.json")
        print("  • remediation_plan.json")
        print("  • code_scan_detailed.json")
        print("  • code_scan_report.xlsx")
        
        return summary, remediation

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Application Code Scanner for SQL Server to PostgreSQL Migration')
    parser.add_argument('--directory', type=str, help='Application directory path to scan')
    parser.add_argument('--recursive', action='store_true', default=True, help='Scan directories recursively (default: True)')
    parser.add_argument('--no-recursive', dest='recursive', action='store_false', help='Do not scan recursively')
    parser.add_argument('--extensions', type=str, help='Comma-separated list of file extensions (e.g., .cs,.java,.py)')
    
    args = parser.parse_args()
    
    if args.directory:
        application_directory = args.directory
    else:
        print("\n" + "="*70)
        print("APPLICATION CODE SCANNER")
        print("="*70)
        print("\nNo directory specified. Please provide a path to scan.")
        print("Example: python 10_application_code_scanner.py --directory /path/to/code")
        print("\nOr enter a path now:")
        application_directory = input("Enter application directory path to scan (or press Enter for current directory): ").strip()
        
        if not application_directory:
            application_directory = "."
            print(f"Using current directory: {os.path.abspath(application_directory)}")
    
    if not os.path.exists(application_directory):
        print(f"\n Error: Directory not found: {application_directory}")
        print("\nPlease provide a valid directory path.")
        sys.exit(1)
    
    if not os.path.isdir(application_directory):
        print(f"\n Error: Path is not a directory: {application_directory}")
        sys.exit(1)
    
    print("\n" + "="*70)
    print("APPLICATION CODE SCANNER")
    print("="*70)
    print(f"Scan Directory: {os.path.abspath(application_directory)}")
    print(f"Recursive Scan: {args.recursive}")
    print("="*70)
    
    scanner = ApplicationCodeScanner(application_directory)
    
    if args.extensions:
        custom_extensions = {ext.strip() if ext.startswith('.') else f'.{ext.strip()}' 
                           for ext in args.extensions.split(',')}
        scanner.SUPPORTED_EXTENSIONS = custom_extensions
        print(f"Custom extensions: {', '.join(sorted(custom_extensions))}")
    
    try:
        # Perform scan
        results = scanner.scan_directory(recursive=args.recursive)
        
        if results:
            summary, remediation = scanner.generate_all_reports()
            print("\n Code scan completed successfully!")
            print(f"\nView results:")
            print(f"  • Excel Report: {scanner.output_dir}/code_scan_report.xlsx")
            print(f"  • JSON Reports: {scanner.output_dir}/")
        else:
            print("\n No SQL Server dependencies found in scanned files!")
            print("This is good news - no application code changes needed!")
    
    except KeyboardInterrupt:
        print("\n\n Scan interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n Error during scan: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)