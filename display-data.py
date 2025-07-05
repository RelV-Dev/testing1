#!/usr/bin/env python3
"""
Supabase Data Explorer & Backup Tool
Menampilkan semua data di Supabase dan memungkinkan backup selektif
"""

import requests
import json
import os
import pandas as pd
from datetime import datetime
from typing import Dict, List, Any, Optional
import sys

class SupabaseExplorer:
    def __init__(self, api_url: str, api_key: str, auth_token: str):
        self.api_url = api_url.rstrip('/')
        self.api_key = api_key
        self.auth_token = auth_token
        self.headers = {
            'apikey': self.api_key,
            'Authorization': f'Bearer {self.auth_token}',
            'Content-Type': 'application/json'
        }
        
    def get_all_schemas(self) -> List[Dict]:
        """Mendapatkan semua schema dalam database"""
        try:
            query = """
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
            ORDER BY schema_name
            """
            
            response = requests.get(
                f"{self.api_url}/rpc/execute_sql",
                headers=self.headers,
                json={"query": query}
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                # Fallback: coba langsung ambil dari schema yang umum
                return [
                    {'schema_name': 'public'},
                    {'schema_name': 'auth'},
                    {'schema_name': 'storage'},
                    {'schema_name': 'realtime'}
                ]
        except Exception as e:
            print(f"Error getting schemas: {e}")
            return [{'schema_name': 'public'}]
    
    def get_tables_in_schema(self, schema_name: str) -> List[Dict]:
        """Mendapatkan semua tabel dalam schema tertentu"""
        try:
            # Untuk schema public, bisa akses langsung via REST API
            if schema_name == 'public':
                # Coba ambil metadata dari information_schema
                response = requests.get(
                    f"{self.api_url}/information_schema.tables",
                    headers=self.headers,
                    params={
                        'table_schema': f'eq.{schema_name}',
                        'select': 'table_name,table_type'
                    }
                )
                
                if response.status_code == 200:
                    return response.json()
                else:
                    # Fallback: coba deteksi tabel dengan mencoba akses
                    return self._detect_public_tables()
            else:
                # Untuk schema lain, gunakan query SQL
                query = f"""
                SELECT table_name, table_type
                FROM information_schema.tables 
                WHERE table_schema = '{schema_name}'
                ORDER BY table_name
                """
                
                response = requests.get(
                    f"{self.api_url}/rpc/execute_sql",
                    headers=self.headers,
                    json={"query": query}
                )
                
                if response.status_code == 200:
                    return response.json()
                else:
                    return []
                    
        except Exception as e:
            print(f"Error getting tables for schema {schema_name}: {e}")
            return []
    
    def _detect_public_tables(self) -> List[Dict]:
        """Deteksi tabel di schema public dengan mencoba akses langsung"""
        common_tables = [
            'users', 'profiles', 'posts', 'comments', 'categories',
            'products', 'orders', 'customers', 'invoices', 'payments',
            'articles', 'pages', 'settings', 'logs', 'notifications'
        ]
        
        detected_tables = []
        for table in common_tables:
            try:
                response = requests.get(
                    f"{self.api_url}/{table}",
                    headers=self.headers,
                    params={'limit': 1}
                )
                
                if response.status_code == 200:
                    detected_tables.append({
                        'table_name': table,
                        'table_type': 'BASE TABLE'
                    })
            except:
                continue
                
        return detected_tables
    
    def get_table_data(self, table_name: str, schema: str = 'public', limit: int = None) -> Dict:
        """Mendapatkan data dari tabel tertentu"""
        try:
            if schema == 'public':
                url = f"{self.api_url}/{table_name}"
                params = {}
                if limit:
                    params['limit'] = limit
                
                response = requests.get(url, headers=self.headers, params=params)
                
                if response.status_code == 200:
                    data = response.json()
                    return {
                        'success': True,
                        'data': data,
                        'count': len(data),
                        'table_name': table_name,
                        'schema': schema
                    }
                else:
                    return {
                        'success': False,
                        'error': f"HTTP {response.status_code}: {response.text}",
                        'table_name': table_name,
                        'schema': schema
                    }
            else:
                # Untuk schema lain, gunakan query SQL
                query = f"SELECT * FROM {schema}.{table_name}"
                if limit:
                    query += f" LIMIT {limit}"
                
                response = requests.get(
                    f"{self.api_url}/rpc/execute_sql",
                    headers=self.headers,
                    json={"query": query}
                )
                
                if response.status_code == 200:
                    data = response.json()
                    return {
                        'success': True,
                        'data': data,
                        'count': len(data),
                        'table_name': table_name,
                        'schema': schema
                    }
                else:
                    return {
                        'success': False,
                        'error': f"HTTP {response.status_code}: {response.text}",
                        'table_name': table_name,
                        'schema': schema
                    }
                    
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'table_name': table_name,
                'schema': schema
            }
    
    def get_table_structure(self, table_name: str, schema: str = 'public') -> Dict:
        """Mendapatkan struktur tabel (kolom, tipe data, dll)"""
        try:
            query = f"""
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default,
                character_maximum_length
            FROM information_schema.columns 
            WHERE table_name = '{table_name}' 
            AND table_schema = '{schema}'
            ORDER BY ordinal_position
            """
            
            response = requests.get(
                f"{self.api_url}/rpc/execute_sql",
                headers=self.headers,
                json={"query": query}
            )
            
            if response.status_code == 200:
                return {
                    'success': True,
                    'structure': response.json(),
                    'table_name': table_name,
                    'schema': schema
                }
            else:
                return {
                    'success': False,
                    'error': f"HTTP {response.status_code}: {response.text}",
                    'table_name': table_name,
                    'schema': schema
                }
                
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'table_name': table_name,
                'schema': schema
            }
    
    def display_database_overview(self):
        """Menampilkan overview database"""
        print("=" * 60)
        print("SUPABASE DATABASE OVERVIEW")
        print("=" * 60)
        
        # Ambil semua schema
        schemas = self.get_all_schemas()
        
        total_tables = 0
        schema_table_map = {}
        
        for schema_info in schemas:
            schema_name = schema_info['schema_name']
            print(f"\n📁 SCHEMA: {schema_name}")
            print("-" * 40)
            
            tables = self.get_tables_in_schema(schema_name)
            schema_table_map[schema_name] = tables
            total_tables += len(tables)
            
            if tables:
                for i, table in enumerate(tables, 1):
                    table_name = table['table_name']
                    table_type = table.get('table_type', 'TABLE')
                    
                    # Coba ambil sample data untuk mengetahui jumlah record
                    data_info = self.get_table_data(table_name, schema_name, limit=1)
                    
                    if data_info['success']:
                        # Estimasi jumlah record (tidak akurat, hanya untuk gambaran)
                        record_status = "✅ Has data"
                    else:
                        record_status = "❌ No access/Empty"
                    
                    print(f"  {i:2d}. {table_name} ({table_type}) - {record_status}")
            else:
                print("  No tables found or no access")
        
        print(f"\n📊 SUMMARY:")
        print(f"   Total Schemas: {len(schemas)}")
        print(f"   Total Tables: {total_tables}")
        
        return schema_table_map
    
    def interactive_backup_selector(self, schema_table_map: Dict):
        """Interface interaktif untuk memilih tabel yang akan di-backup"""
        print("\n" + "=" * 60)
        print("BACKUP SELECTION")
        print("=" * 60)
        
        # Buat daftar semua tabel dengan nomor
        all_tables = []
        table_map = {}
        counter = 1
        
        for schema_name, tables in schema_table_map.items():
            for table in tables:
                table_info = {
                    'number': counter,
                    'schema': schema_name,
                    'table': table['table_name'],
                    'type': table.get('table_type', 'TABLE')
                }
                all_tables.append(table_info)
                table_map[counter] = table_info
                counter += 1
        
        # Tampilkan daftar
        print("\nAvailable tables:")
        for table_info in all_tables:
            print(f"  {table_info['number']:2d}. {table_info['schema']}.{table_info['table']} ({table_info['type']})")
        
        # Input dari user
        print(f"\nSelect tables to backup:")
        print("- Enter numbers separated by comma (e.g., 1,3,5)")
        print("- Enter 'all' to backup all tables")
        print("- Enter 'schema:public' to backup all tables in public schema")
        print("- Enter 'quit' to exit")
        
        while True:
            selection = input("\nYour selection: ").strip()
            
            if selection.lower() == 'quit':
                return []
            
            if selection.lower() == 'all':
                return all_tables
            
            if selection.lower().startswith('schema:'):
                schema_name = selection.split(':')[1]
                selected_tables = [t for t in all_tables if t['schema'] == schema_name]
                if selected_tables:
                    return selected_tables
                else:
                    print(f"No tables found in schema '{schema_name}'")
                    continue
            
            try:
                # Parse nomor yang dipilih
                numbers = [int(x.strip()) for x in selection.split(',')]
                selected_tables = []
                
                for num in numbers:
                    if num in table_map:
                        selected_tables.append(table_map[num])
                    else:
                        print(f"Invalid number: {num}")
                        break
                else:
                    return selected_tables
                    
            except ValueError:
                print("Invalid input. Please enter numbers separated by comma.")
    
    def backup_tables(self, selected_tables: List[Dict]):
        """Backup tabel yang dipilih"""
        if not selected_tables:
            print("No tables selected for backup.")
            return
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_dir = f"supabase_backup_{timestamp}"
        
        if not os.path.exists(backup_dir):
            os.makedirs(backup_dir)
        
        print(f"\n🚀 Starting backup to directory: {backup_dir}")
        print("=" * 60)
        
        backup_summary = []
        
        for table_info in selected_tables:
            schema = table_info['schema']
            table_name = table_info['table']
            
            print(f"\n📋 Backing up {schema}.{table_name}...")
            
            # Ambil struktur tabel
            structure = self.get_table_structure(table_name, schema)
            
            # Ambil data tabel
            data = self.get_table_data(table_name, schema)
            
            if data['success']:
                # Simpan sebagai JSON
                json_filename = f"{backup_dir}/{schema}_{table_name}.json"
                with open(json_filename, 'w', encoding='utf-8') as f:
                    json.dump({
                        'schema': schema,
                        'table': table_name,
                        'structure': structure.get('structure', []),
                        'data': data['data'],
                        'backup_time': datetime.now().isoformat(),
                        'record_count': data['count']
                    }, f, indent=2, ensure_ascii=False)
                
                # Jika ada data, simpan juga sebagai CSV
                if data['data'] and len(data['data']) > 0:
                    try:
                        csv_filename = f"{backup_dir}/{schema}_{table_name}.csv"
                        df = pd.DataFrame(data['data'])
                        df.to_csv(csv_filename, index=False, encoding='utf-8')
                        
                        backup_summary.append({
                            'schema': schema,
                            'table': table_name,
                            'status': 'Success',
                            'records': data['count'],
                            'files': ['JSON', 'CSV']
                        })
                        
                        print(f"   ✅ Success: {data['count']} records saved")
                        
                    except Exception as e:
                        backup_summary.append({
                            'schema': schema,
                            'table': table_name,
                            'status': 'Partial Success',
                            'records': data['count'],
                            'files': ['JSON'],
                            'error': f"CSV export failed: {str(e)}"
                        })
                        
                        print(f"   ⚠️  Partial: {data['count']} records (JSON only)")
                else:
                    backup_summary.append({
                        'schema': schema,
                        'table': table_name,
                        'status': 'Success (Empty)',
                        'records': 0,
                        'files': ['JSON']
                    })
                    
                    print(f"   ✅ Success: Table backed up (empty)")
            else:
                backup_summary.append({
                    'schema': schema,
                    'table': table_name,
                    'status': 'Failed',
                    'records': 0,
                    'files': [],
                    'error': data['error']
                })
                
                print(f"   ❌ Failed: {data['error']}")
        
        # Buat summary report
        summary_filename = f"{backup_dir}/backup_summary.json"
        with open(summary_filename, 'w', encoding='utf-8') as f:
            json.dump({
                'backup_time': datetime.now().isoformat(),
                'total_tables': len(selected_tables),
                'successful_backups': len([s for s in backup_summary if s['status'].startswith('Success')]),
                'failed_backups': len([s for s in backup_summary if s['status'] == 'Failed']),
                'tables': backup_summary
            }, f, indent=2, ensure_ascii=False)
        
        print(f"\n🎉 Backup completed!")
        print(f"   Directory: {backup_dir}")
        print(f"   Summary: {summary_filename}")
        
        return backup_dir, backup_summary

def main():
    """Main function"""
    # Konfigurasi Supabase
    API_URL = 'https://jcygvbkupixjhkijvfmi.supabase.co/rest/v1/'
    API_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoiYW5vbiIsImlhdCI6MTYxNzMzOTY2NiwiZXhwIjoxOTMyOTE1NjY2fQ.pakwp7Cud_wiQ6aipdnB6n1enK8k_SWaDt2RARnM930'
    AUTH_TOKEN = 'eyJhbGciOiJIUzI1NiIsImtpZCI6IkZPM29Dd3BvNDVvMGxCOHkiLCJ0eXAiOiJKV1QifQ.eyJpc3MiOiJodHRwczovL2pjeWd2Ymt1cGl4amhraWp2Zm1pLnN1cGFiYXNlLmNvL2F1dGgvdjEiLCJzdWIiOiI0ZDJjZTZlZi03ZDZiLTQyYjAtYmUyMi05N2M4NDI4MTg5MGMiLCJhdWQiOiJhdXRoZW50aWNhdGVkIiwiZXhwIjoxNzUyMjMxOTczLCJpYXQiOjE3NTE2MzE5NzMsImVtYWlsIjoiZmFyZWxwNTE3MUBnbWFpbC5jb20iLCJwaG9uZSI6IiIsImFwcF9tZXRhZGF0YSI6eyJwcm92aWRlciI6ImVtYWlsIiwicHJvdmlkZXJzIjpbImVtYWlsIl19LCJ1c2VyX21ldGFkYXRhIjp7ImVtYWlsIjoiZmFyZWxwNTE3MUBnbWFpbC5jb20iLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwiZmlyc3RuYW1lIjoiRmFyZWwiLCJncmFkZSI6MTIsImxhc3RuYW1lIjoiRmlybWFuc3lhaCIsImxldmVsIjoibWVtYmVyIiwicGhvbmUiOjg1OTIxNTE5NDQyLCJwaG9uZV92ZXJpZmllZCI6ZmFsc2UsInN1YiI6IjRkMmNlNmVmLTdkNmItNDJiMC1iZTIyLTk3Yzg0MjgxODkwYyJ9LCJyb2xlIjoiYXV0aGVudGljYXRlZCIsImFhbCI6ImFhbDEiLCJhbXIiOlt7Im1ldGhvZCI6InBhc3N3b3JkIiwidGltZXN0YW1wIjoxNzUxNjMxOTczfV0sInNlc3Npb25faWQiOiIwYTFmYjE1YS02MzVlLTRjNjktODYyNi0xNGI2YWU3OTRkNjgiLCJpc19hbm9ueW1vdXMiOmZhbHNlfQ.uT5io7Tysi1PakgXvibNKJH9YAbRrG_caHU17SUaGK0'
    
    # Inisialisasi explorer
    explorer = SupabaseExplorer(API_URL, API_KEY, AUTH_TOKEN)
    
    try:
        # Tampilkan overview database
        schema_table_map = explorer.display_database_overview()
        
        # Interactive backup selector
        selected_tables = explorer.interactive_backup_selector(schema_table_map)
        
        if selected_tables:
            # Lakukan backup
            backup_dir, backup_summary = explorer.backup_tables(selected_tables)
            
            # Tampilkan summary
            print(f"\n📊 BACKUP SUMMARY:")
            successful = len([s for s in backup_summary if s['status'].startswith('Success')])
            failed = len([s for s in backup_summary if s['status'] == 'Failed'])
            total_records = sum([s['records'] for s in backup_summary])
            
            print(f"   ✅ Successful: {successful} tables")
            print(f"   ❌ Failed: {failed} tables")
            print(f"   📊 Total Records: {total_records}")
            
        else:
            print("No backup performed.")
            
    except KeyboardInterrupt:
        print("\n\n⚠️  Process interrupted by user.")
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")

if __name__ == "__main__":
    # Install required packages
    try:
        import pandas as pd
        import requests
    except ImportError:
        print("Please install required packages:")
        print("pip install pandas requests")
        sys.exit(1)
    
    main()