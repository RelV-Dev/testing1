#!/usr/bin/env python3
"""
Advanced Supabase Table Scanner
Menggunakan berbagai metode untuk menemukan semua tabel yang tersedia
"""

import requests
import json
import os
import csv
import string
import itertools
from datetime import datetime
from typing import Dict, List, Any, Optional
import sys
import time

class AdvancedSupabaseScanner:
    def __init__(self, api_url: str, api_key: str, auth_token: str):
        self.api_url = api_url.rstrip('/')
        self.api_key = api_key
        self.auth_token = auth_token
        self.headers = {
            'apikey': self.api_key,
            'Authorization': f'Bearer {self.auth_token}',
            'Content-Type': 'application/json'
        }
        
        # Expanded list of common table names
        self.common_tables = [
            # User & Authentication
            'users', 'profiles', 'accounts', 'auth', 'sessions', 'tokens',
            'user_profiles', 'user_settings', 'user_roles', 'permissions',
            
            # Content & Media
            'posts', 'articles', 'pages', 'content', 'media', 'images',
            'files', 'documents', 'uploads', 'attachments', 'gallery',
            
            # E-commerce
            'products', 'categories', 'orders', 'payments', 'transactions',
            'cart', 'wishlist', 'inventory', 'suppliers', 'customers',
            'invoices', 'receipts', 'discounts', 'coupons', 'reviews',
            
            # Social & Communication
            'comments', 'likes', 'shares', 'follows', 'messages',
            'notifications', 'chats', 'conversations', 'groups',
            
            # Events & Booking
            'events', 'bookings', 'reservations', 'appointments',
            'schedule', 'calendar', 'availability', 'tickets',
            
            # Business & Management
            'projects', 'tasks', 'teams', 'employees', 'departments',
            'contacts', 'leads', 'clients', 'vendors', 'partners',
            
            # Analytics & Logging
            'logs', 'analytics', 'metrics', 'reports', 'statistics',
            'activities', 'audit', 'tracking', 'sessions_log',
            
            # Settings & Configuration
            'settings', 'config', 'preferences', 'options', 'themes',
            'templates', 'layouts', 'menus', 'navigation',
            
            # Location & Geography
            'locations', 'addresses', 'cities', 'countries', 'regions',
            'coordinates', 'maps', 'places', 'venues',
            
            # Educational
            'courses', 'lessons', 'students', 'teachers', 'grades',
            'assignments', 'exams', 'certificates', 'schools',
            
            # Healthcare
            'patients', 'doctors', 'appointments', 'prescriptions',
            'treatments', 'medical_records', 'hospitals', 'clinics',
            
            # Real Estate
            'properties', 'listings', 'agents', 'buyers', 'sellers',
            'contracts', 'inspections', 'valuations',
            
            # Gaming
            'games', 'players', 'scores', 'achievements', 'levels',
            'tournaments', 'matches', 'leaderboards',
            
            # Finance
            'accounts', 'transactions', 'budgets', 'expenses',
            'income', 'investments', 'loans', 'assets',
            
            # Common suffixes/prefixes
            'data', 'info', 'details', 'history', 'archive',
            'backup', 'temp', 'cache', 'queue', 'jobs'
        ]
        
        # Add variations (plural/singular)
        self.generate_table_variations()
        
    def generate_table_variations(self):
        """Generate variations of table names"""
        variations = set(self.common_tables)
        
        # Add singular forms
        for table in self.common_tables:
            if table.endswith('s') and len(table) > 3:
                singular = table[:-1]
                variations.add(singular)
            elif table.endswith('ies'):
                singular = table[:-3] + 'y'
                variations.add(singular)
        
        # Add with common prefixes
        prefixes = ['app_', 'user_', 'admin_', 'sys_', 'tmp_', 'old_', 'new_']
        for prefix in prefixes:
            for table in list(variations):
                variations.add(prefix + table)
        
        # Add with common suffixes
        suffixes = ['_data', '_info', '_details', '_log', '_history']
        for suffix in suffixes:
            for table in list(variations):
                variations.add(table + suffix)
        
        self.common_tables = sorted(list(variations))
        print(f"🔍 Will scan {len(self.common_tables)} potential table names...")
    
    def check_table_exists(self, table_name: str) -> Dict:
        """Check if table exists and get basic info"""
        try:
            response = requests.get(
                f"{self.api_url}/{table_name}",
                headers=self.headers,
                params={'limit': 1},
                timeout=10
            )
            
            if response.status_code == 200:
                return {
                    'exists': True,
                    'accessible': True,
                    'status_code': 200,
                    'sample_data': response.json()
                }
            elif response.status_code == 401:
                return {
                    'exists': True,
                    'accessible': False,
                    'status_code': 401,
                    'error': 'Authentication required'
                }
            elif response.status_code == 403:
                return {
                    'exists': True,
                    'accessible': False,
                    'status_code': 403,
                    'error': 'Access forbidden'
                }
            elif response.status_code == 404:
                return {
                    'exists': False,
                    'accessible': False,
                    'status_code': 404,
                    'error': 'Table not found'
                }
            else:
                return {
                    'exists': True,
                    'accessible': False,
                    'status_code': response.status_code,
                    'error': f'HTTP {response.status_code}'
                }
                
        except requests.exceptions.Timeout:
            return {
                'exists': None,
                'accessible': False,
                'status_code': 0,
                'error': 'Timeout'
            }
        except Exception as e:
            return {
                'exists': None,
                'accessible': False,
                'status_code': 0,
                'error': str(e)
            }
    
    def comprehensive_table_scan(self) -> Dict:
        """Comprehensive scan of all possible tables"""
        print("🚀 Starting comprehensive table scan...")
        print("=" * 60)
        
        results = {
            'accessible': [],
            'protected': [],
            'unknown': [],
            'not_found': []
        }
        
        total_tables = len(self.common_tables)
        batch_size = 10
        
        for i in range(0, total_tables, batch_size):
            batch = self.common_tables[i:i + batch_size]
            
            print(f"📊 Scanning batch {i//batch_size + 1}/{(total_tables + batch_size - 1)//batch_size}...")
            
            for table_name in batch:
                result = self.check_table_exists(table_name)
                
                if result['exists'] and result['accessible']:
                    results['accessible'].append({
                        'name': table_name,
                        'sample_data': result['sample_data']
                    })
                    print(f"   ✅ {table_name}")
                    
                elif result['exists'] and not result['accessible']:
                    results['protected'].append({
                        'name': table_name,
                        'error': result['error'],
                        'status_code': result['status_code']
                    })
                    print(f"   🔒 {table_name} ({result['error']})")
                    
                elif result['exists'] is None:
                    results['unknown'].append({
                        'name': table_name,
                        'error': result['error']
                    })
                    
                # Small delay to avoid overwhelming the API
                time.sleep(0.1)
        
        return results
    
    def try_alternative_discovery_methods(self) -> List[str]:
        """Try alternative methods to discover tables"""
        print("\n🔍 Trying alternative discovery methods...")
        
        discovered_tables = []
        
        # Method 1: Try common REST endpoints that might reveal table names
        endpoints_to_try = [
            '/rest/v1/',  # Root endpoint
            '/rest/v1/rpc/',  # RPC endpoints
        ]
        
        for endpoint in endpoints_to_try:
            try:
                response = requests.get(
                    f"{self.api_url.replace('/rest/v1', '')}{endpoint}",
                    headers=self.headers,
                    timeout=10
                )
                
                if response.status_code == 200:
                    print(f"   ✅ Got response from {endpoint}")
                    # Look for table names in response
                    text = response.text.lower()
                    for table in self.common_tables:
                        if table in text:
                            discovered_tables.append(table)
                            
            except Exception as e:
                print(f"   ❌ Error with {endpoint}: {str(e)}")
        
        # Method 2: Try OpenAPI/Swagger endpoint
        try:
            response = requests.get(
                f"{self.api_url.replace('/rest/v1', '')}/rest/v1/",
                headers=self.headers,
                timeout=10
            )
            
            if response.status_code == 200:
                print("   ✅ Got OpenAPI schema")
                # Parse for table names
                
        except Exception as e:
            print(f"   ❌ OpenAPI discovery failed: {str(e)}")
        
        return list(set(discovered_tables))
    
    def intelligent_table_discovery(self) -> List[str]:
        """Intelligent table discovery based on found tables"""
        print("\n🧠 Intelligent table discovery...")
        
        # Based on already found tables, try to guess related tables
        found_tables = ['profiles', 'payments']  # From your previous scan
        
        intelligent_guesses = []
        
        # If we found 'profiles', look for related tables
        if 'profiles' in found_tables:
            related_to_profiles = [
                'users', 'accounts', 'auth', 'sessions', 'user_settings',
                'user_roles', 'permissions', 'user_profiles'
            ]
            intelligent_guesses.extend(related_to_profiles)
        
        # If we found 'payments', look for related tables
        if 'payments' in found_tables:
            related_to_payments = [
                'transactions', 'orders', 'invoices', 'products',
                'customers', 'receipts', 'payment_methods', 'subscriptions'
            ]
            intelligent_guesses.extend(related_to_payments)
        
        # Test these intelligent guesses
        confirmed_tables = []
        for table in intelligent_guesses:
            result = self.check_table_exists(table)
            if result['exists'] and result['accessible']:
                confirmed_tables.append(table)
                print(f"   ✅ Found: {table}")
        
        return confirmed_tables
    
    def get_table_schema_info(self, table_name: str) -> Dict:
        """Get detailed schema info for a table"""
        try:
            # Try to get more data to understand the structure
            response = requests.get(
                f"{self.api_url}/{table_name}",
                headers=self.headers,
                params={'limit': 10}
            )
            
            if response.status_code == 200:
                data = response.json()
                
                if data:
                    # Analyze the structure
                    columns = list(data[0].keys())
                    
                    # Try to guess column types
                    column_info = {}
                    for col in columns:
                        sample_values = [row.get(col) for row in data[:5] if row.get(col) is not None]
                        
                        if sample_values:
                            value_types = [type(v).__name__ for v in sample_values]
                            most_common_type = max(set(value_types), key=value_types.count)
                            column_info[col] = {
                                'type': most_common_type,
                                'sample_values': sample_values[:3]
                            }
                        else:
                            column_info[col] = {'type': 'unknown', 'sample_values': []}
                    
                    return {
                        'success': True,
                        'columns': columns,
                        'column_info': column_info,
                        'sample_count': len(data),
                        'sample_data': data
                    }
                else:
                    return {
                        'success': True,
                        'columns': [],
                        'column_info': {},
                        'sample_count': 0,
                        'sample_data': []
                    }
            else:
                return {
                    'success': False,
                    'error': f'HTTP {response.status_code}'
                }
                
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }
    
    def display_comprehensive_results(self, scan_results: Dict):
        """Display comprehensive scan results"""
        print("\n" + "=" * 60)
        print("🎯 COMPREHENSIVE SCAN RESULTS")
        print("=" * 60)
        
        accessible_tables = scan_results['accessible']
        protected_tables = scan_results['protected']
        
        print(f"\n✅ ACCESSIBLE TABLES ({len(accessible_tables)}):")
        print("-" * 40)
        
        detailed_accessible = []
        
        for i, table_info in enumerate(accessible_tables, 1):
            table_name = table_info['name']
            
            # Get detailed info
            schema_info = self.get_table_schema_info(table_name)
            
            if schema_info['success']:
                column_count = len(schema_info['columns'])
                sample_count = schema_info['sample_count']
                
                print(f"  {i:2d}. {table_name}")
                print(f"       📊 {column_count} columns, ~{sample_count} sample records")
                
                if schema_info['columns']:
                    cols_preview = schema_info['columns'][:5]
                    if len(schema_info['columns']) > 5:
                        cols_preview.append('...')
                    print(f"       📋 Columns: {', '.join(cols_preview)}")
                
                detailed_accessible.append({
                    'name': table_name,
                    'columns': schema_info['columns'],
                    'column_info': schema_info['column_info'],
                    'sample_data': schema_info['sample_data']
                })
            else:
                print(f"  {i:2d}. {table_name} (⚠️ Structure analysis failed)")
                detailed_accessible.append({
                    'name': table_name,
                    'columns': [],
                    'column_info': {},
                    'sample_data': []
                })
        
        if protected_tables:
            print(f"\n🔒 PROTECTED TABLES ({len(protected_tables)}):")
            print("-" * 40)
            
            for i, table_info in enumerate(protected_tables, 1):
                table_name = table_info['name']
                error = table_info['error']
                status = table_info['status_code']
                print(f"  {i:2d}. {table_name} (HTTP {status}: {error})")
        
        print(f"\n📊 SUMMARY:")
        print(f"   ✅ Accessible: {len(accessible_tables)} tables")
        print(f"   🔒 Protected: {len(protected_tables)} tables")
        print(f"   ❌ Not found: {len(scan_results['not_found'])} tables")
        
        return detailed_accessible

def main():
    """Main function"""
    # Konfigurasi Supabase
    API_URL = 'https://jcygvbkupixjhkijvfmi.supabase.co/rest/v1/'
    API_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoiYW5vbiIsImlhdCI6MTYxNzMzOTY2NiwiZXhwIjoxOTMyOTE1NjY2fQ.pakwp7Cud_wiQ6aipdnB6n1enK8k_SWaDt2RARnM930'
    AUTH_TOKEN = 'eyJhbGciOiJIUzI1NiIsImtpZCI6IkZPM29Dd3BvNDVvMGxCOHkiLCJ0eXAiOiJKV1QifQ.eyJpc3MiOiJodHRwczovL2pjeWd2Ymt1cGl4amhraWp2Zm1pLnN1cGFiYXNlLmNvL2F1dGgvdjEiLCJzdWIiOiI0ZDJjZTZlZi03ZDZiLTQyYjAtYmUyMi05N2M4NDI4MTg5MGMiLCJhdWQiOiJhdXRoZW50aWNhdGVkIiwiZXhwIjoxNzUyMjMxOTczLCJpYXQiOjE3NTE2MzE5NzMsImVtYWlsIjoiZmFyZWxwNTE3MUBnbWFpbC5jb20iLCJwaG9uZSI6IiIsImFwcF9tZXRhZGF0YSI6eyJwcm92aWRlciI6ImVtYWlsIiwicHJvdmlkZXJzIjpbImVtYWlsIl19LCJ1c2VyX21ldGFkYXRhIjp7ImVtYWlsIjoiZmFyZWxwNTE3MUBnbWFpbC5jb20iLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwiZmlyc3RuYW1lIjoiRmFyZWwiLCJncmFkZSI6MTIsImxhc3RuYW1lIjoiRmlybWFuc3lhaCIsImxldmVsIjoibWVtYmVyIiwicGhvbmUiOjg1OTIxNTE5NDQyLCJwaG9uZV92ZXJpZmllZCI6ZmFsc2UsInN1YiI6IjRkMmNlNmVmLTdkNmItNDJiMC1iZTIyLTk3Yzg0MjgxODkwYyJ9LCJyb2xlIjoiYXV0aGVudGljYXRlZCIsImFhbCI6ImFhbDEiLCJhbXIiOlt7Im1ldGhvZCI6InBhc3N3b3JkIiwidGltZXN0YW1wIjoxNzUxNjMxOTczfV0sInNlc3Npb25faWQiOiIwYTFmYjE1YS02MzVlLTRjNjktODYyNi0xNGI2YWU3OTRkNjgiLCJpc19hbm9ueW1vdXMiOmZhbHNlfQ.uT5io7Tysi1PakgXvibNKJH9YAbRrG_caHU17SUaGK0'
    
    print("🔍 Advanced Supabase Table Scanner")
    print("   🎯 Comprehensive discovery of all accessible tables")
    print()
    
    # Inisialisasi scanner
    scanner = AdvancedSupabaseScanner(API_URL, API_KEY, AUTH_TOKEN)
    
    try:
        # Comprehensive scan
        scan_results = scanner.comprehensive_table_scan()
        
        # Display results
        accessible_tables = scanner.display_comprehensive_results(scan_results)
        
        # Try intelligent discovery
        intelligent_results = scanner.intelligent_table_discovery()
        
        if intelligent_results:
            print(f"\n🧠 Additionally discovered {len(intelligent_results)} tables through intelligent guessing:")
            for table in intelligent_results:
                print(f"   ✅ {table}")
        
        # Save results to file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        results_file = f"table_discovery_{timestamp}.json"
        
        with open(results_file, 'w', encoding='utf-8') as f:
            json.dump({
                'scan_time': datetime.now().isoformat(),
                'accessible_tables': accessible_tables,
                'protected_tables': scan_results['protected'],
                'intelligent_discoveries': intelligent_results,
                'summary': {
                    'total_accessible': len(accessible_tables),
                    'total_protected': len(scan_results['protected']),
                    'total_intelligent': len(intelligent_results)
                }
            }, f, indent=2, ensure_ascii=False)
        
        print(f"\n💾 Results saved to: {results_file}")
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Scan interrupted by user.")
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")

if __name__ == "__main__":
    main()
