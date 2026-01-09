# src/modules/bkam_treasury_official.py
# VERSION OFFICIELLE - Alignée sur la méthodologie BKAM/DTFE

# HEADERS TO BYPASS 403 BLOCK
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'DNT': '1',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Cache-Control': 'max-age=0'
}

import pandas as pd
import requests
from datetime import datetime as dt, timedelta
import os
import re
import json
from typing import Dict, List, Tuple, Optional

class BkamTreasuryCurve:
    """
    Classe pour construire et interpréter la courbe des taux du Trésor marocain
    selon la méthodologie officielle BKAM/DTFE.
    """
    
    def __init__(self, csv_content: str = None, csv_url: str = None):
        self.treasury_data = []
        self.reference_date = dt.now().date()
        
        if csv_content:
            self.load_from_csv(csv_content)
        elif csv_url:
            self.download_and_load(csv_url)
    
    def download_and_load(self, page_url: str) -> bool:
        try:
            print(f"[{dt.now()}] Téléchargement depuis BKAM...")
            
            # Use global headers
            global headers
            response = requests.get(page_url, headers=headers, timeout=30)
            
            if response.status_code != 200:
                raise Exception(f"HTTP {response.status_code}")
            
            # Trouver le lien CSV
            html_content = response.text
            pattern = r'href="(/export/blockcsv/[^"]+)"'
            matches = re.findall(pattern, html_content, re.IGNORECASE)
            
            if not matches:
                raise Exception("Lien CSV non trouvé")
            
            csv_url = "https://www.bkam.ma" + matches[0]
            print(f"  → Lien CSV: {csv_url[:80]}...")
            
            # Télécharger le CSV
            csv_response = requests.get(csv_url, headers=headers, timeout=30)
            
            if csv_response.status_code != 200:
                raise Exception(f"CSV HTTP {csv_response.status_code}")
            
            csv_content = csv_response.content.decode('utf-8-sig')
            return self.load_from_csv(csv_content)
            
        except Exception as e:
            print(f"  ❌ Erreur: {e}")
            return False
    
    def load_from_csv(self, csv_content: str) -> bool:
        try:
            lines = csv_content.strip().split('\n')
            self.treasury_data = []
            
            for line in lines[2:]:
                line = line.strip()
                if not line or line.startswith('Total'):
                    continue
                
                parts = [p.strip().strip('"') for p in line.split(';')]
                
                if len(parts) >= 4:
                    try:
                        maturity_date_str = parts[0]
                        rate_str = parts[2]
                        
                        rate_clean = rate_str.replace('%', '').replace(',', '.').strip()
                        rate_value = float(rate_clean)
                        maturity_date = dt.strptime(maturity_date_str, '%d/%m/%Y').date()
                        days_to_maturity = (maturity_date - self.reference_date).days
                        
                        if days_to_maturity < 0: continue
                        
                        self.treasury_data.append({
                            'maturity_date': maturity_date,
                            'maturity_date_str': maturity_date_str,
                            'days': days_to_maturity,
                            'years': days_to_maturity / 365.25,
                            'rate': rate_value
                        })
                    except: continue
            
            self.treasury_data.sort(key=lambda x: x['days'])
            
            if not self.treasury_data:
                raise Exception("Aucune donnée valide dans le CSV")
            
            print(f"  ✅ {len(self.treasury_data)} points de référence chargés")
            return True
        except Exception as e:
            print(f"  ❌ Erreur de parsing: {e}")
            return False
    
    def linear_interpolation(self, target_days: int) -> Tuple[Optional[float], Dict]:
        if len(self.treasury_data) < 2:
            return None, {'error': 'Données insuffisantes'}
        
        # Cas 1: Avant la première échéance
        if target_days <= self.treasury_data[0]['days']:
            point = self.treasury_data[0]
            return point['rate'], {
                'method': 'Première échéance',
                'point_used': point['maturity_date_str'],
                'actual_days': point['days'],
                'actual_years': point['years']
            }
        
        # Cas 2: Après la dernière échéance
        if target_days >= self.treasury_data[-1]['days']:
            point = self.treasury_data[-1]
            return point['rate'], {
                'method': 'Dernière échéance',
                'point_used': point['maturity_date_str'],
                'actual_days': point['days'],
                'actual_years': point['years']
            }
        
        # Cas 3: Interpolation
        for i in range(len(self.treasury_data) - 1):
            d1, r1 = self.treasury_data[i]['days'], self.treasury_data[i]['rate']
            d2, r2 = self.treasury_data[i+1]['days'], self.treasury_data[i+1]['rate']
            
            if d1 <= target_days <= d2:
                interpolated_rate = r1 + (target_days - d1) * (r2 - r1) / (d2 - d1)
                
                metadata = {
                    'method': 'Interpolation linéaire',
                    'point_before': self.treasury_data[i]['maturity_date_str'],
                    'point_after': self.treasury_data[i+1]['maturity_date_str'],
                    'rate_before': r1,
                    'rate_after': r2,
                    'days_before': d1,
                    'days_after': d2,
                    'actual_days': target_days,
                    'actual_years': target_days / 365.25
                }
                return round(interpolated_rate, 4), metadata
        
        return None, {'error': 'Point non trouvé'}
    
    def get_rate_for_maturity(self, target_years: float) -> Dict:
        target_days = int(target_years * 365.25)
        rate, metadata = self.linear_interpolation(target_days)
        return {
            'target_years': target_years,
            'target_days': target_days,
            'rate': rate,
            **metadata
        }
    
    def get_standard_maturities(self) -> Dict:
        results = {}
        for key, years in {'BT2Y': 2.0, 'BT5Y': 5.0, 'BT10Y': 10.0}.items():
            results[key] = self.get_rate_for_maturity(years)
        return results
        
    def display_curve_info(self):
        """Affiche les détails de la courbe dans les logs"""
        print("\n" + "="*60)
        print("COURBE DES TAUX BKAM - INFORMATIONS DÉTAILLÉES")
        print("="*60)
        print(f"Date de référence: {self.reference_date}")
        print(f"Points de référence: {len(self.treasury_data)}")
        print("\nPoints de la courbe:")
        print("-" * 70)
        print(f"{'Date':12} {'Jours':>6} {'Années':>8} {'Taux':>8}")
        print("-" * 70)
        for point in self.treasury_data:
            print(f"{point['maturity_date_str']:12} {point['days']:6} "
                  f"{point['years']:8.2f} {point['rate']:8.3f}%")
        print("-" * 70)

    def export_for_finance_bladi(self) -> Dict:
        standard_rates = self.get_standard_maturities()
        output = {'methodology': 'Interpolation linéaire BKAM/DTFE'}
        for key in ['BT2Y', 'BT5Y', 'BT10Y']:
            if key in standard_rates and standard_rates[key]['rate'] is not None:
                output[key] = standard_rates[key]['rate']
        return output

def get_bkam_treasury_official():
    print("\n" + "="*80)
    print("FINANCE BLADI - COLLECTE DES TAUX DU TRÉSOR (MÉTHODOLOGIE BKAM/DTFE)")
    print("="*80)
    
    PAGE_URL = "https://www.bkam.ma/Marches/Principaux-indicateurs/Marche-obligataire/Marche-des-bons-de-tresor/Marche-secondaire/Taux-de-reference-des-bons-du-tresor"
    
    try:
        curve = BkamTreasuryCurve()
        if not curve.download_and_load(PAGE_URL):
            raise Exception("Échec du chargement des données")
            
        # 1. DISPLAY DETAILED INFO (The part you requested)
        curve.display_curve_info()
        
        # 2. PRINT CALCULATION STEPS
        print("\n" + "="*60)
        print("CALCUL DES TAUX STANDARD 2Y, 5Y, 10Y")
        print("="*60)
        
        standard_rates = curve.get_standard_maturities()
        
        for key, result in standard_rates.items():
            print(f"\n{key} ({result['target_years']} ans):")
            print(f"  → Taux: {result['rate']:.3f}%")
            print(f"  → Méthode: {result['method']}")
            
            if result['method'] == 'Interpolation linéaire':
                print(f"  → Interpolation entre:")
                print(f"     • {result['point_before']}: {result['rate_before']}% "
                      f"({result['days_before']} jours)")
                print(f"     • {result['point_after']}: {result['rate_after']}% "
                      f"({result['days_after']} jours)")
            
            print(f"  → Maturité réelle: {result['actual_years']:.2f} ans "
                  f"({result['actual_days']} jours)")
            
        return curve.export_for_finance_bladi()
        
    except Exception as e:
        print(f"\n❌ ERREUR: {e}")
        return {'error': str(e), 'BT2Y': None, 'BT5Y': None, 'BT10Y': None}

if __name__ == "__main__":
    print(get_bkam_treasury_official())