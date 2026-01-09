# src/modules/bkam_treasury_official.py
# VERSION OFFICIELLE - Alignée sur la méthodologie BKAM/DTFE

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
        """
        Initialise la courbe des taux.
        
        Args:
            csv_content: Contenu CSV brut (optionnel)
            csv_url: URL pour télécharger le CSV (optionnel)
        """
        self.treasury_data = []
        self.reference_date = dt.now().date()
        
        if csv_content:
            self.load_from_csv(csv_content)
        elif csv_url:
            self.download_and_load(csv_url)
    
    def download_and_load(self, page_url: str) -> bool:
        """
        Télécharge le CSV depuis la page BKAM et charge les données.
        """
        try:
            print(f"[{dt.now()}] Téléchargement depuis BKAM...")
            
            headers = {'User-Agent': 'Mozilla/5.0'}
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
        """
        Charge et parse le contenu CSV selon le format BKAM.
        """
        try:
            lines = csv_content.strip().split('\n')
            self.treasury_data = []
            
            # Ignorer les 2 premières lignes (en-têtes)
            for line in lines[2:]:
                line = line.strip()
                if not line or line.startswith('Total'):
                    continue
                
                parts = [p.strip().strip('"') for p in line.split(';')]
                
                if len(parts) >= 4:
                    try:
                        maturity_date_str = parts[0]  # "16/03/2026"
                        rate_str = parts[2]           # "2,210 %"
                        
                        # Nettoyer et convertir
                        rate_clean = rate_str.replace('%', '').replace(',', '.').strip()
                        rate_value = float(rate_clean)
                        
                        # Convertir la date
                        maturity_date = dt.strptime(maturity_date_str, '%d/%m/%Y').date()
                        
                        # Calculer les jours exacts
                        days_to_maturity = (maturity_date - self.reference_date).days
                        
                        if days_to_maturity < 0:
                            continue  # Ignorer les échéances passées
                        
                        self.treasury_data.append({
                            'maturity_date': maturity_date,
                            'maturity_date_str': maturity_date_str,
                            'days': days_to_maturity,
                            'years': days_to_maturity / 365.25,
                            'rate': rate_value
                        })
                        
                    except Exception as e:
                        continue  # Ignorer les lignes erronées
            
            # Trier par maturité croissante
            self.treasury_data.sort(key=lambda x: x['days'])
            
            if not self.treasury_data:
                raise Exception("Aucune donnée valide dans le CSV")
            
            print(f"  ✅ {len(self.treasury_data)} points de référence chargés")
            print(f"  → Période: {self.treasury_data[0]['maturity_date_str']} "
                  f"à {self.treasury_data[-1]['maturity_date_str']}")
            print(f"  → Maturités: {self.treasury_data[0]['years']:.2f} à "
                  f"{self.treasury_data[-1]['years']:.2f} ans")
            
            return True
            
        except Exception as e:
            print(f"  ❌ Erreur de parsing: {e}")
            return False
    
    def linear_interpolation(self, target_days: int) -> Tuple[Optional[float], Dict]:
        """
        Interpolation linéaire selon la méthodologie BKAM/DTFE.
        
        Formule: r = r1 + (d - d1) × (r2 - r1) / (d2 - d1)
        
        Returns:
            (taux_interpolé, métadonnées)
        """
        if len(self.treasury_data) < 2:
            return None, {'error': 'Données insuffisantes'}
        
        # Cas 1: La cible est avant la première échéance
        if target_days <= self.treasury_data[0]['days']:
            point = self.treasury_data[0]
            return point['rate'], {
                'method': 'Première échéance',
                'point_used': point['maturity_date_str'],
                'actual_days': point['days'],
                'actual_years': point['years']
            }
        
        # Cas 2: La cible est après la dernière échéance
        if target_days >= self.treasury_data[-1]['days']:
            point = self.treasury_data[-1]
            return point['rate'], {
                'method': 'Dernière échéance',
                'point_used': point['maturity_date_str'],
                'actual_days': point['days'],
                'actual_years': point['years']
            }
        
        # Cas 3: Interpolation entre deux points
        for i in range(len(self.treasury_data) - 1):
            d1, r1 = self.treasury_data[i]['days'], self.treasury_data[i]['rate']
            d2, r2 = self.treasury_data[i+1]['days'], self.treasury_data[i+1]['rate']
            
            if d1 <= target_days <= d2:
                # INTERPOLATION LINÉAIRE (méthode officielle)
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
                    'actual_years': target_days / 365.25,
                    'formula': f"r = {r1} + ({target_days} - {d1}) × ({r2} - {r1}) / ({d2} - {d1})"
                }
                
                return round(interpolated_rate, 4), metadata
        
        return None, {'error': 'Point non trouvé'}
    
    def get_rate_for_maturity(self, target_years: float) -> Dict:
        """
        Calcule le taux pour une maturité cible en années.
        """
        target_days = int(target_years * 365.25)
        
        rate, metadata = self.linear_interpolation(target_days)
        
        result = {
            'target_years': target_years,
            'target_days': target_days,
            'rate': rate,
            **metadata
        }
        
        return result
    
    def get_standard_maturities(self) -> Dict:
        """
        Calcule les taux pour les maturités standard 2Y, 5Y, 10Y.
        """
        standard_maturities = {
            'BT2Y': 2.0,   # 2 ans
            'BT5Y': 5.0,   # 5 ans
            'BT10Y': 10.0  # 10 ans
        }
        
        results = {}
        
        for key, years in standard_maturities.items():
            result = self.get_rate_for_maturity(years)
            results[key] = result
        
        return results
    
    def display_curve_info(self):
        """
        Affiche des informations détaillées sur la courbe.
        """
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
        
        # Afficher un exemple de calcul
        if len(self.treasury_data) >= 2:
            print("\nExemple de calcul d'interpolation (méthode BKAM):")
            print(f"Pour une obligation à 3 ans (1095 jours):")
            
            result = self.get_rate_for_maturity(3.0)
            if result['rate'] is not None:
                print(f"  → Taux interpolé: {result['rate']:.3f}%")
                if 'formula' in result:
                    print(f"  → Formule: {result['formula']}")
    
    def export_for_finance_bladi(self) -> Dict:
        """
        Formatte les résultats pour l'intégration dans Finance Bladi.
        """
        standard_rates = self.get_standard_maturities()
        
        output = {
            'extraction_time': dt.now().isoformat(),
            'reference_date': self.reference_date.isoformat(),
            'data_points': len(self.treasury_data),
            'methodology': 'Interpolation linéaire BKAM/DTFE (standard OPCVM)'
        }
        
        # Ajouter les taux standard
        for key in ['BT2Y', 'BT5Y', 'BT10Y']:
            if key in standard_rates and standard_rates[key]['rate'] is not None:
                result = standard_rates[key]
                output[key] = result['rate']
                output[f'{key}_METHOD'] = result['method']
                output[f'{key}_DAYS'] = result['actual_days']
                output[f'{key}_YEARS'] = result['actual_years']
                
                if 'point_before' in result:
                    output[f'{key}_INTERPOLATED_FROM'] = \
                        f"{result['point_before']} ({result['rate_before']}%) → " \
                        f"{result['point_after']} ({result['rate_after']}%)"
        
        # Ajouter les points extrêmes
        if self.treasury_data:
            output['MIN_MATURITY'] = self.treasury_data[0]['maturity_date_str']
            output['MAX_MATURITY'] = self.treasury_data[-1]['maturity_date_str']
            output['MIN_RATE'] = self.treasury_data[0]['rate']
            output['MAX_RATE'] = self.treasury_data[-1]['rate']
        
        return output

def get_bkam_treasury_official():
    """
    Fonction principale pour récupérer les taux selon la méthodologie officielle.
    """
    print("\n" + "="*80)
    print("FINANCE BLADI - COLLECTE DES TAUX DU TRÉSOR (MÉTHODOLOGIE BKAM/DTFE)")
    print("="*80)
    
    PAGE_URL = "https://www.bkam.ma/Marches/Principaux-indicateurs/Marche-obligataire/Marche-des-bons-de-tresor/Marche-secondaire/Taux-de-reference-des-bons-du-tresor"
    
    try:
        # 1. Initialiser et charger la courbe
        curve = BkamTreasuryCurve()
        
        if not curve.download_and_load(PAGE_URL):
            raise Exception("Échec du chargement des données")
        
        # 2. Afficher les informations détaillées
        curve.display_curve_info()
        
        # 3. Calculer les taux standard
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
        
        # 4. Exporter pour Finance Bladi
        print("\n" + "="*60)
        print("RÉSULTATS POUR FINANCE BLADI")
        print("="*60)
        
        export_data = curve.export_for_finance_bladi()
        
        # Afficher les résultats clés
        for key in ['BT2Y', 'BT5Y', 'BT10Y']:
            if key in export_data:
                method = export_data.get(f'{key}_METHOD', 'N/A')
                source = export_data.get(f'{key}_INTERPOLATED_FROM', 'Direct')
                
                print(f"{key}: {export_data[key]:.3f}%")
                print(f"  [via {method}]")
                if 'INTERPOLATED_FROM' in export_data:
                    print(f"  {source}")
        
        print(f"\nMéthodologie: {export_data['methodology']}")
        print(f"Points de référence: {export_data['data_points']}")
        print(f"Courbe: {export_data.get('MIN_MATURITY', 'N/A')} → "
              f"{export_data.get('MAX_MATURITY', 'N/A')}")
        
        return export_data
        
    except Exception as e:
        print(f"\n❌ ERREUR: {e}")
        import traceback
        traceback.print_exc()
        
        return {
            'error': str(e),
            'BT2Y': None,
            'BT5Y': None, 
            'BT10Y': None
        }

def main():
    """
    Point d'entrée principal avec validation.
    """
    print("Démarrage de la collecte des taux du Trésor...")
    print("Méthodologie: Interpolation linéaire (standard BKAM/DTFE pour OPCVM)")
    
    results = get_bkam_treasury_official()
    
    print("\n" + "="*80)
    print("VALIDATION DES RÉSULTATS")
    print("="*80)
    
    # Vérifier la cohérence des résultats
    if 'error' in results:
        print(f"STATUT: ÉCHEC - {results['error']}")
        return results
    
    # Vérifier que les taux augmentent avec la maturité (courbe normale)
    rates = []
    for key in ['BT2Y', 'BT5Y', 'BT10Y']:
        if key in results and results[key] is not None:
            rates.append(results[key])
    
    if len(rates) == 3:
        if rates[0] < rates[1] < rates[2]:
            print("✓ Courbe cohérente: taux croissants avec la maturité")
            print(f"  {rates[0]:.3f}% < {rates[1]:.3f}% < {rates[2]:.3f}%")
        else:
            print("⚠️  Attention: courbe inversée ou plate")
            print(f"  {rates[0]:.3f}% → {rates[1]:.3f}% → {rates[2]:.3f}%")
    
    print(f"\n✓ Collecte terminée à {results['extraction_time']}")
    
    # Format final pour intégration
    final_output = {
        'EUR/MAD': None,  # À remplir par un autre module
        'USD/MAD': None,  # À remplir par un autre module
        'BT2Y': results.get('BT2Y'),
        'BT5Y': results.get('BT5Y'),
        'BT10Y': results.get('BT10Y'),
        'TREASURY_DATA_SOURCE': 'BKAM Official',
        'TREASURY_METHOD': results.get('methodology', 'Linear Interpolation')
    }
    
    print(f"\nDonnées prêtes pour Google Sheets:")
    for key, value in final_output.items():
        if value is not None:
            print(f"  {key}: {value}")
    
    return final_output

if __name__ == "__main__":
    # Exécuter le module
    results = main()
    
    # Sauvegarder les résultats pour débogage
    downloads_dir = "/workspaces/finance-bladi-automation/downloads"
    os.makedirs(downloads_dir, exist_ok=True)
    
    output_file = os.path.join(downloads_dir, 
                               f"treasury_results_{dt.now().strftime('%Y%m%d_%H%M%S')}.json")
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False, default=str)
    
    print(f"\nRésultats sauvegardés dans: {output_file}")