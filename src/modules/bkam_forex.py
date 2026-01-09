# src/modules/bkam_forex.py

import pandas as pd
import requests
from datetime import datetime
import os
import io

def get_bkam_forex_rates():
    """
    Télécharge et extrait EUR/MAD et USD/MAD du CSV des cours de référence.
    """
    print("\n" + "="*60)
    print("EXTRACTION TAUX DE CHANGE BKAM")
    print("="*60)
    
    # URL du CSV des taux de change (celle qui fonctionne)
    CSV_URL = "https://www.bkam.ma/export/blockcsv/4550/5312b6def4ad0a94c5a992522868ac0a/cc51b5ce6878a3dc655dae26c47fddf8?block=cc51b5ce6878a3dc655dae26c47fddf8"
    
    rates = {'EUR/MAD': None, 'USD/MAD': None}
    
    try:
        print(f"[{datetime.now()}] Téléchargement du CSV...")
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        response = requests.get(CSV_URL, headers=headers, timeout=30)
        
        if response.status_code != 200:
            print(f"  ❌ Erreur HTTP: {response.status_code}")
            return rates
        
        # Convertir le contenu en texte
        content = response.content.decode('utf-8-sig')
        
        print("  ✅ CSV téléchargé")
        print(f"  Taille: {len(content)} caractères")
        print("\n  Aperçu du contenu:")
        print("-" * 50)
        print(content[:400])
        print("-" * 50)
        
        # Méthode SIMPLE : parser ligne par ligne
        lines = content.split('\n')
        
        for line in lines:
            line = line.strip().strip('"')
            if not line:
                continue
            
            # Chercher les lignes avec EURO et DOLLAR
            if 'EURO' in line.upper():
                parts = line.split(';')
                if len(parts) >= 2:
                    # Prendre la première valeur (colonne "Moyen" du jour J)
                    value = parts[1].replace(',', '.').strip()
                    try:
                        rates['EUR/MAD'] = float(value)
                        print(f"  ✅ EUR/MAD extrait: {rates['EUR/MAD']}")
                    except:
                        pass
            
            elif 'DOLLAR U.S.A.' in line.upper():
                parts = line.split(';')
                if len(parts) >= 2:
                    value = parts[1].replace(',', '.').strip()
                    try:
                        rates['USD/MAD'] = float(value)
                        print(f"  ✅ USD/MAD extrait: {rates['USD/MAD']}")
                    except:
                        pass
        
        # Si la méthode simple échoue, utiliser pandas en sautant les premières lignes
        if rates['EUR/MAD'] is None:
            print("  ⚠️  Méthode simple échouée, tentative avec pandas...")
            
            # Sauvegarder pour analyse
            downloads_dir = "/workspaces/finance-bladi-automation/downloads"
            os.makedirs(downloads_dir, exist_ok=True)
            filepath = os.path.join(downloads_dir, f"forex_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
            
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
            
            # Lire en sautant les 3 premières lignes (en-têtes)
            try:
                df = pd.read_csv(filepath, delimiter=';', skiprows=3, encoding='utf-8')
                print(f"  DataFrame shape: {df.shape}")
                print(df.head())
                
                # Chercher dans la première colonne
                for idx, row in df.iterrows():
                    if isinstance(row.iloc[0], str):
                        if 'EURO' in row.iloc[0].upper():
                            rates['EUR/MAD'] = float(str(row.iloc[1]).replace(',', '.'))
                        elif 'DOLLAR U.S.A.' in row.iloc[0].upper():
                            rates['USD/MAD'] = float(str(row.iloc[1]).replace(',', '.'))
            except Exception as e:
                print(f"  ❌ Erreur pandas: {e}")
    
    except Exception as e:
        print(f"  ❌ Erreur générale: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "="*60)
    print("RÉSULTATS:")
    print(f"  EUR/MAD: {rates['EUR/MAD']}")
    print(f"  USD/MAD: {rates['USD/MAD']}")
    print("="*60)
    
    # Fallback avec valeurs réalistes
    if rates['EUR/MAD'] is None:
        rates['EUR/MAD'] = 10.82
        rates['USD/MAD'] = 9.78
        rates['NOTE'] = 'FALLBACK'
    
    return rates

if __name__ == "__main__":
    results = get_bkam_forex_rates()
    print(f"\nSortie: {results}")