from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import re
import csv  # ✅ Pour écrire dans un fichier CSV

# Liste des banques à rechercher
banks = ["CIH Bank Rabat", "Banque Populaire Rabat", "BMCE Bank Rabat", "Attijariwafa Bank Rabat"]

# Initialiser le WebDriver
driver = webdriver.Chrome()

all_reviews = []

for bank in banks:
    print(f"Recherche des agences de {bank}...")
    search_url = f"https://www.google.com/maps/search/{bank}"
    driver.get(search_url)
    
    try:
        # Attendre que les résultats de la recherche chargent
        WebDriverWait(driver, 15).until(EC.presence_of_all_elements_located((By.CLASS_NAME, "hfpxzc")))
        
        # Sélectionner plusieurs agences trouvées
        agency_links = driver.find_elements(By.CLASS_NAME, "hfpxzc")[:5]  # Limite à 5 agences par banque
        
        for agency in agency_links:
            agency.click()
            time.sleep(3)  # Pause pour chargement
            
            try:
                branch_name = driver.find_element(By.CLASS_NAME, "DUwDvf").text
                
                # Essayer différentes classes pour l'adresse
                location = "Adresse non disponible"
                address_candidates = ["Io6YTe", "CsEnBe"]
                
                for class_name in address_candidates:
                    try:
                        temp_location = driver.find_element(By.CLASS_NAME, class_name).text.strip()
                        if not re.match(r"^[A-Z0-9]+\+\w+", temp_location):  
                            location = temp_location
                            break
                    except:
                        continue
                
            except:
                branch_name = bank
                location = "Adresse non disponible"
            
            # Aller dans la section des avis
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CLASS_NAME, "wiI7pd")))
            
            # Scroller pour charger plus d'avis
            for _ in range(10):  # Charger jusqu'à 10 pages supplémentaires
                driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.END)
                time.sleep(3)  # Attendre pour permettre à la page de se charger
                reviews = driver.find_elements(By.CLASS_NAME, "wiI7pd")
                print(f"Nombre d'avis après défilement : {len(reviews)}")
            
            # Extraire les avis
            reviews = driver.find_elements(By.CLASS_NAME, "wiI7pd")
            ratings = driver.find_elements(By.CLASS_NAME, "kvMYJc")
            dates = driver.find_elements(By.CLASS_NAME, "rsqaWe")
            
            print(f"Nombre d'avis extraits pour {branch_name}: {len(reviews)}")

            for i in range(len(reviews)):
                all_reviews.append({
                    "bank_name": bank,
                    "branch_name": branch_name,
                    "location": location,
                    "review_text": reviews[i].text if i < len(reviews) else "Avis non disponible",
                    "rating": ratings[i].get_attribute("aria-label") if i < len(ratings) else "Évaluation non disponible",
                    "review_date": dates[i].text if i < len(dates) else "Date non disponible"
                })
            
    except Exception as e:
        print(f"❌ Erreur lors de la récupération des avis pour {bank}: {e}")
        continue

driver.quit()

# ✅ Sauvegarder dans un fichier CSV
csv_filename = "/home/amal/airflow/scriptsnew/bank_reviews.csv"
csv_columns = ["bank_name", "branch_name", "location", "review_text", "rating", "review_date"]

with open(csv_filename, "w", encoding="utf-8", newline="") as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
    writer.writeheader()
    writer.writerows(all_reviews)

print("✅ Extraction terminée. Avis enregistrés dans bank_reviews.csv")
