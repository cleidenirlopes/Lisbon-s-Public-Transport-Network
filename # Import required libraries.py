# Imports
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import re

# --- Helper: Convert RGB string to color name ---
def rgb_to_color_name(rgb_string):
    try:
        # Extract R, G, B from "rgb(r, g, b)"
        rgb = tuple(map(int, re.findall(r'\d+', rgb_string)))
        # Approximate color
        min_colors = {}
        for key, name in CSS3_HEX_TO_NAMES.items():
            r_c, g_c, b_c = tuple(int(key.lstrip('#')[i:i+2], 16) for i in (0, 2 ,4))
            rd = (r_c - rgb[0]) ** 2
            gd = (g_c - rgb[1]) ** 2
            bd = (b_c - rgb[2]) ** 2
            min_colors[(rd + gd + bd)] = name
        return min_colors[min(min_colors.keys())]
    except:
        return rgb_string  # fallback

# --- Setup Chrome Options ---
options = Options()
options.headless = True
options.add_argument("--window-size=1920,1080")

# --- Initialize WebDriver ---
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
url = "https://carrismetropolitana.pt/lines"
driver.get(url)

# --- Scroll to bottom in steps ---
scroll_pause = 1
last_height = driver.execute_script("return document.body.scrollHeight")

while True:
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(scroll_pause)
    new_height = driver.execute_script("return document.body.scrollHeight")
    if new_height == last_height:
        break
    last_height = new_height

# --- Extract Elements ---
elements = driver.find_elements(By.CLASS_NAME, "styles_badge__P2Lfq")
print(f"🔍 Total scraped items: {len(elements)}")

data = []
for element in elements:
    try:
        line_id = element.text.strip()
        style = element.get_attribute("style")
        color_rgb = re.search(r'rgb\([^)]+\)', style).group()
        color_name = rgb_to_color_name(color_rgb)
        data.append({"ID": line_id, "Color": color_name})
    except Exception as e:
        print("⚠️ Error:", e)

# --- Convert to DataFrame & Save ---
df = pd.DataFrame(data)

if not df.empty and "ID" in df.columns:
    df = df.drop_duplicates().sort_values(by="ID")
    df.to_csv("bus_line_colors.csv", index=False)
    print("✅ Saved to 'bus_line_colors.csv'")
    print(df.head())
else:
    print("❌ No data scraped or missing 'ID' column.")

# --- Cleanup ---
driver.quit()
