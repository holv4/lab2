import os
import re
import urllib.request
import pandas as pd
from datetime import datetime as dt

# 1. Завантаження CSV-файлів з часовим маркером
def download_vhi_data(save_dir='data'):
    os.makedirs(save_dir, exist_ok=True)
    for area_id in range(1, 26):
        url = f"https://www.star.nesdis.noaa.gov/smcd/emb/vci/VH/get_TS_admin.php?country=UKR&provinceID={area_id}&year1=1981&year2=2024&type=Mean"
        timestamp = dt.now().strftime("%d%m%Y%H%M%S")
        filename = f'vhi_id_{area_id}_{timestamp}.csv'
        filepath = os.path.join(save_dir, filename)

        if os.path.exists(filepath):
            print(f"Файл уже існує: {filename}. Пропускаємо.")
            continue

        try:
            response = urllib.request.urlopen(url)
            with open(filepath, 'wb') as f:
                f.write(response.read())
            print(f"Завантажено VHI для області {area_id}: {filename}")
        except Exception as e:
            print(f"Помилка для області {area_id}: {e}")

# 2. Зчитування та очищення CSV-файлів
def read_vhi_from_csv(directory):
    data_frames = []
    for filename in os.listdir(directory):
        if filename.startswith('vhi_id_') and filename.endswith('.csv'):
            filepath = os.path.join(directory, filename)
            try:
                area_id = int(filename.split('_')[2])
            except (IndexError, ValueError):
                print(f"Невірне ім’я файлу: {filename}. Видаляємо.")
                os.remove(filepath)
                continue
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    lines = [re.sub(r'<.*?>', '', line) for line in f if 'N/A' not in line]
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.writelines(lines)
                df = pd.read_csv(filepath, index_col=False, header=1)
                df.columns = df.columns.str.strip()
                df['area_ID'] = area_id

                df['VHI'] = pd.to_numeric(df['VHI'], errors='coerce')
                df = df[df['VHI'].notna() & (df['VHI'] >= 0)]

                df['year'] = df['year'].astype(str).str.extract(r'(\d+)')
                df = df.dropna(subset=['year']).copy()
                df['year'] = df['year'].astype(int)
                
                data_frames.append(df)
            except Exception as e:
                print(f"[!] Помилка при зчитуванні {filename}: {e}")
    
    return pd.concat(data_frames, ignore_index=True) if data_frames else pd.DataFrame()

# 3. Додавання назв областей українською
def recode_region_ids(df):
    region_names = {
        1: "Вінницька", 2: "Волинська", 3: "Дніпропетровська", 4: "Донецька",
        5: "Житомирська", 6: "Закарпатська", 7: "Запорізька", 8: "Івано-Франківська",
        9: "Київська", 10: "Кіровоградська", 11: "Луганська", 12: "Львівська",
        13: "Миколаївська", 14: "Одеська", 15: "Полтавська", 16: "Рівенська",
        17: "Сумська", 18: "Тернопільська", 19: "Харківська", 20: "Херсонська",
        21: "Хмельницька", 22: "Черкаська", 23: "Чернівецька", 24: "Чернігівська",
        25: "Республіка Крим"
    }
    df['area_name'] = df['area_ID'].map(region_names)
    return df

# 4. VHI для області за рік
def vhi_series_for_year(df):
    area_id = int(input("Введіть ID області (1–25): "))
    year = int(input("Введіть рік: "))
    subset = df[(df['area_ID'] == area_id) & (df['year'] == year)]
    print(f"\nVHI для області {df[df['area_ID']==area_id]['area_name'].iloc[0]} ({area_id}) за {year} рік:")
    print(subset[['week', 'VHI']])
    return subset

# 5. Cтатистика VHI
def vhi_statistics(df):
    area_ids = input("Введіть ID областей через кому (наприклад: 1,2,3): ")
    area_ids = list(map(int, area_ids.split(',')))
    years = input("Введіть роки через кому (наприклад: 2010,2020): ")
    years = list(map(int, years.split(',')))
    filtered = df[df['area_ID'].isin(area_ids) & df['year'].isin(years)]
    result = filtered.groupby(['area_ID', 'year'])['VHI'].agg(['min', 'max', 'mean', 'median'])
    print("\nСтатистика VHI для обраних областей і років:")
    print(result)
    return result

# 6. Ряд VHI по діапазону
def vhi_series_range(df):
    area_ids = input("Введіть ID областей через кому (наприклад: 1,2): ")
    area_ids = list(map(int, area_ids.split(',')))
    year_start = int(input("Введіть початковий рік: "))
    year_end = int(input("Введіть кінцевий рік: "))
    filtered = df[df['area_ID'].isin(area_ids) & df['year'].between(year_start, year_end)]
    print(f"\nVHI для областей {area_ids} з {year_start} по {year_end}:")
    print(filtered[['year', 'week', 'area_ID', 'area_name', 'VHI']])
    return filtered

# 7. Екстремальні посухи (VHI < 15)
def extreme_drought_years(df):
    try:
        percent_input = float(input("Введіть відсоток областей для виявлення екстремальних посух (наприклад, 20): "))
        if percent_input <= 0 or percent_input > 100:
            raise ValueError("Відсоток має бути в межах від 0 до 100.")
    except ValueError as e:
        print(f"Невірне значення: {e}")
        return pd.DataFrame()

    total_regions = 25
    threshold_count = int((percent_input / 100) * total_regions)
    print(f"\nПорогова кількість областей: {threshold_count} з {total_regions} ({percent_input}%)")

    drought = df[df['VHI'] < 15]
    grouped = drought.groupby(['year', 'area_ID']).size().reset_index(name='count')
    drought_years = grouped.groupby('year').area_ID.nunique().reset_index()
    drought_years = drought_years[drought_years['area_ID'] >= threshold_count]

    if drought_years.empty:
        print("Немає років, коли вказаний відсоток областей мав екстремальні посухи.")
        return pd.DataFrame()

    print("\nРоки з екстремальними посухами:")
    for _, row in drought_years.iterrows():
        year = row['year']
        affected = grouped[grouped['year'] == year]
        region_names = [
            df[df['area_ID'] == rid]['area_name'].iloc[0] for rid in affected['area_ID']
        ]
        vhi_values = [
            df[(df['year'] == year) & (df['area_ID'] == rid) & (df['VHI'] < 15)]['VHI'].min()
            for rid in affected['area_ID']
        ]
        print(f"Рік: {year}")
        for name, vhi in zip(region_names, vhi_values):
            print(f" - {name}: мінімальний VHI = {vhi:.2f}")
    return drought_years


data_dir = 'data'
    
print("\n1. Завантаження VHI CSV-файлів")
download_vhi_data(data_dir)

print("\n2. Зчитування та очищення CSV-файлів")
df = read_vhi_from_csv(data_dir)

print("\n3. Додавання назв областей українською")
df = recode_region_ids(df)

print("\n4. Введіть параметри для ряду VHI по року")
vhi_series_for_year(df)

print("\n5. Введіть параметри для статистики VHI")
vhi_statistics(df)

print("\n6. Введіть параметри для ряду VHI по діапазону")
vhi_series_range(df)

print("\n7. Виявлення років з областями з посухами (VHI < 15)")
extreme_drought_years(df)
