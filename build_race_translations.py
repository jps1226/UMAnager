#!/usr/bin/env python3
"""
Build English translations for specialRaces in race_name_dict.json
Strategy: Translate location prefixes, keep suffixes in Japanese romanized form
"""

import json
import re

# Load current dict
with open('src/UMAnager.Nexus/wwwroot/static/race_name_dict.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

special = data['specialRaces'].copy()

# Comprehensive location name lookup
location_lookup = {
    # JRA Main Tracks
    '札幌': 'Sapporo',
    '函館': 'Hakodate',
    '福島': 'Fukushima',
    '新潟': 'Niigata',
    '東京': 'Tokyo',
    '中山': 'Nakayama',
    '京都': 'Kyoto',
    '阪神': 'Hanshin',
    '小倉': 'Kokura',
    # Regional/NAR Tracks
    '笠松': 'Kasamatsu',
    '名古屋': 'Nagoya',
    '中京': 'Chukyo',
    '大阪': 'Osaka',
    '岡山': 'Okayama',
    '広島': 'Hiroshima',
    '高知': 'Kochi',
    '佐賀': 'Saga',
    '唐津': 'Karatsu',
    '長崎': 'Nagasaki',
    '熊本': 'Kumamoto',
    '大分': 'Oita',
    '宮崎': 'Miyazaki',
    '鹿児島': 'Kagoshima',
    '門別': 'Monbetsu',
    '札幌競馬': 'Sapporo',
    # Prefectures & Regions
    '北海道': 'Hokkaido',
    '青森': 'Aomori',
    '岩手': 'Iwate',
    '宮城': 'Miyagi',
    '秋田': 'Akita',
    '山形': 'Yamagata',
    '茨城': 'Ibaraki',
    '栃木': 'Tochigi',
    '群馬': 'Gunma',
    '埼玉': 'Saitama',
    '千葉': 'Chiba',
    '神奈川': 'Kanagawa',
    '山梨': 'Yamanashi',
    '長野': 'Nagano',
    '岐阜': 'Gifu',
    '愛知': 'Aichi',
    '三重': 'Mie',
    '滋賀': 'Shiga',
    '兵庫': 'Hyogo',
    '和歌山': 'Wakayama',
    '奈良': 'Nara',
    '岡山': 'Okayama',
    '広島': 'Hiroshima',
    '山口': 'Yamaguchi',
    '徳島': 'Tokushima',
    '香川': 'Kagawa',
    '愛媛': 'Ehime',
    '高知': 'Kochi',
    '福岡': 'Fukuoka',
    '佐賀': 'Saga',
    '長崎': 'Nagasaki',
    '熊本': 'Kumamoto',
    # Specific Landmarks
    '宝塚': 'Takarazuka',
    '神戸': 'Kobe',
    '高松': 'Takamatsu',
    '高松宮': 'Takamatsu no Miya',
    '石打': 'Ishiuchi',
    '白川': 'Shirakawa',
    '盛岡': 'Morioka',
    '豊橋': 'Toyohashi',
    '静岡': 'Shizuoka',
    '富山': 'Toyama',
    '石川': 'Ishikawa',
    '福井': 'Fukui',
    # Track abbreviations that might appear
    '札': 'Sapporo',
    '函': 'Hakodate',
}

# Suffixes to KEEP in Japanese (or minimal romanization)
suffix_patterns = [
    '特別', '記念', '杯', 'ステークス', '賞',
    'ハンデキャップ', 'ダッシュ', 'チャンピオンシップ',
    'トロフィー', 'クラシック', 'オープン',
]

def translate_race_name(name):
    """
    Translate a race name: romanize location prefix, keep suffix in Japanese.
    Returns the original name if no clear pattern found.
    """
    # If already translated (has Latin chars and Japanese), skip
    if any(ord(c) < 128 for c in name if c not in '０１２３４５６７８９') and any(ord(c) > 128 for c in name):
        # Mixed - likely already has some translation, check if it's a full match already
        pass

    # Try to find location + suffix pattern
    for location, romaji in location_lookup.items():
        if location in name:
            # Found a location - try to build translation
            # Extract the part before and after
            idx = name.index(location)
            before = name[:idx]
            matched = location
            after = name[idx + len(location):]

            # Check if what remains looks like a suffix or class designation
            has_suffix = any(suffix in after for suffix in suffix_patterns)

            # Only translate if:
            # 1. Location is at start or after some context
            # 2. Has a recognizable suffix after
            # 3. The "before" part isn't too long or complex
            if len(before) < 30 and (has_suffix or len(after) < 15):
                # Build translation: [before] Romaji [after]
                # Keep Japanese suffixes and class designations as-is
                trans = (before + romaji + after).strip()

                # Don't translate if result is too similar to original or too weird
                if trans != name and len(trans) > 3:
                    return trans
            break  # Only use first location match

    # No pattern found - return original
    return name

# Count translations that will be made
translation_count = 0
translated_entries = {}

for name in special.keys():
    trans = translate_race_name(name)
    if trans != name:
        translated_entries[name] = trans
        translation_count += 1

print(f"Found {translation_count} race names that can be translated")
print(f"\nSample translations:")
for orig, trans in list(translated_entries.items())[:20]:
    print(f"  {orig:60} → {trans}")

# Update the specialRaces dict with translations
for orig, trans in translated_entries.items():
    special[orig] = trans

# Save updated JSON
data['specialRaces'] = special
data['lastUpdated'] = '2026-05-19'

with open('src/UMAnager.Nexus/wwwroot/static/race_name_dict.json', 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print(f"\n[OK] Updated race_name_dict.json with {translation_count} translations")
