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
    '山口': 'Yamaguchi',
    '徳島': 'Tokushima',
    '香川': 'Kagawa',
    '愛媛': 'Ehime',
    '福岡': 'Fukuoka',
    '越後': 'Echigo',
    '大和': 'Yamato',
    '近江': 'Omi',
    '駿河': 'Suruga',
    '東海': 'Tokai',
    '武蔵野': 'Musashino',
    # Cities & Towns
    '宝塚': 'Takarazuka',
    '神戸': 'Kobe',
    '高松': 'Takamatsu',
    '高松宮': 'Takamatsu no Miya',
    '盛岡': 'Morioka',
    '豊橋': 'Toyohashi',
    '静岡': 'Shizuoka',
    '富山': 'Toyama',
    '福井': 'Fukui',
    '調布': 'Chofu',
    '立川': 'Tachikawa',
    '青梅': 'Ome',
    '府中': 'Fuchu',
    '分倍河原': 'Bubaikawara',
    '是政': 'Koremasa',
    '栗東': 'Ritto',
    '豊明': 'Toyoake',
    '久御山': 'Kuminoyama',
    '五泉': 'Gosen',
    'いわき': 'Iwaki',
    '弥彦': 'Yahiko',
    # Mountains, Gorges, Rivers
    '荒川峡': 'Arakawa-kyo',
    '谷川岳': 'Tanigawadake',
    '磐梯山': 'Bandaisan',
    '駒ケ岳': 'Komagatake',
    '中ノ岳': 'Nakanodake',
    '信濃川': 'Shinanogawa',
    '胎内川': 'Tainaikawa',
    '鴨川': 'Kamogawa',
    '長良川': 'Nagarakawa',
    '最上川': 'Mogamigawa',
    '阿賀野川': 'Aganogawa',
    # Historic Places / Shrines / Districts
    '石清水': 'Iwashimizu',
    '平城京': 'Heijokyo',
    '上賀茂': 'Kamigamo',
    '御池': 'Oike',
    '烏丸': 'Karasuma',
    '鞍馬': 'Kurama',
    '橘': 'Tachibana',
    '日吉': 'Hiyoshi',
    '六社': 'Rokusha',
    '石打': 'Ishiuchi',
    '白川': 'Shirakawa',
    '咲花': 'Sakihana',
    '三条': 'Sanjo',
    # Poetic / Nature Names (romanized as-is)
    '邁進': 'Maishin',
    '飛竜': 'Hiryu',
    '青竜': 'Seiryu',
    '矢車': 'Yaguruma',
    '駿風': 'Shunpu',
    '立夏': 'Rikka',
    '錦': 'Nishiki',
    '初音': 'Hatsune',
    '栄': 'Sakae',
    '恵山': 'Esan',
    '城崎': 'Kinosaki',
    '高千穂': 'Takachiho',
    '常滑': 'Tokoname',
    '尖閣湾': 'Senkakuwan',
    '粟島': 'Awashima',
    '岩船': 'Iwafune',
    '柏崎': 'Kashiwazaki',
    '二本松': 'Nihonmatsu',
    '飯盛山': 'Iimoriyama',
    '御在所': 'Gozaisho',
    '大須': 'Osu',
    '長万部': 'Oshamanbe',
    '北辰': 'Hokushin',
    '高湯温泉': 'Takayu Onsen',
    '八幡': 'Yahata',
    '浄土平': 'Jodogahara',
    '伊達': 'Date',
    '戸畑': 'Tobata',
    '湯浜': 'Yuhama',
    '香嵐渓': 'Korankei',
    '河津桜': 'Kawazu Zakura',
    '織姫': 'Orihime',
    '古作': 'Kosaku',
    '五頭連峰': 'Gozu Renpo',
    '稲光': 'Inabikari',
    '君子蘭': 'Kunshiran',
    '石狩': 'Ishikari',
    '黒松': 'Kuromatsu',
    '赤松': 'Akamatsu',
    '白菊': 'Shiragiku',
    '閃光': 'Senkou',
    '山吹': 'Yamabuki',
    '秋明菊': 'Shumeigiku',
    '紫菊': 'Shigiku',
    '菜の花': 'Nanohana',
    'エリカ': 'Erika',
    'ミモザ': 'Mimosa',
    'こぶし': 'Kobushi',
    'ゆきつばき': 'Yukitsubaki',
    'ゆきやなぎ': 'Yukiyanagi',
    'ひめさゆり': 'Himesayuri',
    'さざんか': 'Sazanka',
    'あずさ': 'Azusa',
    'わらび': 'Warabi',
    'ゆりかもめ': 'Yurikamome',
    'かもめ島': 'Kamome-jima',
    # Track abbreviations
    '札': 'Sapporo',
    '函': 'Hakodate',
}

# Suffix translations (Japanese → Romanized/English)
suffix_translations = {
    '特別': 'Tokubetsu',
    '記念': 'Kinen',
    '杯': 'Hai',
    'ステークス': 'Stakes',
    '賞': 'Sho',
    'ハンデキャップ': 'Handicap',
    'ダッシュ': 'Dash',
    'チャンピオンシップ': 'Championship',
    'トロフィー': 'Trophy',
    'クラシック': 'Classic',
    'オープン': 'Open',
    'カップ': 'Cup',
}

def translate_race_name(name):
    """
    Translate a race name: romanize location prefix AND suffix.
    Returns the original name if no clear pattern found.
    """
    # Try to find location + suffix pattern
    for location, romaji in location_lookup.items():
        if location in name:
            # Found a location - extract parts
            idx = name.index(location)
            before = name[:idx]
            after = name[idx + len(location):]

            # Check if what remains has a translatable suffix
            translated_suffix = None
            suffix_in_after = None
            for suffix, trans in suffix_translations.items():
                if suffix in after:
                    translated_suffix = trans
                    suffix_in_after = suffix
                    break

            # Only translate if:
            # 1. Location is at reasonable position
            # 2. Has a recognizable suffix after
            # 3. The "before" part isn't too long or complex
            if len(before) < 30 and translated_suffix:
                # Build translation: [before] Romaji [space] [suffix translation] [remainder]
                remainder = after.replace(suffix_in_after, '', 1).strip()
                parts = [before.strip(), romaji, translated_suffix]
                if remainder:
                    parts.append(remainder)
                trans = ' '.join([p for p in parts if p])

                # Don't translate if result is too similar to original or too weird
                if trans != name and len(trans) > 3:
                    return trans
            break  # Only use first location match

    # No pattern found - return original
    return name

# Named races that don't fit the location+suffix pattern — add directly to stakes dict.
# Sourced from DB profile of JRA central track races (TrackCode 01-10).
named_stakes_additions = {
    'きさらぎ賞': 'Kisaragi Sho',
    'ヴィクトリアマイル': 'Victoria Mile',
    'エプソムカップ': 'Epsom Cup',
    'ＮＨＫマイルカップ': 'NHK Mile Cup',
    '天皇賞（春）': "Emperor's Cup (Spring)",
    '天皇賞（秋）': "Emperor's Cup (Autumn)",
    '京王杯スプリングカップ': 'Keio Cup Spring Cup',
    '京都新聞杯': 'Kyoto Shimbun Cup',
    '東京新聞杯': 'Tokyo Shimbun Cup',
    'ユニコーンステークス': 'Unicorn Stakes',
    'メトロポリタンステークス': 'Metropolitan Stakes',
    'プリンシパルステークス': 'Principal Stakes',
    'ブリリアントステークス': 'Brilliant Stakes',
    'スイートピーステークス': 'Sweet Pea Stakes',
    'マレーシアカップ': 'Malaysia Cup',
    'フィリピントロフィー': 'Philippines Trophy',
    'ＢＳイレブン賞': 'BS11 Sho',
    'テレ玉杯': 'Teletama Cup',
    'ターフオーソリティーオブインディア賞': 'Turf Authority of India Sho',
    'ロイヤルバンコクスポーツクラブ賞': 'Royal Bangkok Sports Club Sho',
    'アイビスサマーダッシュ': 'Ibis Summer Dash',
    'アメリカジョッキークラブカップ': 'American Jockey Club Cup',
    'アルゼンチン共和国杯': 'Argentine Republic Cup',
    'アイルランドトロフィー': 'Ireland Trophy',
    'オーストラリアトロフィー': 'Australia Trophy',
    'ドバイターフ': 'Dubai Turf',
    'アルクオーツスプリント': 'Al Quoz Sprint',
    '京都ハイジャンプ': 'Kyoto High Jump',
    'イルミネーションジャンプステークス': 'Illumination Jump Stakes',
}
for name, trans in named_stakes_additions.items():
    if name in data.get('stakes', {}):
        pass  # already in stakes dict — don't overwrite
    else:
        data.setdefault('stakes', {})[name] = trans

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
