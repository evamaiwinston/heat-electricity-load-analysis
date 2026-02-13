#same setup as visualizations.py

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import json
from pathlib import Path
import numpy as np

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 8)
plt.rcParams['font.size'] = 10

# Paths
DATA_PATH = Path("data.json")
OUTPUT_PATH = Path("figures")
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

with open(DATA_PATH, 'r') as f:
    data = json.load(f)

# Convert to DataFrame
df = pd.DataFrame(data)

# Create commonly used columns
df['capacity_numeric'] = pd.to_numeric(df['capacity_mw'], errors='coerce')
df['pop_numeric'] = pd.to_numeric(df['pop_within_mile'], errors='coerce')

# EJ concern: flag if any EJ percentile is >= 80
ej_cols = [c for c in df.columns if c.startswith('ej_')]
df['ej_max_percentile'] = df[ej_cols].apply(pd.to_numeric, errors='coerce').max(axis=1)
df['ej_concern'] = df['ej_max_percentile'] >= 80


def calculate_exemption_score(row):
    score = 0
    reasons = []

    # 1. POWER (28 pts) - based on capacity_mw
    cap = row['capacity_numeric']
    if pd.notna(cap):
        if cap <= 20:
            score += 28
        elif cap <= 75:
            score += 18
        elif cap <= 200:
            score += 7
        else:
            score += 0
            reasons.append(f"High capacity ({cap:.0f} MW)")
    else:
        score += 0
        reasons.append("No capacity data")

    # 2. ENVIRONMENTAL JUSTICE (20 pts) - based on max EJ percentile
    if not row['ej_concern']:
        score += 20
    else:
        score += 0
        reasons.append(f"Environmental justice concerns (max percentile: {row['ej_max_percentile']:.0f})")

    # 3. POPULATION (15 pts) - based on pop_within_mile
    pop = row['pop_within_mile']
    if pd.notna(pop):
        pop_num = float(pop)
        if pop_num < 1000:
            score += 15
        elif pop_num < 5000:
            score += 10
            reasons.append(f"Population exposure: {pop_num:.0f} residents")
        elif pop_num < 10000:
            score += 5
            reasons.append(f"High population exposure: {pop_num:.0f} residents")
        else:
            score += 0
            reasons.append(f"Very high population exposure: {pop_num:.0f} residents")
    else:
        score += 0
        reasons.append("No population data")

    # 4. WATER STRESS (12 pts)
    water_stress = row['water_stress_index']
    if water_stress in ['Low (<10%)', 'Low - Medium (10-20%)']:
        score += 12
    elif water_stress in ['Medium - High (20-40%)']:
        score += 6
        reasons.append("Medium-high water stress")
    elif water_stress in ['High (40-80%)', 'Extremely High (>80%)']:
        score += 0
        reasons.append(f"High water stress ({water_stress})")
    else:
        score += 0

    # 5-8. REMAINING (25 pts) - renewables, emissions (CO2), community, disclosure
    # All 0 due to no data
    reasons.append("No renewable energy data (0/10)")
    reasons.append("No CO2 emissions data (0/7)")
    reasons.append("No community engagement data (0/4)")
    reasons.append("No disclosure data (0/4)")

    if score >= 80:
        tier = "Full Exemption (100%)"
    elif score >= 60:
        tier = "Partial Exemption (50%)"
    else:
        tier = "No Exemption (0%)"

    return score, tier, reasons


def print_detailed_breakdown(row, idx):
    """Print a detailed scoring breakdown for a single facility"""

    facility_name = row.get('brand', row.get('company', 'Unknown Facility'))
    county = row.get('county', 'Unknown County')

    print(f"\n{'='*80}")
    print(f"FACILITY #{idx}: {facility_name} - {county}")
    print(f"{'='*80}")

    score_breakdown = {}
    total = 0

    # 1. POWER (28 pts)
    cap = row['capacity_numeric']
    if pd.notna(cap):
        if cap <= 20:
            power_score, power_detail = 28, f'{cap:.0f} MW (≤20)'
        elif cap <= 75:
            power_score, power_detail = 18, f'{cap:.0f} MW (20-75)'
        elif cap <= 200:
            power_score, power_detail = 7, f'{cap:.0f} MW (75-200)'
        else:
            power_score, power_detail = 0, f'{cap:.0f} MW (>200)'
    else:
        power_score, power_detail = 0, 'No data'
    total += power_score
    score_breakdown['Power'] = (power_score, 28, power_detail)

    # 2. ENVIRONMENTAL JUSTICE (20 pts)
    if not row['ej_concern']:
        ej_score, ej_detail = 20, 'No EJ concerns (all percentiles < 80)'
    else:
        ej_score, ej_detail = 0, f'EJ flag (max percentile: {row["ej_max_percentile"]:.0f})'
    total += ej_score
    score_breakdown['Environmental Justice'] = (ej_score, 20, ej_detail)

    # 3. POPULATION (15 pts)
    pop = row['pop_within_mile']
    if pd.notna(pop):
        pop_num = float(pop)
        if pop_num < 1000:
            pop_score, pop_detail = 15, f'{int(pop_num):,} residents (<1,000)'
        elif pop_num < 5000:
            pop_score, pop_detail = 10, f'{int(pop_num):,} residents (1k-5k)'
        elif pop_num < 10000:
            pop_score, pop_detail = 5, f'{int(pop_num):,} residents (5k-10k)'
        else:
            pop_score, pop_detail = 0, f'{int(pop_num):,} residents (≥10k)'
    else:
        pop_score, pop_detail = 0, 'No data'
    total += pop_score
    score_breakdown['Population'] = (pop_score, 15, pop_detail)

    # 4. WATER STRESS (12 pts)
    water_stress = row['water_stress_index']
    if water_stress in ['Low (<10%)', 'Low - Medium (10-20%)']:
        water_score, water_detail = 12, f'Low stress: {water_stress}'
    elif water_stress in ['Medium - High (20-40%)']:
        water_score, water_detail = 6, f'Med-high stress: {water_stress}'
    elif water_stress in ['High (40-80%)', 'Extremely High (>80%)']:
        water_score, water_detail = 0, f'High/extreme stress: {water_stress}'
    else:
        water_score, water_detail = 0, 'No data'
    total += water_score
    score_breakdown['Water Stress'] = (water_score, 12, water_detail)

    # 5-8. REMAINING (25 pts) - all 0
    score_breakdown['Renewable Energy'] = (0, 10, 'No data available')
    score_breakdown['CO2 Emissions'] = (0, 7, 'No data available')
    score_breakdown['Community Engagement'] = (0, 4, 'No data available')
    score_breakdown['Disclosure'] = (0, 4, 'No data available')

    # Print breakdown
    print(f"\nFINAL SCORE: {total}/100")
    print(f"TIER: {row['exemption_tier']}\n")

    # Scored categories
    print(f"SCORED CATEGORIES: {total}/75 pts achievable")
    for cat in ['Power', 'Environmental Justice', 'Population', 'Water Stress']:
        s, mx, detail = score_breakdown[cat]
        print(f"  • {cat}: {s}/{mx} pts — {detail}")

    # Unscored categories
    print(f"\nUNSCORED CATEGORIES: 0/25 pts (no data)")
    for cat in ['Renewable Energy', 'CO2 Emissions', 'Community Engagement', 'Disclosure']:
        s, mx, detail = score_breakdown[cat]
        print(f"  • {cat}: {s}/{mx} pts — {detail}")

    # Issues
    if row['failing_criteria']:
        print(f"\n KEY ISSUES:")
        for issue in row['failing_criteria']:
            if 'No renewable' not in issue and 'No CO2' not in issue and 'No community' not in issue and 'No disclosure' not in issue:
                print(f"  • {issue}")


# Calculate SCORES
results = df.apply(calculate_exemption_score, axis=1)
df['exemption_score'] = results.apply(lambda x: x[0])
df['exemption_tier'] = results.apply(lambda x: x[1])
df['failing_criteria'] = results.apply(lambda x: x[2])

# Select 5 diverse examples - mix of high, medium, low scorers
df_sorted = df.sort_values('exemption_score')

sample_indices = [
    0,  # Lowest scorer
    len(df_sorted) // 4,  # Lower quartile
    len(df_sorted) // 2,  # Median
    3 * len(df_sorted) // 4,  # Upper quartile
    len(df_sorted) - 1  # Highest scorer
]

for i, idx in enumerate(sample_indices, 1):
    row = df_sorted.iloc[idx]
    print_detailed_breakdown(row, i)
