#!/usr/bin/env python3
"""Generate report figures: scalability chart and crime-hours distribution."""

import json
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import os

NTUA = '#0B3D6B'
GRAY = '#4A4A4A'
plt.rcParams.update({
    'font.size': 11,
    'axes.edgecolor': GRAY,
    'axes.labelcolor': GRAY,
    'xtick.color': GRAY,
    'ytick.color': GRAY,
    'axes.titlecolor': NTUA
})

# Output directory
out_dir = os.path.join(os.path.dirname(__file__), '..', 'report')
os.makedirs(out_dir, exist_ok=True)

# ========== Scalability Figure (Query 4) ==========
fig, ax = plt.subplots(1, 2, figsize=(9, 3.7))

# Vertical scaling
coresA = [2, 4, 8]
timeA = [74162, 63700, 65669]
sA = [t / 1000 for t in timeA]

ax[0].plot(coresA, sA, 'o-', color=NTUA, lw=2, ms=7)
ax[0].set_title('(A) Vertical scaling\n2 executors', fontsize=11)
ax[0].set_xlabel('Total cores')
ax[0].set_ylabel('Time (s)')
ax[0].set_xticks(coresA)
ax[0].set_ylim(min(sA) - 3, max(sA) + 5)
ax[0].grid(True, alpha=0.3)

for x, t in zip(coresA, timeA):
    ax[0].annotate(f'{t/1000:.0f}s', (x, t/1000),
                   textcoords='offset points', xytext=(0, 8),
                   ha='center', fontsize=9, color=GRAY)

# Horizontal scaling
labelsB = ['2x4c', '4x2c', '8x1c']
timeB = [65669, 36223, 204331]
sB = [t / 1000 for t in timeB]
colors = [GRAY, NTUA, '#A03020']

bars = ax[1].bar(labelsB, sB, color=colors, width=0.6)
ax[1].set_title('(B) Horizontal scaling\n8 cores fixed', fontsize=11)
ax[1].set_xlabel('Configuration (executors x cores)')
ax[1].set_ylabel('Time (s)')
ax[1].set_ylim(0, max(sB) + 22)
ax[1].grid(True, axis='y', alpha=0.3)

for b, t in zip(bars, timeB):
    ax[1].annotate(f'{t/1000:.0f}s', (b.get_x() + b.get_width()/2, t/1000),
                   textcoords='offset points', xytext=(0, 4),
                   ha='center', fontsize=9, color=GRAY)

fig.tight_layout()
fig.savefig(os.path.join(out_dir, 'fig_scalability.pdf'), bbox_inches='tight')
print('✓ fig_scalability.pdf')

# ========== Crime Hours Distribution (EDA) ==========
# Load precomputed hour counts (or regenerate from data)
hours_file = '/tmp/claude-1000/-home-gskourts-opt-projects-ntua-2nd-Semester-Big-Data-2------------------2025-2026-exercise-bigdata/d7228749-d63f-4f2a-bdfe-57715478299b/scratchpad/hours.json'

if os.path.exists(hours_file):
    with open(hours_file) as f:
        data = json.load(f)
    hc = data['hour_counts']
else:
    print('Warning: hours.json not found; skipping crime hours figure')
    exit(1)

hours = list(range(24))
vals = [hc.get(str(h), hc.get(h, 0)) / 1000 for h in hours]

def hour_to_part_color(h):
    if 5 <= h <= 11:
        return '#7CA6C8'  # Morning
    elif 12 <= h <= 16:
        return '#3E78A8'  # Afternoon
    elif 17 <= h <= 20:
        return NTUA  # Evening
    else:
        return '#16263A'  # Night

colors = [hour_to_part_color(h) for h in hours]

fig2, ax2 = plt.subplots(figsize=(9, 3.6))
ax2.bar(hours, vals, color=colors, width=0.85)
ax2.set_xlabel('Hour of day (TIME OCC, HH)')
ax2.set_ylabel('Crimes (thousands)')
ax2.set_title('Distribution of crimes by hour - 3.138.128 records')
ax2.set_xticks(hours)
ax2.grid(True, axis='y', alpha=0.3)

legend = [
    Patch(color='#7CA6C8', label='Morning 05-11'),
    Patch(color='#3E78A8', label='Afternoon 12-16'),
    Patch(color=NTUA, label='Evening 17-20'),
    Patch(color='#16263A', label='Night 21-04')
]
ax2.legend(handles=legend, fontsize=8, ncol=4, loc='upper center',
           frameon=False, bbox_to_anchor=(0.5, -0.22))

fig2.tight_layout()
fig2.savefig(os.path.join(out_dir, 'fig_crime_hours.pdf'), bbox_inches='tight')
print('✓ fig_crime_hours.pdf')

print('Done. Figures saved to:', out_dir)
