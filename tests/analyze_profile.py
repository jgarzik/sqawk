#!/usr/bin/env python3
"""Analyze samply profile data and report top hotspots."""
import json
import gzip
import sys
from collections import defaultdict

# Get profile path from command line or use default
profile_path = sys.argv[1] if len(sys.argv) > 1 else '/tmp/sqawk_profile.json.gz'
syms_path = profile_path.replace('.json.gz', '.json.syms.json')

# Load the symbols file
with open(syms_path) as f:
    syms_data = json.load(f)

string_table = syms_data['string_table']
libs = syms_data['data']

# Build address lookup: (lib_idx, address) -> symbol_name
# known_addresses is [[addr, sym_table_idx], ...]
# symbol_table[sym_table_idx]['symbol'] is index into string_table
addr_to_sym = {}
lib_names = []
for lib_idx, lib in enumerate(libs):
    lib_names.append(lib['debug_name'])
    sym_table = lib.get('symbol_table', [])
    known_addrs = lib.get('known_addresses', [])

    for addr, sym_idx in known_addrs:
        if sym_idx < len(sym_table):
            sym_entry = sym_table[sym_idx]
            name_idx = sym_entry['symbol']
            sym_name = string_table[name_idx]
            addr_to_sym[(lib_idx, addr)] = sym_name

print(f"Built lookup with {len(addr_to_sym)} symbols from {len(libs)} libraries")

# Load the profile
with gzip.open(profile_path, 'rt') as f:
    data = json.load(f)

thread = data['threads'][0]
strings = thread['stringArray']
func_names = thread['funcTable']['name']
func_resources = thread['funcTable']['resource']
frame_funcs = thread['frameTable']['func']
stack_prefixes = thread['stackTable']['prefix']
stack_frames = thread['stackTable']['frame']
sample_stacks = thread['samples']['stack']
sample_weights = thread['samples'].get('weight', [1] * len(sample_stacks))

# Map resource indices to library indices
res_table = thread['resourceTable']
res_libs = res_table.get('lib', [])

def get_func_name(func_idx):
    name_idx = func_names[func_idx]
    name = strings[name_idx]

    if name.startswith('0x'):
        addr = int(name, 16)
        res_idx = func_resources[func_idx]
        if res_idx is not None and res_idx >= 0 and res_idx < len(res_libs):
            lib_idx = res_libs[res_idx]
            if lib_idx is not None and (lib_idx, addr) in addr_to_sym:
                return addr_to_sym[(lib_idx, addr)]
    return name

# Count samples
self_counts = defaultdict(int)
total_counts = defaultdict(int)

for i, stack_idx in enumerate(sample_stacks):
    if stack_idx is None:
        continue

    weight = sample_weights[i] if i < len(sample_weights) else 1
    seen = set()
    first = True

    while stack_idx is not None:
        frame_idx = stack_frames[stack_idx]
        func_idx = frame_funcs[frame_idx]
        func_name = get_func_name(func_idx)

        if first:
            self_counts[func_name] += weight
            first = False

        if func_name not in seen:
            total_counts[func_name] += weight
            seen.add(func_name)

        stack_idx = stack_prefixes[stack_idx]

total_samples = sum(self_counts.values())
print(f"Total samples: {total_samples}")
print()

print("=" * 100)
print("TOP 10 HOTSPOTS BY SELF TIME (time directly in function)")
print("=" * 100)
sorted_self = sorted(self_counts.items(), key=lambda x: -x[1])
for i, (name, count) in enumerate(sorted_self[:10], 1):
    pct = 100.0 * count / total_samples
    print(f"{i:2}. {pct:5.1f}% ({count:6} samples) | {name[:95]}")

print()
print("=" * 100)
print("TOP 10 HOTSPOTS BY TOTAL TIME (function anywhere on stack)")
print("=" * 100)
sorted_total = sorted(total_counts.items(), key=lambda x: -x[1])
for i, (name, count) in enumerate(sorted_total[:10], 1):
    pct = 100.0 * count / total_samples
    print(f"{i:2}. {pct:5.1f}% ({count:6} samples) | {name[:95]}")
