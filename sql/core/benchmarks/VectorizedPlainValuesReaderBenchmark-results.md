# VectorizedPlainValuesReader JMH Benchmark Results

## Environment
- JVM: Zulu JDK 17
- OS: macOS (Darwin)
- CPU: Apple Silicon (MacBook Pro)
- JMH Version: 1.37
- JMH Config: Warmup 3x2s, Measurement 5x2s, Fork 1, Throughput mode
- Batch Size: 4096 elements

---

## Step 0: Baseline (Before Optimization)

| Benchmark | bufferType | vectorType | Score (ops/s) | Error (ops/s) |
|-----------|-----------|------------|--------------|---------------|
| readIntegers | HEAP | ON_HEAP | 2,957,004 | +/- 383,920 |
| readIntegers | HEAP | OFF_HEAP | 3,411,835 | +/- 118,018 |
| readIntegers | DIRECT | ON_HEAP | 1,365,973 | +/- 144,853 |
| readIntegers | DIRECT | OFF_HEAP | 1,524,349 | +/- 326,918 |
| readLongs | HEAP | ON_HEAP | 2,193,786 | +/- 396,951 |
| readLongs | HEAP | OFF_HEAP | 2,239,047 | +/- 330,077 |
| readLongs | DIRECT | ON_HEAP | 554,308 | +/- 212,809 |
| readLongs | DIRECT | OFF_HEAP | 1,529,224 | +/- 380,394 |
| readFloats | HEAP | ON_HEAP | 4,732,282 | +/- 250,912 |
| readFloats | HEAP | OFF_HEAP | 3,127,614 | +/- 609,169 |
| readFloats | DIRECT | ON_HEAP | 1,031,327 | +/- 131,742 |
| readFloats | DIRECT | OFF_HEAP | 1,421,838 | +/- 46,323 |
| readDoubles | HEAP | ON_HEAP | 2,206,259 | +/- 867,135 |
| readDoubles | HEAP | OFF_HEAP | 2,285,897 | +/- 443,121 |
| readDoubles | DIRECT | ON_HEAP | 697,402 | +/- 15,158 |
| readDoubles | DIRECT | OFF_HEAP | 703,139 | +/- 2,347 |
| readBooleans | HEAP | ON_HEAP | 970,635 | +/- 843,192 |
| readBooleans | HEAP | OFF_HEAP | 1,277,167 | +/- 141,116 |
| readBooleans | DIRECT | ON_HEAP | 1,397,326 | +/- 67,344 |
| readBooleans | DIRECT | OFF_HEAP | 1,334,941 | +/- 145,728 |
| readBytes | HEAP | ON_HEAP | 1,627,965 | +/- 75,835 |
| readBytes | HEAP | OFF_HEAP | 1,598,881 | +/- 206,565 |
| readBytes | DIRECT | ON_HEAP | 1,689,879 | +/- 109,569 |
| readBytes | DIRECT | OFF_HEAP | 1,636,143 | +/- 98,193 |
| readShorts | HEAP | ON_HEAP | 1,642,031 | +/- 146,744 |
| readShorts | HEAP | OFF_HEAP | 1,679,673 | +/- 169,598 |
| readShorts | DIRECT | ON_HEAP | 1,678,874 | +/- 138,808 |
| readShorts | DIRECT | OFF_HEAP | 1,379,053 | +/- 94,037 |
| readBinary | HEAP | ON_HEAP | 133,620 | +/- 3,878 |
| readBinary | HEAP | OFF_HEAP | 104,176 | +/- 11,260 |
| readBinary | DIRECT | ON_HEAP | 78,898 | +/- 2,325 |
| readBinary | DIRECT | OFF_HEAP | 68,972 | +/- 3,163 |
| readUnsignedIntegers | HEAP | ON_HEAP | 807,569 | +/- 30,930 |
| readUnsignedIntegers | HEAP | OFF_HEAP | 1,295,994 | +/- 87,401 |
| readUnsignedIntegers | DIRECT | ON_HEAP | 1,077,144 | +/- 137,744 |
| readUnsignedIntegers | DIRECT | OFF_HEAP | 1,312,479 | +/- 142,320 |
| readIntegersWithRebase | HEAP | ON_HEAP | 7,035 | +/- 1,809 |
| readIntegersWithRebase | HEAP | OFF_HEAP | 7,158 | +/- 1,175 |
| readIntegersWithRebase | DIRECT | ON_HEAP | 7,187 | +/- 814 |
| readIntegersWithRebase | DIRECT | OFF_HEAP | 6,741 | +/- 2,353 |

### Key Observations (Baseline)
1. **DIRECT buffer penalty for fixed-width types**: readIntegers DIRECT is ~2x slower than HEAP; readLongs DIRECT+ON_HEAP is ~4x slower; readDoubles DIRECT is ~3x slower than HEAP. This confirms the per-element loop fallback is a major bottleneck.
2. **readBooleans**: Relatively consistent across buffer types (~1M-1.4M ops/s), indicating the per-byte `in.read()` overhead is the dominant cost regardless of buffer type.
3. **readBytes/readShorts**: ~1.4-1.7M ops/s across all configs, limited by per-element extraction from 4-byte INT32 encoding.
4. **readBinary**: 69K-134K ops/s, significantly slower due to variable-length nature; DIRECT is ~40-48% slower than HEAP (per-value byte[] allocation in DIRECT path).
5. **readUnsignedIntegers**: 808K-1.3M ops/s, limited by per-element loop with type conversion.
6. **readIntegersWithRebase**: ~7K ops/s, extremely slow due to `RebaseDateTime.lastSwitchJulianDay()` comparison (but this is dominated by the rebase check logic, not buffer access).
