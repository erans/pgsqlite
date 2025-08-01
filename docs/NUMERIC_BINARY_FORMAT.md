# PostgreSQL NUMERIC Binary Format

## Format Specification

PostgreSQL's NUMERIC type uses a custom binary format:

```
+--------+--------+--------+--------+
| ndigits| weight | sign   | dscale |
| (2b)   | (2b)   | (2b)   | (2b)   |
+--------+--------+--------+--------+
| digit1 | digit2 | ...    | digitN |
| (2b)   | (2b)   |        | (2b)   |
+--------+--------+--------+--------+
```

### Header (8 bytes):
- **ndigits** (int16): Number of digit groups (0 = zero value)
- **weight** (int16): Weight of first digit group
- **sign** (int16): 
  - 0x0000 = positive
  - 0x4000 = negative  
  - 0xC000 = NaN
  - 0xD000 = Infinity
  - 0xE000 = -Infinity
- **dscale** (int16): Display scale (digits after decimal point)

### Digits:
- Each digit is a 16-bit integer (0-9999)
- Represents 4 decimal digits
- Stored in big-endian order

## Examples

### 123.45
- Digits: [123, 4500]
- ndigits: 2
- weight: 0 (first digit group is 10^0)
- sign: 0x0000 (positive)
- dscale: 2 (two decimal places)

### -9876.543
- Digits: [9876, 5430]  
- ndigits: 2
- weight: 0
- sign: 0x4000 (negative)
- dscale: 3

### 0.0001
- Digits: [1]
- ndigits: 1
- weight: -1 (first digit group is 10^-4)
- sign: 0x0000
- dscale: 4

## Implementation Strategy

1. Parse decimal string or use rust_decimal
2. Break into groups of 4 digits
3. Calculate weight based on position
4. Encode header and digits