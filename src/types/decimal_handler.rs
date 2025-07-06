use rust_decimal::Decimal;
use std::str::FromStr;
use byteorder::{BigEndian, ByteOrder};

pub struct DecimalHandler;

impl DecimalHandler {
    /// Convert a string to rust_decimal::Decimal
    pub fn parse_decimal(s: &str) -> Result<Decimal, String> {
        Decimal::from_str(s)
            .map_err(|e| format!("Invalid numeric value: {}", e))
    }
    
    /// Convert rust_decimal to PostgreSQL binary NUMERIC format
    pub fn encode_numeric(decimal: &Decimal) -> Vec<u8> {
        // PostgreSQL NUMERIC format:
        // - ndigits (int16): number of digit groups
        // - weight (int16): weight of first digit group (in base NBASE)
        // - sign (int16): NUMERIC_POS, NUMERIC_NEG, or NUMERIC_NAN
        // - dscale (int16): display scale (digits after decimal point)
        // - digits: array of int16, each containing up to 4 decimal digits (NBASE=10000)
        
        const _NBASE: i32 = 10000;
        const NUMERIC_POS: u16 = 0x0000;
        const NUMERIC_NEG: u16 = 0x4000;
        
        let mut result = Vec::new();
        
        // Get string representation to work with digits
        let s = decimal.to_string();
        let (sign, digits_str) = if s.starts_with('-') {
            (NUMERIC_NEG, &s[1..])
        } else {
            (NUMERIC_POS, s.as_str())
        };
        
        // Split into integer and fractional parts
        let parts: Vec<&str> = digits_str.split('.').collect();
        let int_part = parts[0];
        let frac_part = parts.get(1).copied().unwrap_or("");
        
        // Remove leading zeros from integer part
        let int_part = int_part.trim_start_matches('0');
        let int_part = if int_part.is_empty() { "0" } else { int_part };
        
        if int_part == "0" && frac_part.trim_end_matches('0').is_empty() {
            // Zero value
            result.extend_from_slice(&0i16.to_be_bytes()); // ndigits = 0
            result.extend_from_slice(&0i16.to_be_bytes()); // weight = 0
            result.extend_from_slice(&(NUMERIC_POS as i16).to_be_bytes()); // sign = POS
            result.extend_from_slice(&(decimal.scale() as i16).to_be_bytes()); // dscale
            return result;
        }
        
        // Group digits into NBASE (10000) chunks starting from the decimal point
        let mut digit_groups = Vec::new();
        
        // Process integer part from right to left (chunking from the right)
        let int_digits: Vec<char> = int_part.chars().collect();
        let mut int_groups = Vec::new();
        
        // Start from the end and work backwards
        let mut pos = int_digits.len();
        while pos > 0 {
            let start = if pos >= 4 { pos - 4 } else { 0 };
            let chunk = &int_digits[start..pos];
            
            let mut group_val = 0i16;
            let mut multiplier = 1;
            for &ch in chunk.iter().rev() {
                let digit = ch.to_digit(10).unwrap() as i16;
                group_val += digit * multiplier;
                multiplier *= 10;
            }
            int_groups.push(group_val);
            pos = start;
        }
        
        // Reverse to get correct order (most significant first)
        int_groups.reverse();
        let int_group_count = int_groups.len();
        digit_groups.extend(int_groups);
        
        // Process fractional part from left to right
        let frac_digits: Vec<char> = frac_part.chars().collect();
        for chunk_start in (0..frac_digits.len()).step_by(4) {
            let chunk_end = (chunk_start + 4).min(frac_digits.len());
            
            let mut group_val = 0i16;
            for (i, &ch) in frac_digits[chunk_start..chunk_end].iter().enumerate() {
                let digit = ch.to_digit(10).unwrap() as i16;
                group_val += digit * 10i16.pow((3 - i) as u32);
            }
            digit_groups.push(group_val);
        }
        
        // Remove trailing zero groups from fractional part
        while digit_groups.len() > 1 && digit_groups.last() == Some(&0) {
            digit_groups.pop();
        }
        
        // Count leading zeros before removing them
        let mut leading_zeros = 0;
        while digit_groups.len() > 1 && digit_groups[0] == 0 {
            digit_groups.remove(0);
            leading_zeros += 1;
        }
        
        // Calculate weight - the power of NBASE for the first digit group
        // Weight 0 means the first group represents values 0-9999
        // Weight 1 means the first group represents values 10000-99999999
        // Adjust for removed leading zeros
        let weight = if int_group_count > 0 {
            int_group_count as i16 - 1 - leading_zeros as i16
        } else {
            // Fractional number less than 1
            -(leading_zeros as i16 + 1)
        };
        
        // Build result
        result.extend_from_slice(&(digit_groups.len() as i16).to_be_bytes()); // ndigits
        result.extend_from_slice(&weight.to_be_bytes()); // weight
        result.extend_from_slice(&(sign as i16).to_be_bytes()); // sign
        result.extend_from_slice(&(frac_part.len() as i16).to_be_bytes()); // dscale
        
        // Add digit groups
        for digit in digit_groups {
            result.extend_from_slice(&digit.to_be_bytes());
        }
        
        result
    }
    
    /// Decode PostgreSQL binary NUMERIC format to rust_decimal
    pub fn decode_numeric(bytes: &[u8]) -> Result<Decimal, String> {
        if bytes.len() < 8 {
            return Err("Invalid NUMERIC binary format: too short".to_string());
        }
        
        let ndigits = BigEndian::read_i16(&bytes[0..2]);
        let weight = BigEndian::read_i16(&bytes[2..4]);
        let sign = BigEndian::read_u16(&bytes[4..6]);
        let dscale = BigEndian::read_i16(&bytes[6..8]);
        
        const _NUMERIC_POS: u16 = 0x0000;
        const NUMERIC_NEG: u16 = 0x4000;
        const NUMERIC_NAN: u16 = 0xC000;
        const _NBASE: i32 = 10000;
        
        // Handle special cases
        if sign == NUMERIC_NAN {
            return Err("NUMERIC NaN not supported".to_string());
        }
        
        if ndigits == 0 {
            return Ok(Decimal::ZERO);
        }
        
        // Read digit groups
        let mut digits = Vec::new();
        let mut offset = 8;
        for _ in 0..ndigits {
            if offset + 2 > bytes.len() {
                return Err("Invalid NUMERIC binary format: truncated".to_string());
            }
            digits.push(BigEndian::read_i16(&bytes[offset..offset+2]));
            offset += 2;
        }
        
        // Reconstruct the number
        let mut result = String::new();
        if sign == NUMERIC_NEG {
            result.push('-');
        }
        
        // Convert digit groups to string, handling the weight correctly
        // Weight indicates the power of NBASE (10000) for the first digit group
        let mut all_digits = String::new();
        
        // First, convert all digit groups to a continuous string
        for (i, &digit) in digits.iter().enumerate() {
            if i == 0 {
                // First digit group - no leading zeros
                all_digits.push_str(&digit.to_string());
            } else {
                // Subsequent groups - pad with zeros to ensure 4 digits
                all_digits.push_str(&format!("{digit:04}"));
            }
        }
        
        // Calculate decimal position
        // The first digit group's position is determined by weight
        // weight = 0 means the first digit group represents 0-9999 (ones to thousands)
        // weight = -1 means the first digit group represents 0.0001-0.9999
        let first_group_digits = if digits[0] >= 1000 { 4 }
                            else if digits[0] >= 100 { 3 }
                            else if digits[0] >= 10 { 2 }
                            else { 1 };
        
        let decimal_position = first_group_digits + weight * 4;
        
        if decimal_position <= 0 {
            // All digits are fractional
            result.push_str("0.");
            for _ in 0..(-decimal_position) {
                result.push('0');
            }
            result.push_str(&all_digits);
        } else if decimal_position as usize >= all_digits.len() {
            // All digits are integer
            result.push_str(&all_digits);
            // Add trailing zeros if needed
            for _ in all_digits.len()..(decimal_position as usize) {
                result.push('0');
            }
            if dscale > 0 {
                result.push('.');
                for _ in 0..dscale {
                    result.push('0');
                }
            }
        } else {
            // Mixed integer and fractional
            let (int_part, frac_part) = all_digits.split_at(decimal_position as usize);
            result.push_str(int_part);
            if !frac_part.is_empty() || dscale > 0 {
                result.push('.');
                result.push_str(frac_part);
                // Pad fractional part if needed
                for _ in frac_part.len()..dscale as usize {
                    result.push('0');
                }
            }
        }
        
        Decimal::from_str(&result)
            .map_err(|e| format!("Failed to parse reconstructed decimal: {}", e))
    }
    
    /// Validate that a string can be parsed as a valid NUMERIC value
    pub fn validate_numeric_string(s: &str) -> Result<(), String> {
        Self::parse_decimal(s)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_parse_decimal() {
        assert!(DecimalHandler::parse_decimal("123.45").is_ok());
        assert!(DecimalHandler::parse_decimal("-123.45").is_ok());
        assert!(DecimalHandler::parse_decimal("0").is_ok());
        assert!(DecimalHandler::parse_decimal("0.0").is_ok());
        assert!(DecimalHandler::parse_decimal("invalid").is_err());
    }
    
    #[test]
    fn test_encode_decode_numeric() {
        let test_cases = vec![
            "123.45",
            "-123.45",
            "0",
            "0.0",
            "1234567890.123456",
            "0.0001", 
            "99999.9999",
        ];
        
        for case in test_cases {
            let decimal = DecimalHandler::parse_decimal(case).unwrap();
            let encoded = DecimalHandler::encode_numeric(&decimal);
            let decoded = DecimalHandler::decode_numeric(&encoded).unwrap();
            
            // Compare values, not strings (to handle different representations of same value)
            assert_eq!(decimal, decoded, "Failed for case: {}", case);
        }
    }
}