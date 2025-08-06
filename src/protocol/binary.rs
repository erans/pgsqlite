use bytes::{BufMut, BytesMut};
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use std::convert::TryInto;
use std::str::FromStr;
use crate::types::{PgType, DecimalHandler};

/// Binary format encoders for PostgreSQL types
pub struct BinaryEncoder;

impl BinaryEncoder {
    /// Encode a boolean value (OID 16)
    #[inline]
    pub fn encode_bool(value: bool) -> Vec<u8> {
        vec![if value { 1 } else { 0 }]
    }

    /// Encode an int2/smallint value (OID 21)
    #[inline]
    pub fn encode_int2(value: i16) -> Vec<u8> {
        value.to_be_bytes().to_vec()
    }

    /// Encode an int4/integer value (OID 23)
    #[inline]
    pub fn encode_int4(value: i32) -> Vec<u8> {
        value.to_be_bytes().to_vec()
    }

    /// Encode an int8/bigint value (OID 20)
    #[inline]
    pub fn encode_int8(value: i64) -> Vec<u8> {
        value.to_be_bytes().to_vec()
    }

    /// Encode a float4/real value (OID 700)
    #[inline]
    pub fn encode_float4(value: f32) -> Vec<u8> {
        value.to_be_bytes().to_vec()
    }

    /// Encode a float8/double precision value (OID 701)
    #[inline]
    pub fn encode_float8(value: f64) -> Vec<u8> {
        value.to_be_bytes().to_vec()
    }

    /// Encode a text/varchar value (OID 25, 1043)
    /// Binary format is the same as text format for these types
    #[inline]
    pub fn encode_text(value: &str) -> Vec<u8> {
        value.as_bytes().to_vec()
    }

    /// Encode a bytea value (OID 17)
    /// Binary format is just the raw bytes
    #[inline]
    pub fn encode_bytea(value: &[u8]) -> Vec<u8> {
        value.to_vec()
    }

    /// Encode a numeric/decimal value (OID 1700)
    /// Uses PostgreSQL's binary NUMERIC format
    pub fn encode_numeric(value: &Decimal) -> Vec<u8> {
        DecimalHandler::encode_numeric(value)
    }
    
    /// Encode a UUID value (OID 2950)
    /// Binary format is 16 bytes raw UUID
    pub fn encode_uuid(uuid_str: &str) -> Result<Vec<u8>, String> {
        // Remove hyphens and validate length
        let hex_str = uuid_str.replace('-', "");
        if hex_str.len() != 32 {
            return Err("Invalid UUID format".to_string());
        }
        
        // Convert hex string to bytes
        let mut bytes = Vec::with_capacity(16);
        for i in (0..32).step_by(2) {
            let byte = u8::from_str_radix(&hex_str[i..i+2], 16)
                .map_err(|_| "Invalid UUID hex characters")?;
            bytes.push(byte);
        }
        
        Ok(bytes)
    }
    
    /// Encode JSON value (OID 114)
    /// Binary format is the same as text for JSON
    pub fn encode_json(json_str: &str) -> Vec<u8> {
        json_str.as_bytes().to_vec()
    }
    
    /// Encode JSONB value (OID 3802)
    /// Binary format has a 1-byte version header
    pub fn encode_jsonb(json_str: &str) -> Vec<u8> {
        let mut result = Vec::with_capacity(json_str.len() + 1);
        result.push(1); // JSONB version 1
        result.extend_from_slice(json_str.as_bytes());
        result
    }
    
    /// Encode MONEY value (OID 790)
    /// Binary format is 8-byte integer representing cents * 100
    pub fn encode_money(amount_str: &str) -> Result<Vec<u8>, String> {
        // Parse the string, removing currency symbols and commas
        let clean_str = amount_str
            .replace(['$', ','], "")
            .trim()
            .to_string();
        
        // Parse as decimal to handle fractional cents
        let decimal = Decimal::from_str(&clean_str)
            .map_err(|e| format!("Invalid money value: {e}"))?;
        
        // Convert to cents (multiply by 100)
        let cents = decimal * Decimal::from(100);
        
        // Convert to i64
        let cents_i64 = cents.round().to_i64()
            .ok_or_else(|| "Money value too large".to_string())?;
        
        Ok(cents_i64.to_be_bytes().to_vec())
    }
    
    /// Encode an array value
    /// PostgreSQL array binary format:
    /// - ndim (i32): number of dimensions
    /// - dataoffset (i32): offset to data, 0 if no NULLs
    /// - elemtype (i32): element type OID
    /// - For each dimension:
    ///   - dim_size (i32): number of elements in this dimension
    ///   - lower_bound (i32): lower bound (typically 1)
    /// - NULL bitmap (optional): bit array indicating NULL positions
    /// - Elements: each prefixed with length (i32), -1 for NULL
    pub fn encode_array(
        json_array_str: &str,
        elem_type_oid: i32,
    ) -> Result<Vec<u8>, String> {
        // Parse JSON array
        let array: serde_json::Value = serde_json::from_str(json_array_str)
            .map_err(|e| format!("Invalid JSON array: {e}"))?;
        
        let elements = array.as_array()
            .ok_or_else(|| "Not a JSON array".to_string())?;
        
        if elements.is_empty() {
            // Empty array
            let mut result = Vec::new();
            result.extend_from_slice(&0i32.to_be_bytes()); // ndim = 0
            result.extend_from_slice(&0i32.to_be_bytes()); // dataoffset = 0
            result.extend_from_slice(&elem_type_oid.to_be_bytes()); // elemtype
            return Ok(result);
        }
        
        // Check for NULLs
        let has_nulls = elements.iter().any(|e| e.is_null());
        
        let mut result = Vec::new();
        
        // Header
        result.extend_from_slice(&1i32.to_be_bytes()); // ndim = 1 (1D array)
        result.extend_from_slice(&(if has_nulls { 1i32 } else { 0i32 }).to_be_bytes()); // dataoffset placeholder
        result.extend_from_slice(&elem_type_oid.to_be_bytes()); // elemtype
        
        // Dimension info
        result.extend_from_slice(&(elements.len() as i32).to_be_bytes()); // dim_size
        result.extend_from_slice(&1i32.to_be_bytes()); // lower_bound = 1
        
        // NULL bitmap if needed
        let bitmap_start = result.len();
        if has_nulls {
            // Create bitmap (1 bit per element, padded to byte boundary)
            let bitmap_bytes = elements.len().div_ceil(8);
            let mut bitmap = vec![0u8; bitmap_bytes];
            
            for (i, elem) in elements.iter().enumerate() {
                if !elem.is_null() {
                    let byte_idx = i / 8;
                    let bit_idx = i % 8;
                    bitmap[byte_idx] |= 1 << (7 - bit_idx);
                }
            }
            
            result.extend_from_slice(&bitmap);
        }
        
        // Update dataoffset if we have nulls
        if has_nulls {
            let dataoffset = (bitmap_start + elements.len().div_ceil(8)) as i32;
            result[4..8].copy_from_slice(&dataoffset.to_be_bytes());
        }
        
        // Encode elements
        for elem in elements {
            if elem.is_null() {
                // NULL element
                result.extend_from_slice(&(-1i32).to_be_bytes());
            } else {
                // Encode element based on type
                let elem_bytes = match elem_type_oid {
                    t if t == PgType::Int4.to_oid() => {
                        elem.as_i64()
                            .and_then(|v| v.try_into().ok())
                            .map(|v: i32| v.to_be_bytes().to_vec())
                    }
                    t if t == PgType::Int8.to_oid() => {
                        elem.as_i64()
                            .map(|v| v.to_be_bytes().to_vec())
                    }
                    t if t == PgType::Text.to_oid() || t == PgType::Varchar.to_oid() => {
                        elem.as_str()
                            .map(|s| s.as_bytes().to_vec())
                    }
                    t if t == PgType::Float8.to_oid() => {
                        elem.as_f64()
                            .map(|v| v.to_be_bytes().to_vec())
                    }
                    t if t == PgType::Bool.to_oid() => {
                        elem.as_bool()
                            .map(|v| vec![if v { 1 } else { 0 }])
                    }
                    _ => {
                        // Fall back to string representation
                        Some(elem.to_string().into_bytes())
                    }
                };
                
                match elem_bytes {
                    Some(bytes) => {
                        result.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                        result.extend_from_slice(&bytes);
                    }
                    None => {
                        return Err(format!("Cannot encode array element: {elem:?}"));
                    }
                }
            }
        }
        
        Ok(result)
    }
    
    /// Encode a range type value
    /// PostgreSQL range binary format:
    /// - flags (1 byte): 0x01=empty, 0x02=LB_INC, 0x04=UB_INC, 0x08=LB_INF, 0x10=UB_INF
    /// - lower bound length + data (if not infinite)
    /// - upper bound length + data (if not infinite)
    pub fn encode_int4range(range_str: &str) -> Result<Vec<u8>, String> {
        let trimmed = range_str.trim();
        let mut result = Vec::new();
        
        // Handle empty range
        if trimmed == "empty" {
            result.push(0x01); // RANGE_EMPTY flag
            return Ok(result);
        }
        
        // Parse range format: [lower,upper), (lower,upper], etc.
        if trimmed.len() < 3 {
            return Err("Invalid range format".to_string());
        }
        
        let lower_inclusive = trimmed.starts_with('[');
        let upper_inclusive = trimmed.ends_with(']');
        
        // Extract bounds
        let inner = &trimmed[1..trimmed.len()-1];
        let parts: Vec<&str> = inner.split(',').collect();
        
        if parts.len() != 2 {
            return Err("Invalid range format: expected two bounds".to_string());
        }
        
        let lower_str = parts[0].trim();
        let upper_str = parts[1].trim();
        
        // Calculate flags
        let mut flags = 0u8;
        if lower_inclusive {
            flags |= 0x02; // LB_INC
        }
        if upper_inclusive {
            flags |= 0x04; // UB_INC
        }
        
        // Check for infinite bounds
        let lower_infinite = lower_str == "-infinity" || lower_str.is_empty();
        let upper_infinite = upper_str == "infinity" || upper_str.is_empty();
        
        if lower_infinite {
            flags |= 0x08; // LB_INF
        }
        if upper_infinite {
            flags |= 0x10; // UB_INF
        }
        
        result.push(flags);
        
        // Encode lower bound if not infinite
        if !lower_infinite {
            let lower_val: i32 = lower_str.parse()
                .map_err(|_| format!("Invalid lower bound: {lower_str}"))?;
            let lower_bytes = lower_val.to_be_bytes();
            result.extend_from_slice(&(lower_bytes.len() as i32).to_be_bytes());
            result.extend_from_slice(&lower_bytes);
        }
        
        // Encode upper bound if not infinite
        if !upper_infinite {
            let upper_val: i32 = upper_str.parse()
                .map_err(|_| format!("Invalid upper bound: {upper_str}"))?;
            let upper_bytes = upper_val.to_be_bytes();
            result.extend_from_slice(&(upper_bytes.len() as i32).to_be_bytes());
            result.extend_from_slice(&upper_bytes);
        }
        
        Ok(result)
    }
    
    /// Encode an int8range value
    pub fn encode_int8range(range_str: &str) -> Result<Vec<u8>, String> {
        let trimmed = range_str.trim();
        let mut result = Vec::new();
        
        // Handle empty range
        if trimmed == "empty" {
            result.push(0x01); // RANGE_EMPTY flag
            return Ok(result);
        }
        
        // Parse range format
        if trimmed.len() < 3 {
            return Err("Invalid range format".to_string());
        }
        
        let lower_inclusive = trimmed.starts_with('[');
        let upper_inclusive = trimmed.ends_with(']');
        
        let inner = &trimmed[1..trimmed.len()-1];
        let parts: Vec<&str> = inner.split(',').collect();
        
        if parts.len() != 2 {
            return Err("Invalid range format: expected two bounds".to_string());
        }
        
        let lower_str = parts[0].trim();
        let upper_str = parts[1].trim();
        
        // Calculate flags
        let mut flags = 0u8;
        if lower_inclusive {
            flags |= 0x02; // LB_INC
        }
        if upper_inclusive {
            flags |= 0x04; // UB_INC
        }
        
        let lower_infinite = lower_str == "-infinity" || lower_str.is_empty();
        let upper_infinite = upper_str == "infinity" || upper_str.is_empty();
        
        if lower_infinite {
            flags |= 0x08; // LB_INF
        }
        if upper_infinite {
            flags |= 0x10; // UB_INF
        }
        
        result.push(flags);
        
        // Encode bounds
        if !lower_infinite {
            let lower_val: i64 = lower_str.parse()
                .map_err(|_| format!("Invalid lower bound: {lower_str}"))?;
            let lower_bytes = lower_val.to_be_bytes();
            result.extend_from_slice(&(lower_bytes.len() as i32).to_be_bytes());
            result.extend_from_slice(&lower_bytes);
        }
        
        if !upper_infinite {
            let upper_val: i64 = upper_str.parse()
                .map_err(|_| format!("Invalid upper bound: {upper_str}"))?;
            let upper_bytes = upper_val.to_be_bytes();
            result.extend_from_slice(&(upper_bytes.len() as i32).to_be_bytes());
            result.extend_from_slice(&upper_bytes);
        }
        
        Ok(result)
    }
    
    /// Encode a numrange value
    pub fn encode_numrange(range_str: &str) -> Result<Vec<u8>, String> {
        let trimmed = range_str.trim();
        let mut result = Vec::new();
        
        // Handle empty range
        if trimmed == "empty" {
            result.push(0x01); // RANGE_EMPTY flag
            return Ok(result);
        }
        
        // Parse range format
        if trimmed.len() < 3 {
            return Err("Invalid range format".to_string());
        }
        
        let lower_inclusive = trimmed.starts_with('[');
        let upper_inclusive = trimmed.ends_with(']');
        
        let inner = &trimmed[1..trimmed.len()-1];
        let parts: Vec<&str> = inner.split(',').collect();
        
        if parts.len() != 2 {
            return Err("Invalid range format: expected two bounds".to_string());
        }
        
        let lower_str = parts[0].trim();
        let upper_str = parts[1].trim();
        
        // Calculate flags
        let mut flags = 0u8;
        if lower_inclusive {
            flags |= 0x02; // LB_INC
        }
        if upper_inclusive {
            flags |= 0x04; // UB_INC
        }
        
        let lower_infinite = lower_str == "-infinity" || lower_str.is_empty();
        let upper_infinite = upper_str == "infinity" || upper_str.is_empty();
        
        if lower_infinite {
            flags |= 0x08; // LB_INF
        }
        if upper_infinite {
            flags |= 0x10; // UB_INF
        }
        
        result.push(flags);
        
        // Encode numeric bounds using DecimalHandler
        if !lower_infinite {
            let lower_decimal = Decimal::from_str(lower_str)
                .map_err(|e| format!("Invalid lower bound: {e}"))?;
            let lower_bytes = DecimalHandler::encode_numeric(&lower_decimal);
            result.extend_from_slice(&(lower_bytes.len() as i32).to_be_bytes());
            result.extend_from_slice(&lower_bytes);
        }
        
        if !upper_infinite {
            let upper_decimal = Decimal::from_str(upper_str)
                .map_err(|e| format!("Invalid upper bound: {e}"))?;
            let upper_bytes = DecimalHandler::encode_numeric(&upper_decimal);
            result.extend_from_slice(&(upper_bytes.len() as i32).to_be_bytes());
            result.extend_from_slice(&upper_bytes);
        }
        
        Ok(result)
    }
    
    /// Encode DATE (days since 2000-01-01)
    pub fn encode_date(unix_timestamp: f64) -> Vec<u8> {
        // For dates stored as INTEGER days since epoch in SQLite, treat as days
        // For dates stored as REAL Unix timestamps, convert from seconds
        if unix_timestamp < 100000.0 {
            // This looks like days since epoch (1970-01-01), convert to PostgreSQL days since 2000-01-01
            let days_since_1970 = unix_timestamp as i32;
            let days_since_2000 = days_since_1970 - 10957; // 10957 days between 1970-01-01 and 2000-01-01
            days_since_2000.to_be_bytes().to_vec()
        } else {
            // This looks like seconds since epoch, convert to days since 2000-01-01
            const PG_EPOCH_OFFSET: i64 = 946684800; // seconds between 1970-01-01 and 2000-01-01
            const SECS_PER_DAY: i64 = 86400;
            let unix_secs = unix_timestamp.trunc() as i64;
            let pg_days = ((unix_secs - PG_EPOCH_OFFSET) / SECS_PER_DAY) as i32;
            pg_days.to_be_bytes().to_vec()
        }
    }
    
    /// Encode TIME (microseconds since midnight)
    pub fn encode_time(microseconds_since_midnight: f64) -> Vec<u8> {
        // The input is already in microseconds, just convert to i64
        let micros = microseconds_since_midnight.round() as i64;
        micros.to_be_bytes().to_vec()
    }
    
    /// Encode TIMESTAMP/TIMESTAMPTZ (microseconds since epoch to PostgreSQL format)
    pub fn encode_timestamp(unix_microseconds: f64) -> Vec<u8> {
        const PG_EPOCH_OFFSET: i64 = 946684800 * 1_000_000; // microseconds between 1970-01-01 and 2000-01-01
        let unix_micros = unix_microseconds.round() as i64;
        let pg_micros = unix_micros - PG_EPOCH_OFFSET;
        pg_micros.to_be_bytes().to_vec()
    }
    
    /// Encode INTERVAL (microseconds, days, months)
    pub fn encode_interval(total_seconds: f64) -> Vec<u8> {
        // For simple intervals, encode as microseconds + 0 days + 0 months
        let micros = (total_seconds * 1_000_000.0).round() as i64;
        let mut bytes = Vec::with_capacity(16);
        bytes.extend_from_slice(&micros.to_be_bytes());
        bytes.extend_from_slice(&0i32.to_be_bytes()); // days
        bytes.extend_from_slice(&0i32.to_be_bytes()); // months
        bytes
    }

    /// Encode CIDR value (OID 650)
    /// Binary format: 1 byte family + 1 byte bits + 1 byte is_cidr + 1 byte addr_len + addr bytes
    pub fn encode_cidr(cidr_str: &str) -> Result<Vec<u8>, String> {
        let trimmed = cidr_str.trim();
        let mut result = Vec::new();
        
        // Parse CIDR format: address/prefix_length
        let (addr_str, prefix_len) = if let Some(slash_pos) = trimmed.find('/') {
            let addr = &trimmed[..slash_pos];
            let prefix = &trimmed[slash_pos + 1..];
            let len = prefix.parse::<u8>()
                .map_err(|_| format!("Invalid prefix length: {prefix}"))?;
            (addr, len)
        } else {
            return Err("CIDR must include prefix length".to_string());
        };
        
        // Determine if IPv4 or IPv6
        if addr_str.contains(':') {
            // IPv6
            let octets = Self::parse_ipv6(addr_str)?;
            if prefix_len > 128 {
                return Err("IPv6 prefix length cannot exceed 128".to_string());
            }
            
            result.push(2); // AF_INET6
            result.push(prefix_len); // bits
            result.push(1); // is_cidr = true
            result.push(16); // addr_len = 16 bytes for IPv6
            result.extend_from_slice(&octets);
        } else {
            // IPv4
            let octets = Self::parse_ipv4(addr_str)?;
            if prefix_len > 32 {
                return Err("IPv4 prefix length cannot exceed 32".to_string());
            }
            
            result.push(1); // AF_INET
            result.push(prefix_len); // bits
            result.push(1); // is_cidr = true
            result.push(4); // addr_len = 4 bytes for IPv4
            result.extend_from_slice(&octets);
        }
        
        Ok(result)
    }
    
    /// Encode INET value (OID 869)
    /// Binary format: same as CIDR but is_cidr flag is 0
    pub fn encode_inet(inet_str: &str) -> Result<Vec<u8>, String> {
        let trimmed = inet_str.trim();
        let mut result = Vec::new();
        
        // Parse INET format: address or address/prefix_length
        let (addr_str, prefix_len) = if let Some(slash_pos) = trimmed.find('/') {
            let addr = &trimmed[..slash_pos];
            let prefix = &trimmed[slash_pos + 1..];
            let len = prefix.parse::<u8>()
                .map_err(|_| format!("Invalid prefix length: {prefix}"))?;
            (addr, len)
        } else if trimmed.contains(':') {
            // IPv6 without prefix - default to /128
            (trimmed, 128)
        } else {
            // IPv4 without prefix - default to /32
            (trimmed, 32)
        };
        
        // Determine if IPv4 or IPv6
        if addr_str.contains(':') {
            // IPv6
            let octets = Self::parse_ipv6(addr_str)?;
            if prefix_len > 128 {
                return Err("IPv6 prefix length cannot exceed 128".to_string());
            }
            
            result.push(2); // AF_INET6
            result.push(prefix_len); // bits
            result.push(0); // is_cidr = false
            result.push(16); // addr_len = 16 bytes for IPv6
            result.extend_from_slice(&octets);
        } else {
            // IPv4
            let octets = Self::parse_ipv4(addr_str)?;
            if prefix_len > 32 {
                return Err("IPv4 prefix length cannot exceed 32".to_string());
            }
            
            result.push(1); // AF_INET
            result.push(prefix_len); // bits
            result.push(0); // is_cidr = false
            result.push(4); // addr_len = 4 bytes for IPv4
            result.extend_from_slice(&octets);
        }
        
        Ok(result)
    }
    
    /// Encode MACADDR value (OID 829)
    /// Binary format: 6 bytes representing the MAC address
    pub fn encode_macaddr(mac_str: &str) -> Result<Vec<u8>, String> {
        let trimmed = mac_str.trim();
        
        // Parse MAC address format: aa:bb:cc:dd:ee:ff or aa-bb-cc-dd-ee-ff
        let hex_parts: Vec<&str> = if trimmed.contains(':') {
            trimmed.split(':').collect()
        } else if trimmed.contains('-') {
            trimmed.split('-').collect()
        } else {
            return Err("Invalid MAC address format".to_string());
        };
        
        if hex_parts.len() != 6 {
            return Err("MAC address must have 6 components".to_string());
        }
        
        let mut result = Vec::with_capacity(6);
        for part in hex_parts {
            let byte = u8::from_str_radix(part, 16)
                .map_err(|_| format!("Invalid MAC address component: {part}"))?;
            result.push(byte);
        }
        
        Ok(result)
    }
    
    /// Encode MACADDR8 value (OID 774)
    /// Binary format: 8 bytes representing the EUI-64 MAC address
    pub fn encode_macaddr8(mac_str: &str) -> Result<Vec<u8>, String> {
        let trimmed = mac_str.trim();
        
        // Parse MAC address format: support both 6-byte and 8-byte formats
        let hex_parts: Vec<&str> = if trimmed.contains(':') {
            trimmed.split(':').collect()
        } else if trimmed.contains('-') {
            trimmed.split('-').collect()
        } else {
            return Err("Invalid MAC address format".to_string());
        };
        
        let mut result = Vec::with_capacity(8);
        
        if hex_parts.len() == 6 {
            // Convert 6-byte MAC to 8-byte EUI-64 format
            // Insert FF:FE between 3rd and 4th bytes
            for (i, part) in hex_parts.iter().enumerate() {
                let byte = u8::from_str_radix(part, 16)
                    .map_err(|_| format!("Invalid MAC address component: {part}"))?;
                result.push(byte);
                
                if i == 2 {
                    // Insert FF:FE after the 3rd byte
                    result.push(0xFF);
                    result.push(0xFE);
                }
            }
        } else if hex_parts.len() == 8 {
            // Already 8-byte format
            for part in hex_parts {
                let byte = u8::from_str_radix(part, 16)
                    .map_err(|_| format!("Invalid MAC address component: {part}"))?;
                result.push(byte);
            }
        } else {
            return Err("MAC address must have 6 or 8 components".to_string());
        }
        
        Ok(result)
    }
    
    /// Parse IPv4 address string to 4-byte array
    fn parse_ipv4(addr_str: &str) -> Result<[u8; 4], String> {
        let parts: Vec<&str> = addr_str.split('.').collect();
        if parts.len() != 4 {
            return Err("IPv4 address must have 4 octets".to_string());
        }
        
        let mut octets = [0u8; 4];
        for (i, part) in parts.iter().enumerate() {
            let octet = part.parse::<u8>()
                .map_err(|_| format!("Invalid IPv4 octet: {part}"))?;
            octets[i] = octet;
        }
        
        Ok(octets)
    }
    
    /// Parse IPv6 address string to 16-byte array
    fn parse_ipv6(addr_str: &str) -> Result<[u8; 16], String> {
        // Simple IPv6 parsing - for production, consider using a proper library
        let addr_str = addr_str.trim();
        
        // Handle special cases
        if addr_str == "::" {
            return Ok([0u8; 16]);
        }
        
        // Handle IPv6 address expansion for "::" compression
        let (left, right) = if let Some(pos) = addr_str.find("::") {
            let left_part = &addr_str[..pos];
            let right_part = &addr_str[pos + 2..];
            (left_part, right_part)
        } else {
            (addr_str, "")
        };
        
        let mut result = [0u8; 16];
        let mut pos = 0;
        
        // Parse left part
        if !left.is_empty() {
            for group in left.split(':') {
                if group.is_empty() {
                    continue;
                }
                let value = u16::from_str_radix(group, 16)
                    .map_err(|_| format!("Invalid IPv6 group: {group}"))?;
                result[pos] = (value >> 8) as u8;
                result[pos + 1] = (value & 0xFF) as u8;
                pos += 2;
            }
        }
        
        // Parse right part (if any)
        if !right.is_empty() {
            let mut right_groups = Vec::new();
            for group in right.split(':') {
                if group.is_empty() {
                    continue;
                }
                let value = u16::from_str_radix(group, 16)
                    .map_err(|_| format!("Invalid IPv6 group: {group}"))?;
                right_groups.push(value);
            }
            
            // Place right groups at the end
            let mut right_pos = 16 - (right_groups.len() * 2);
            for value in right_groups {
                result[right_pos] = (value >> 8) as u8;
                result[right_pos + 1] = (value & 0xFF) as u8;
                right_pos += 2;
            }
        }
        
        Ok(result)
    }

    /// Encode a value based on its PostgreSQL type OID
    pub fn encode_value(value: &rusqlite::types::Value, type_oid: i32, binary_format: bool) -> Option<Vec<u8>> {
        if !binary_format {
            // Text format - use existing converters
            return None;
        }

        // Handle NULL values
        if matches!(value, rusqlite::types::Value::Null) {
            return Some(vec![]);
        }

        // Binary format encoding based on type OID
        match type_oid {
            t if t == PgType::Bool.to_oid() => {
                // BOOL
                match value {
                    rusqlite::types::Value::Integer(i) => Some(Self::encode_bool(*i != 0)),
                    _ => None,
                }
            }
            t if t == PgType::Int2.to_oid() => {
                // INT2
                match value {
                    rusqlite::types::Value::Integer(i) => {
                        if let Ok(v) = (*i).try_into() {
                            Some(Self::encode_int2(v))
                        } else {
                            None
                        }
                    }
                    _ => None,
                }
            }
            t if t == PgType::Int4.to_oid() => {
                // INT4
                match value {
                    rusqlite::types::Value::Integer(i) => {
                        if let Ok(v) = (*i).try_into() {
                            Some(Self::encode_int4(v))
                        } else {
                            None
                        }
                    }
                    _ => None,
                }
            }
            t if t == PgType::Int8.to_oid() => {
                // INT8
                match value {
                    rusqlite::types::Value::Integer(i) => Some(Self::encode_int8(*i)),
                    _ => None,
                }
            }
            t if t == PgType::Float4.to_oid() => {
                // FLOAT4
                match value {
                    rusqlite::types::Value::Real(f) => Some(Self::encode_float4(*f as f32)),
                    rusqlite::types::Value::Integer(i) => Some(Self::encode_float4(*i as f32)),
                    _ => None,
                }
            }
            t if t == PgType::Float8.to_oid() => {
                // FLOAT8
                match value {
                    rusqlite::types::Value::Real(f) => Some(Self::encode_float8(*f)),
                    rusqlite::types::Value::Integer(i) => Some(Self::encode_float8(*i as f64)),
                    _ => None,
                }
            }
            t if t == PgType::Bytea.to_oid() => {
                // BYTEA
                match value {
                    rusqlite::types::Value::Blob(b) => Some(Self::encode_bytea(b)),
                    _ => None,
                }
            }
            t if t == PgType::Text.to_oid() || t == PgType::Varchar.to_oid() => {
                // TEXT, VARCHAR - binary format is the same as text
                match value {
                    rusqlite::types::Value::Text(s) => Some(Self::encode_text(s)),
                    _ => None,
                }
            }
            t if t == PgType::Date.to_oid() => {
                // DATE - stored as INTEGER days since epoch (1970-01-01)
                match value {
                    rusqlite::types::Value::Real(f) => Some(Self::encode_date(*f)),
                    rusqlite::types::Value::Integer(i) => {
                        // Convert days since 1970-01-01 to PostgreSQL days since 2000-01-01
                        let days_since_1970 = *i as i32;
                        let days_since_2000 = days_since_1970 - 10957; // 10957 days between 1970-01-01 and 2000-01-01
                        Some(days_since_2000.to_be_bytes().to_vec())
                    },
                    _ => None,
                }
            }
            t if t == PgType::Time.to_oid() || t == PgType::Timetz.to_oid() => {
                // TIME/TIMETZ - stored as microseconds since midnight
                match value {
                    rusqlite::types::Value::Real(f) => Some(Self::encode_time(*f)),
                    rusqlite::types::Value::Integer(i) => Some(Self::encode_time(*i as f64)),
                    _ => None,
                }
            }
            t if t == PgType::Timestamp.to_oid() || t == PgType::Timestamptz.to_oid() => {
                // TIMESTAMP/TIMESTAMPTZ - stored as microseconds since Unix epoch
                match value {
                    rusqlite::types::Value::Real(f) => Some(Self::encode_timestamp(*f)),
                    rusqlite::types::Value::Integer(i) => Some(Self::encode_timestamp(*i as f64)),
                    _ => None,
                }
            }
            t if t == PgType::Interval.to_oid() => {
                // INTERVAL - stored as total seconds
                match value {
                    rusqlite::types::Value::Real(f) => Some(Self::encode_interval(*f)),
                    rusqlite::types::Value::Integer(i) => Some(Self::encode_interval(*i as f64)),
                    _ => None,
                }
            }
            t if t == PgType::Numeric.to_oid() => {
                // NUMERIC/DECIMAL - use proper binary encoding
                match value {
                    rusqlite::types::Value::Text(s) => {
                        // Parse and encode as PostgreSQL numeric
                        match Decimal::from_str(s) {
                            Ok(decimal) => Some(Self::encode_numeric(&decimal)),
                            Err(_) => None,
                        }
                    }
                    rusqlite::types::Value::Real(f) => {
                        // Convert float to decimal (may lose precision)
                        Decimal::from_f64_retain(*f).map(|decimal| Self::encode_numeric(&decimal))
                    }
                    rusqlite::types::Value::Integer(i) => {
                        // Convert integer to decimal
                        Decimal::from_i64(*i).map(|decimal| Self::encode_numeric(&decimal))
                    }
                    _ => None,
                }
            }
            t if t == PgType::Uuid.to_oid() => {
                // UUID - 16 bytes binary
                match value {
                    rusqlite::types::Value::Text(s) => {
                        Self::encode_uuid(s).ok()
                    }
                    _ => None,
                }
            }
            t if t == PgType::Json.to_oid() => {
                // JSON - same as text in binary format
                match value {
                    rusqlite::types::Value::Text(s) => Some(Self::encode_json(s)),
                    _ => None,
                }
            }
            t if t == PgType::Jsonb.to_oid() => {
                // JSONB - with version header
                match value {
                    rusqlite::types::Value::Text(s) => Some(Self::encode_jsonb(s)),
                    _ => None,
                }
            }
            t if t == PgType::Money.to_oid() => {
                // MONEY - 8-byte integer
                match value {
                    rusqlite::types::Value::Text(s) => {
                        Self::encode_money(s).ok()
                    }
                    _ => None,
                }
            }
            // Array types
            t if t == PgType::Int4Array.to_oid() => {
                // INT4 array
                match value {
                    rusqlite::types::Value::Text(s) => {
                        Self::encode_array(s, PgType::Int4.to_oid()).ok()
                    }
                    _ => None,
                }
            }
            t if t == PgType::Int8Array.to_oid() => {
                // INT8 array
                match value {
                    rusqlite::types::Value::Text(s) => {
                        Self::encode_array(s, PgType::Int8.to_oid()).ok()
                    }
                    _ => None,
                }
            }
            t if t == PgType::TextArray.to_oid() => {
                // TEXT array
                match value {
                    rusqlite::types::Value::Text(s) => {
                        Self::encode_array(s, PgType::Text.to_oid()).ok()
                    }
                    _ => None,
                }
            }
            t if t == PgType::Float8Array.to_oid() => {
                // FLOAT8 array
                match value {
                    rusqlite::types::Value::Text(s) => {
                        Self::encode_array(s, PgType::Float8.to_oid()).ok()
                    }
                    _ => None,
                }
            }
            t if t == PgType::BoolArray.to_oid() => {
                // BOOL array
                match value {
                    rusqlite::types::Value::Text(s) => {
                        Self::encode_array(s, PgType::Bool.to_oid()).ok()
                    }
                    _ => None,
                }
            }
            // Range types
            t if t == PgType::Int4range.to_oid() => {
                // INT4RANGE
                match value {
                    rusqlite::types::Value::Text(s) => {
                        Self::encode_int4range(s).ok()
                    }
                    _ => None,
                }
            }
            t if t == PgType::Int8range.to_oid() => {
                // INT8RANGE
                match value {
                    rusqlite::types::Value::Text(s) => {
                        Self::encode_int8range(s).ok()
                    }
                    _ => None,
                }
            }
            t if t == PgType::Numrange.to_oid() => {
                // NUMRANGE
                match value {
                    rusqlite::types::Value::Text(s) => {
                        Self::encode_numrange(s).ok()
                    }
                    _ => None,
                }
            }
            // Network types
            t if t == PgType::Cidr.to_oid() => {
                // CIDR
                match value {
                    rusqlite::types::Value::Text(s) => {
                        Self::encode_cidr(s).ok()
                    }
                    _ => None,
                }
            }
            t if t == PgType::Inet.to_oid() => {
                // INET
                match value {
                    rusqlite::types::Value::Text(s) => {
                        Self::encode_inet(s).ok()
                    }
                    _ => None,
                }
            }
            t if t == PgType::Macaddr.to_oid() => {
                // MACADDR
                match value {
                    rusqlite::types::Value::Text(s) => {
                        Self::encode_macaddr(s).ok()
                    }
                    _ => None,
                }
            }
            t if t == PgType::Macaddr8.to_oid() => {
                // MACADDR8
                match value {
                    rusqlite::types::Value::Text(s) => {
                        Self::encode_macaddr8(s).ok()
                    }
                    _ => None,
                }
            }
            _ => {
                // For other types, fall back to text format
                None
            }
        }
    }
}

/// Zero-copy binary format encoder using BytesMut
pub struct ZeroCopyBinaryEncoder<'a> {
    buffer: &'a mut BytesMut,
}

impl<'a> ZeroCopyBinaryEncoder<'a> {
    pub fn new(buffer: &'a mut BytesMut) -> Self {
        Self { buffer }
    }

    /// Encode a boolean value directly into buffer
    #[inline]
    pub fn encode_bool(&mut self, value: bool) -> usize {
        let start = self.buffer.len();
        self.buffer.put_u8(if value { 1 } else { 0 });
        start
    }

    /// Encode an int2 value directly into buffer
    #[inline]
    pub fn encode_int2(&mut self, value: i16) -> usize {
        let start = self.buffer.len();
        self.buffer.put_i16(value);
        start
    }

    /// Encode an int4 value directly into buffer
    #[inline]
    pub fn encode_int4(&mut self, value: i32) -> usize {
        let start = self.buffer.len();
        self.buffer.put_i32(value);
        start
    }

    /// Encode an int8 value directly into buffer
    #[inline]
    pub fn encode_int8(&mut self, value: i64) -> usize {
        let start = self.buffer.len();
        self.buffer.put_i64(value);
        start
    }

    /// Encode a float4 value directly into buffer
    #[inline]
    pub fn encode_float4(&mut self, value: f32) -> usize {
        let start = self.buffer.len();
        self.buffer.put_f32(value);
        start
    }

    /// Encode a float8 value directly into buffer
    #[inline]
    pub fn encode_float8(&mut self, value: f64) -> usize {
        let start = self.buffer.len();
        self.buffer.put_f64(value);
        start
    }

    /// Encode text value directly into buffer
    #[inline]
    pub fn encode_text(&mut self, value: &str) -> usize {
        let start = self.buffer.len();
        self.buffer.put_slice(value.as_bytes());
        start
    }

    /// Encode bytea value directly into buffer
    #[inline]
    pub fn encode_bytea(&mut self, value: &[u8]) -> usize {
        let start = self.buffer.len();
        self.buffer.put_slice(value);
        start
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_binary_bool() {
        assert_eq!(BinaryEncoder::encode_bool(true), vec![1]);
        assert_eq!(BinaryEncoder::encode_bool(false), vec![0]);
    }

    #[test]
    fn test_binary_integers() {
        assert_eq!(BinaryEncoder::encode_int2(42), vec![0, 42]);
        assert_eq!(BinaryEncoder::encode_int4(0x01020304), vec![1, 2, 3, 4]);
        assert_eq!(
            BinaryEncoder::encode_int8(0x0102030405060708),
            vec![1, 2, 3, 4, 5, 6, 7, 8]
        );
    }

    #[test]
    fn test_binary_floats() {
        let f4_bytes = BinaryEncoder::encode_float4(1.5);
        assert_eq!(f4_bytes.len(), 4);
        
        let f8_bytes = BinaryEncoder::encode_float8(1.5);
        assert_eq!(f8_bytes.len(), 8);
    }

    #[test]
    fn test_zero_copy_encoder() {
        let mut buffer = BytesMut::with_capacity(1024);
        let mut encoder = ZeroCopyBinaryEncoder::new(&mut buffer);

        let pos1 = encoder.encode_bool(true);
        let pos2 = encoder.encode_int4(42);
        let pos3 = encoder.encode_text("hello");

        assert_eq!(&buffer[pos1..pos1 + 1], &[1]);
        assert_eq!(&buffer[pos2..pos2 + 4], &[0, 0, 0, 42]);
        assert_eq!(&buffer[pos3..pos3 + 5], b"hello");
    }
    
    #[test]
    fn test_date_encoding() {
        // Test DATE encoding
        // 2024-01-15 00:00:00 UTC = 1705276800 Unix timestamp
        let encoded = BinaryEncoder::encode_date(1705276800.0);
        // Days since 2000-01-01: (1705276800 - 946684800) / 86400 = 8780
        let expected: i32 = 8780;
        assert_eq!(encoded, expected.to_be_bytes().to_vec());
    }
    
    #[test]
    fn test_time_encoding() {
        // Test TIME encoding
        // 14:30:45.123456 = 52245123456 microseconds since midnight
        let encoded = BinaryEncoder::encode_time(52245123456.0);
        // Microseconds: 52245123456
        let expected: i64 = 52245123456;
        assert_eq!(encoded, expected.to_be_bytes().to_vec());
    }
    
    #[test]
    fn test_timestamp_encoding() {
        // Test TIMESTAMP encoding
        // 2024-01-15 14:30:45.123456 UTC = 1705329045123456 microseconds since Unix epoch
        let encoded = BinaryEncoder::encode_timestamp(1705329045123456.0);
        // Microseconds since 2000-01-01: 1705329045123456 - 946684800000000
        let expected: i64 = 758644245123456;
        assert_eq!(encoded, expected.to_be_bytes().to_vec());
    }
    
    #[test]
    fn test_interval_encoding() {
        // Test INTERVAL encoding
        // 1 day 2:30:00 = 95400 seconds
        let encoded = BinaryEncoder::encode_interval(95400.0);
        assert_eq!(encoded.len(), 16); // 8 bytes microseconds + 4 bytes days + 4 bytes months
        
        // Check microseconds part
        let micros = i64::from_be_bytes(encoded[0..8].try_into().unwrap());
        assert_eq!(micros, 95400000000); // 95400 * 1_000_000
        
        // Check days and months (should be 0)
        let days = i32::from_be_bytes(encoded[8..12].try_into().unwrap());
        let months = i32::from_be_bytes(encoded[12..16].try_into().unwrap());
        assert_eq!(days, 0);
        assert_eq!(months, 0);
    }
    
    #[test]
    fn test_uuid_encoding() {
        // Test UUID encoding
        let uuid_str = "550e8400-e29b-41d4-a716-446655440000";
        let encoded = BinaryEncoder::encode_uuid(uuid_str).unwrap();
        assert_eq!(encoded.len(), 16);
        
        // Verify first few bytes
        assert_eq!(encoded[0], 0x55);
        assert_eq!(encoded[1], 0x0e);
        assert_eq!(encoded[2], 0x84);
        assert_eq!(encoded[3], 0x00);
    }
    
    #[test]
    fn test_json_jsonb_encoding() {
        let json_str = r#"{"key": "value"}"#;
        
        // JSON encoding - same as text
        let json_encoded = BinaryEncoder::encode_json(json_str);
        assert_eq!(json_encoded, json_str.as_bytes());
        
        // JSONB encoding - with version header
        let jsonb_encoded = BinaryEncoder::encode_jsonb(json_str);
        assert_eq!(jsonb_encoded[0], 1); // version
        assert_eq!(&jsonb_encoded[1..], json_str.as_bytes());
    }
    
    #[test]
    fn test_money_encoding() {
        // Test various money formats
        let encoded1 = BinaryEncoder::encode_money("123.45").unwrap();
        let money1 = i64::from_be_bytes(encoded1.try_into().unwrap());
        assert_eq!(money1, 12345); // $123.45 = 12345 cents
        
        let encoded2 = BinaryEncoder::encode_money("$1,234.56").unwrap();
        let money2 = i64::from_be_bytes(encoded2.try_into().unwrap());
        assert_eq!(money2, 123456); // $1,234.56 = 123456 cents
        
        let encoded3 = BinaryEncoder::encode_money("-99.99").unwrap();
        let money3 = i64::from_be_bytes(encoded3.try_into().unwrap());
        assert_eq!(money3, -9999); // -$99.99 = -9999 cents
    }
    
    #[test]
    fn test_numeric_encoding() {
        // Test is already covered by decimal_handler tests
        // Just verify the function is accessible
        let decimal = Decimal::from_str("123.45").unwrap();
        let encoded = BinaryEncoder::encode_numeric(&decimal);
        assert!(!encoded.is_empty());
    }
    
    #[test]
    fn test_array_encoding() {
        // Test empty array
        let empty = BinaryEncoder::encode_array("[]", PgType::Int4.to_oid()).unwrap();
        assert_eq!(empty.len(), 12); // 3 * 4 bytes for header
        assert_eq!(&empty[0..4], &0i32.to_be_bytes()); // ndim = 0
        
        // Test simple int array
        let int_array = BinaryEncoder::encode_array("[1, 2, 3]", PgType::Int4.to_oid()).unwrap();
        // Verify header
        assert_eq!(i32::from_be_bytes(int_array[0..4].try_into().unwrap()), 1); // ndim = 1
        assert_eq!(i32::from_be_bytes(int_array[4..8].try_into().unwrap()), 0); // no nulls
        assert_eq!(i32::from_be_bytes(int_array[8..12].try_into().unwrap()), PgType::Int4.to_oid()); // elemtype
        assert_eq!(i32::from_be_bytes(int_array[12..16].try_into().unwrap()), 3); // dim size
        assert_eq!(i32::from_be_bytes(int_array[16..20].try_into().unwrap()), 1); // lower bound
        
        // Test array with nulls
        let null_array = BinaryEncoder::encode_array("[1, null, 3]", PgType::Int4.to_oid()).unwrap();
        assert_eq!(i32::from_be_bytes(null_array[0..4].try_into().unwrap()), 1); // ndim = 1
        assert!(i32::from_be_bytes(null_array[4..8].try_into().unwrap()) > 0); // has nulls
        
        // Test text array
        let text_array = BinaryEncoder::encode_array(r#"["hello", "world"]"#, PgType::Text.to_oid()).unwrap();
        assert_eq!(i32::from_be_bytes(text_array[8..12].try_into().unwrap()), PgType::Text.to_oid());
        
        // Test bool array
        let bool_array = BinaryEncoder::encode_array("[true, false, true]", PgType::Bool.to_oid()).unwrap();
        assert_eq!(i32::from_be_bytes(bool_array[8..12].try_into().unwrap()), PgType::Bool.to_oid());
    }
    
    #[test]
    fn test_range_encoding() {
        // Test INT4RANGE
        let empty_range = BinaryEncoder::encode_int4range("empty").unwrap();
        assert_eq!(empty_range, vec![0x01]); // RANGE_EMPTY
        
        let inclusive_range = BinaryEncoder::encode_int4range("[1,10]").unwrap();
        assert_eq!(inclusive_range[0], 0x06); // LB_INC | UB_INC
        
        let exclusive_range = BinaryEncoder::encode_int4range("(1,10)").unwrap();
        assert_eq!(exclusive_range[0], 0x00); // neither inclusive
        
        let half_open = BinaryEncoder::encode_int4range("[1,10)").unwrap();
        assert_eq!(half_open[0], 0x02); // LB_INC only
        
        // Test INT8RANGE
        let int8_range = BinaryEncoder::encode_int8range("[1000000000000,2000000000000]").unwrap();
        assert_eq!(int8_range[0], 0x06); // LB_INC | UB_INC
        
        // Test NUMRANGE
        let num_range = BinaryEncoder::encode_numrange("[1.5,3.14]").unwrap();
        assert_eq!(num_range[0], 0x06); // LB_INC | UB_INC
        
        // Test infinite bounds
        let infinite_lower = BinaryEncoder::encode_int4range("(,100]").unwrap();
        assert_eq!(infinite_lower[0], 0x0C); // UB_INC | LB_INF
        
        let infinite_upper = BinaryEncoder::encode_int4range("[0,)").unwrap();
        assert_eq!(infinite_upper[0], 0x12); // LB_INC | UB_INF
        
        let infinite_both = BinaryEncoder::encode_int4range("(,)").unwrap();
        assert_eq!(infinite_both[0], 0x18); // LB_INF | UB_INF
    }
    
    #[test]
    fn test_network_encoding() {
        // Test IPv4 CIDR
        let ipv4_cidr = BinaryEncoder::encode_cidr("192.168.1.0/24").unwrap();
        assert_eq!(ipv4_cidr[0], 1); // AF_INET
        assert_eq!(ipv4_cidr[1], 24); // prefix length
        assert_eq!(ipv4_cidr[2], 1); // is_cidr = true
        assert_eq!(ipv4_cidr[3], 4); // address length
        assert_eq!(&ipv4_cidr[4..8], &[192, 168, 1, 0]); // address bytes
        
        // Test IPv4 INET
        let ipv4_inet = BinaryEncoder::encode_inet("192.168.1.1").unwrap();
        assert_eq!(ipv4_inet[0], 1); // AF_INET
        assert_eq!(ipv4_inet[1], 32); // default prefix for IPv4
        assert_eq!(ipv4_inet[2], 0); // is_cidr = false
        assert_eq!(ipv4_inet[3], 4); // address length
        assert_eq!(&ipv4_inet[4..8], &[192, 168, 1, 1]); // address bytes
        
        // Test IPv6 CIDR
        let ipv6_cidr = BinaryEncoder::encode_cidr("2001:db8::/32").unwrap();
        assert_eq!(ipv6_cidr[0], 2); // AF_INET6
        assert_eq!(ipv6_cidr[1], 32); // prefix length
        assert_eq!(ipv6_cidr[2], 1); // is_cidr = true
        assert_eq!(ipv6_cidr[3], 16); // address length
        assert_eq!(&ipv6_cidr[4..8], &[0x20, 0x01, 0x0d, 0xb8]); // first 4 bytes
        
        // Test IPv6 INET
        let ipv6_inet = BinaryEncoder::encode_inet("::1").unwrap();
        assert_eq!(ipv6_inet[0], 2); // AF_INET6
        assert_eq!(ipv6_inet[1], 128); // default prefix for IPv6
        assert_eq!(ipv6_inet[2], 0); // is_cidr = false
        assert_eq!(ipv6_inet[3], 16); // address length
        // Last two bytes should be [0, 1] for ::1
        assert_eq!(&ipv6_inet[18..20], &[0, 1]);
    }
    
    #[test]
    fn test_macaddr_encoding() {
        // Test MACADDR (6 bytes)
        let mac6 = BinaryEncoder::encode_macaddr("08:00:2b:01:02:03").unwrap();
        assert_eq!(mac6.len(), 6);
        assert_eq!(mac6, vec![0x08, 0x00, 0x2b, 0x01, 0x02, 0x03]);
        
        // Test MACADDR with dashes
        let mac6_dash = BinaryEncoder::encode_macaddr("aa-bb-cc-dd-ee-ff").unwrap();
        assert_eq!(mac6_dash.len(), 6);
        assert_eq!(mac6_dash, vec![0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff]);
        
        // Test MACADDR8 with 6-byte input (should be converted to 8-byte EUI-64)
        let mac8_from6 = BinaryEncoder::encode_macaddr8("08:00:2b:01:02:03").unwrap();
        assert_eq!(mac8_from6.len(), 8);
        assert_eq!(mac8_from6, vec![0x08, 0x00, 0x2b, 0xff, 0xfe, 0x01, 0x02, 0x03]);
        
        // Test MACADDR8 with 8-byte input
        let mac8_full = BinaryEncoder::encode_macaddr8("08:00:2b:01:02:03:04:05").unwrap();
        assert_eq!(mac8_full.len(), 8);
        assert_eq!(mac8_full, vec![0x08, 0x00, 0x2b, 0x01, 0x02, 0x03, 0x04, 0x05]);
    }
    
    #[test]
    fn test_ipv4_parsing() {
        let addr = BinaryEncoder::parse_ipv4("127.0.0.1").unwrap();
        assert_eq!(addr, [127, 0, 0, 1]);
        
        let addr2 = BinaryEncoder::parse_ipv4("255.255.255.255").unwrap();
        assert_eq!(addr2, [255, 255, 255, 255]);
        
        // Test error cases
        assert!(BinaryEncoder::parse_ipv4("256.0.0.1").is_err()); // Invalid octet
        assert!(BinaryEncoder::parse_ipv4("1.2.3").is_err()); // Too few octets
        assert!(BinaryEncoder::parse_ipv4("1.2.3.4.5").is_err()); // Too many octets
    }
    
    #[test]
    fn test_ipv6_parsing() {
        // Test simple cases
        let addr = BinaryEncoder::parse_ipv6("::").unwrap();
        assert_eq!(addr, [0u8; 16]);
        
        let addr2 = BinaryEncoder::parse_ipv6("::1").unwrap();
        let mut expected = [0u8; 16];
        expected[15] = 1;
        assert_eq!(addr2, expected);
        
        // Test 2001:db8::
        let addr3 = BinaryEncoder::parse_ipv6("2001:db8::").unwrap();
        let mut expected3 = [0u8; 16];
        expected3[0] = 0x20;
        expected3[1] = 0x01;
        expected3[2] = 0x0d;
        expected3[3] = 0xb8;
        assert_eq!(addr3, expected3);
    }
}