#[cfg(test)]
mod tests {
    use std::time::Instant;

    #[test]
    fn test_number_formatting_performance() {
        const ITERATIONS: usize = 100_000;
        
        // Test integer formatting
        let integers: Vec<i64> = (0..1000).collect();
        
        // Old approach (to_string)
        let start = Instant::now();
        for _ in 0..ITERATIONS/1000 {
            for &i in &integers {
                let _ = i.to_string().as_bytes().to_vec();
            }
        }
        let old_duration = start.elapsed();
        
        // New approach (itoa)
        let start = Instant::now();
        for _ in 0..ITERATIONS/1000 {
            for &i in &integers {
                let mut buf = itoa::Buffer::new();
                let _ = buf.format(i).as_bytes().to_vec();
            }
        }
        let new_duration = start.elapsed();
        
        println!("Integer formatting ({} iterations):", ITERATIONS);
        println!("  Old (to_string): {:?}", old_duration);
        println!("  New (itoa):      {:?}", new_duration);
        println!("  Speedup:         {:.2}x", old_duration.as_secs_f64() / new_duration.as_secs_f64());
        
        // Test float formatting
        let floats: Vec<f64> = (0..1000).map(|i| i as f64 * 1.234567).collect();
        
        // Old approach
        let start = Instant::now();
        for _ in 0..ITERATIONS/1000 {
            for &f in &floats {
                let _ = f.to_string().as_bytes().to_vec();
            }
        }
        let old_duration = start.elapsed();
        
        // New approach (ryu)
        let start = Instant::now();
        for _ in 0..ITERATIONS/1000 {
            for &f in &floats {
                let mut buf = ryu::Buffer::new();
                let _ = buf.format(f).as_bytes().to_vec();
            }
        }
        let new_duration = start.elapsed();
        
        println!("\nFloat formatting ({} iterations):", ITERATIONS);
        println!("  Old (to_string): {:?}", old_duration);
        println!("  New (ryu):       {:?}", new_duration);
        println!("  Speedup:         {:.2}x", old_duration.as_secs_f64() / new_duration.as_secs_f64());
        
        // Verify correctness
        for i in [0, 42, -42, i64::MAX, i64::MIN] {
            let old = i.to_string();
            let mut buf = itoa::Buffer::new();
            let new = buf.format(i);
            assert_eq!(old, new, "Integer formatting mismatch for {}", i);
        }
        
        for f in [0.0, 42.42, -42.42, std::f64::consts::PI] {
            let old = f.to_string();
            let mut buf = ryu::Buffer::new();
            let new = buf.format(f);
            // Float formatting might differ slightly, so just check they parse to same value
            let old_parsed: f64 = old.parse().unwrap();
            let new_parsed: f64 = new.parse().unwrap();
            assert!((old_parsed - new_parsed).abs() < 1e-10, 
                "Float formatting mismatch for {}: {} vs {}", f, old, new);
        }
    }
}