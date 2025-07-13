// Test to reproduce the JSON path parameter issue
use regex::Regex;

fn main() {
    // This reproduces the current broken behavior
    let re = Regex::new(r"(\b\w+(?:\.\w+)?)\s*->>\s*'([^']+)'").unwrap();
    let sql = "SELECT data->>'name' FROM users WHERE config->>'$.items[0]' = 'value'";
    
    // This will incorrectly handle the $$ in the replacement
    let result = re.replace_all(sql, r"json_extract($1, '$$.$2')");
    println!("Broken result: {}", result);
    
    // The correct replacement should escape the dollar sign properly
    let correct_result = re.replace_all(sql, r"json_extract($1, '$$$.$2')");
    println!("Fixed result: {}", correct_result);
    
    // Test with a simple case
    let simple_sql = "SELECT data->>'name' FROM users";
    let simple_result = re.replace_all(simple_sql, r"json_extract($1, '$$.$2')");
    println!("Simple broken result: {}", simple_result);
    
    let simple_correct = re.replace_all(simple_sql, r"json_extract($1, '$$$.$2')");
    println!("Simple fixed result: {}", simple_correct);
}