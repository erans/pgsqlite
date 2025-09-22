/// Optimized query type detection utilities to avoid expensive to_uppercase() calls
pub struct QueryTypeDetector;

impl QueryTypeDetector {
    /// Detect query type with optimized byte comparison
    #[inline]
    pub fn detect_query_type(query: &str) -> QueryType {
        let trimmed = query.trim();
        let bytes = trimmed.as_bytes();
        
        // Check for WITH (CTE) queries first - these are SELECT queries
        if bytes.len() >= 4 {
            match &bytes[0..4] {
                b"WITH" | b"with" | b"With" => return QueryType::Select,
                _ => {}
            }
        }
        
        if bytes.len() >= 6 {
            match &bytes[0..6] {
                b"SELECT" | b"select" | b"Select" => return QueryType::Select,
                b"INSERT" | b"insert" | b"Insert" => return QueryType::Insert,
                b"UPDATE" | b"update" | b"Update" => return QueryType::Update,
                b"DELETE" | b"delete" | b"Delete" => return QueryType::Delete,
                b"CREATE" | b"create" | b"Create" => return QueryType::Create,
                _ => {}
            }
        }
        
        if bytes.len() >= 4 {
            match &bytes[0..4] {
                b"DROP" | b"drop" | b"Drop" => return QueryType::Drop,
                _ => {}
            }
        }
        
        if bytes.len() >= 5 {
            match &bytes[0..5] {
                b"ALTER" | b"alter" | b"Alter" => return QueryType::Alter,
                b"BEGIN" | b"begin" | b"Begin" => return QueryType::Begin,
                _ => {}
            }
        }
        
        if bytes.len() >= 6 {
            match &bytes[0..6] {
                b"COMMIT" | b"commit" | b"Commit" => return QueryType::Commit,
                _ => {}
            }
        }
        
        if bytes.len() >= 7 {
            match &bytes[0..7] {
                b"COMMENT" | b"comment" | b"Comment" => return QueryType::Comment,
                _ => {}
            }
        }
        
        if bytes.len() >= 8 {
            match &bytes[0..8] {
                b"ROLLBACK" | b"rollback" | b"Rollback" => return QueryType::Rollback,
                b"TRUNCATE" | b"truncate" | b"Truncate" => return QueryType::Truncate,
                _ => {}
            }
        }
        
        // Fall back to eq_ignore_ascii_case for less common or mixed case patterns
        if (trimmed.len() >= 4 && trimmed[..4].eq_ignore_ascii_case("WITH")) ||
           (trimmed.len() >= 6 && trimmed[..6].eq_ignore_ascii_case("SELECT")) {
            QueryType::Select
        } else if trimmed.len() >= 6 && trimmed[..6].eq_ignore_ascii_case("INSERT") {
            QueryType::Insert
        } else if trimmed.len() >= 6 && trimmed[..6].eq_ignore_ascii_case("UPDATE") {
            QueryType::Update
        } else if trimmed.len() >= 6 && trimmed[..6].eq_ignore_ascii_case("DELETE") {
            QueryType::Delete
        } else if trimmed.len() >= 6 && trimmed[..6].eq_ignore_ascii_case("CREATE") {
            QueryType::Create
        } else if trimmed.len() >= 4 && trimmed[..4].eq_ignore_ascii_case("DROP") {
            QueryType::Drop
        } else if trimmed.len() >= 5 && trimmed[..5].eq_ignore_ascii_case("ALTER") {
            QueryType::Alter
        } else if trimmed.len() >= 8 && trimmed[..8].eq_ignore_ascii_case("TRUNCATE") {
            QueryType::Truncate
        } else if trimmed.len() >= 5 && trimmed[..5].eq_ignore_ascii_case("BEGIN") {
            QueryType::Begin
        } else if trimmed.len() >= 6 && trimmed[..6].eq_ignore_ascii_case("COMMIT") {
            QueryType::Commit
        } else if trimmed.len() >= 8 && trimmed[..8].eq_ignore_ascii_case("ROLLBACK") {
            QueryType::Rollback
        } else if trimmed.len() >= 7 && trimmed[..7].eq_ignore_ascii_case("COMMENT") {
            QueryType::Comment
        } else {
            QueryType::Other
        }
    }
    
    /// Check if query is DDL with optimized comparison
    #[inline]
    pub fn is_ddl(query: &str) -> bool {
        let trimmed = query.trim();
        let bytes = trimmed.as_bytes();
        
        if bytes.len() >= 6 {
            match &bytes[0..6] {
                b"CREATE" | b"create" | b"Create" => return true,
                b"ALTER " | b"alter " | b"Alter " => return true,
                _ => {}
            }
        }
        
        if bytes.len() >= 4 {
            match &bytes[0..4] {
                b"DROP" | b"drop" | b"Drop" => return true,
                _ => {}
            }
        }
        
        if bytes.len() >= 7 {
            match &bytes[0..7] {
                b"COMMENT" | b"comment" | b"Comment" => return true,
                _ => {}
            }
        }
        
        if bytes.len() >= 8 {
            match &bytes[0..8] {
                b"TRUNCATE" | b"truncate" | b"Truncate" => return true,
                _ => {}
            }
        }
        
        // Fallback for mixed case
        (trimmed.len() >= 6 && trimmed[..6].eq_ignore_ascii_case("CREATE")) ||
        (trimmed.len() >= 4 && trimmed[..4].eq_ignore_ascii_case("DROP")) ||
        (trimmed.len() >= 5 && trimmed[..5].eq_ignore_ascii_case("ALTER")) ||
        (trimmed.len() >= 7 && trimmed[..7].eq_ignore_ascii_case("COMMENT")) ||
        (trimmed.len() >= 8 && trimmed[..8].eq_ignore_ascii_case("TRUNCATE"))
    }
    
    /// Check if query is DML with optimized comparison
    #[inline]
    pub fn is_dml(query: &str) -> bool {
        matches!(
            Self::detect_query_type(query),
            QueryType::Insert | QueryType::Update | QueryType::Delete
        )
    }
    
    /// Check if query is transaction control
    #[inline]
    pub fn is_transaction(query: &str) -> bool {
        matches!(
            Self::detect_query_type(query),
            QueryType::Begin | QueryType::Commit | QueryType::Rollback
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryType {
    Select,
    Insert,
    Update,
    Delete,
    Create,
    Drop,
    Alter,
    Truncate,
    Begin,
    Commit,
    Rollback,
    Comment,
    Other,
}

impl QueryType {
    pub fn starts_with_keyword(&self) -> &'static str {
        match self {
            QueryType::Select => "SELECT",
            QueryType::Insert => "INSERT",
            QueryType::Update => "UPDATE",
            QueryType::Delete => "DELETE",
            QueryType::Create => "CREATE",
            QueryType::Drop => "DROP",
            QueryType::Alter => "ALTER",
            QueryType::Truncate => "TRUNCATE",
            QueryType::Begin => "BEGIN",
            QueryType::Commit => "COMMIT",
            QueryType::Rollback => "ROLLBACK",
            QueryType::Comment => "COMMENT",
            QueryType::Other => "",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_query_type_detection() {
        assert_eq!(QueryTypeDetector::detect_query_type("SELECT * FROM users"), QueryType::Select);
        assert_eq!(QueryTypeDetector::detect_query_type("select * from users"), QueryType::Select);
        assert_eq!(QueryTypeDetector::detect_query_type("Select * From users"), QueryType::Select);
        assert_eq!(QueryTypeDetector::detect_query_type("SeLeCt * from users"), QueryType::Select);
        
        assert_eq!(QueryTypeDetector::detect_query_type("INSERT INTO table VALUES (1)"), QueryType::Insert);
        assert_eq!(QueryTypeDetector::detect_query_type("insert into table values (1)"), QueryType::Insert);
        
        assert_eq!(QueryTypeDetector::detect_query_type("UPDATE table SET x = 1"), QueryType::Update);
        assert_eq!(QueryTypeDetector::detect_query_type("update table set x = 1"), QueryType::Update);
        
        assert_eq!(QueryTypeDetector::detect_query_type("DELETE FROM table"), QueryType::Delete);
        assert_eq!(QueryTypeDetector::detect_query_type("delete from table"), QueryType::Delete);
        
        assert_eq!(QueryTypeDetector::detect_query_type("CREATE TABLE test"), QueryType::Create);
        assert_eq!(QueryTypeDetector::detect_query_type("create table test"), QueryType::Create);
        
        assert_eq!(QueryTypeDetector::detect_query_type("DROP TABLE test"), QueryType::Drop);
        assert_eq!(QueryTypeDetector::detect_query_type("drop table test"), QueryType::Drop);
        
        assert_eq!(QueryTypeDetector::detect_query_type("ALTER TABLE test"), QueryType::Alter);
        assert_eq!(QueryTypeDetector::detect_query_type("alter table test"), QueryType::Alter);
        
        assert_eq!(QueryTypeDetector::detect_query_type("BEGIN TRANSACTION"), QueryType::Begin);
        assert_eq!(QueryTypeDetector::detect_query_type("begin transaction"), QueryType::Begin);
        
        assert_eq!(QueryTypeDetector::detect_query_type("COMMIT"), QueryType::Commit);
        assert_eq!(QueryTypeDetector::detect_query_type("commit"), QueryType::Commit);
        
        assert_eq!(QueryTypeDetector::detect_query_type("ROLLBACK"), QueryType::Rollback);
        assert_eq!(QueryTypeDetector::detect_query_type("rollback"), QueryType::Rollback);
        
        assert_eq!(QueryTypeDetector::detect_query_type("EXPLAIN SELECT * FROM test"), QueryType::Other);
        assert_eq!(QueryTypeDetector::detect_query_type("   SELECT * FROM test"), QueryType::Select);
        
        // Test CTE (WITH) queries
        assert_eq!(QueryTypeDetector::detect_query_type("WITH cte AS (SELECT 1) SELECT * FROM cte"), QueryType::Select);
        assert_eq!(QueryTypeDetector::detect_query_type("with cte as (select 1) select * from cte"), QueryType::Select);
        assert_eq!(QueryTypeDetector::detect_query_type("With Cte As (Select 1) Select * From Cte"), QueryType::Select);
        assert_eq!(QueryTypeDetector::detect_query_type("WiTh cte AS (SELECT 1) SELECT * FROM cte"), QueryType::Select);
    }
    
    #[test]
    fn test_is_ddl() {
        assert!(QueryTypeDetector::is_ddl("CREATE TABLE test"));
        assert!(QueryTypeDetector::is_ddl("create table test"));
        assert!(QueryTypeDetector::is_ddl("DROP TABLE test"));
        assert!(QueryTypeDetector::is_ddl("drop table test"));
        assert!(QueryTypeDetector::is_ddl("ALTER TABLE test"));
        assert!(QueryTypeDetector::is_ddl("alter table test"));
        assert!(QueryTypeDetector::is_ddl("TRUNCATE TABLE test"));
        assert!(QueryTypeDetector::is_ddl("truncate table test"));
        
        assert!(!QueryTypeDetector::is_ddl("SELECT * FROM test"));
        assert!(!QueryTypeDetector::is_ddl("INSERT INTO test"));
        assert!(!QueryTypeDetector::is_ddl("UPDATE test SET x = 1"));
        assert!(!QueryTypeDetector::is_ddl("DELETE FROM test"));
    }
}