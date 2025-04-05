use rand::prelude::*;
use rand_chacha::ChaCha8Rng;
use std::collections::HashSet;

pub fn generate_complex_lexer_inputs(len : usize) -> Vec<String> {
    let mut rng: ChaCha8Rng = ChaCha8Rng::from_entropy(); // Use ChaCha8Rng seeded from entropy
    let mut inputs: HashSet<String> = HashSet::new();

    // Helper function to safely insert non-empty strings
    fn safe_insert(inputs: &mut HashSet<String>, input: String) {
        if !input.is_empty() {
            inputs.insert(input);
        } else {
            //println!("DEBUG: Attempted to insert an empty string.");
        }
    }

    // Single tokens
    let keywords = ["CREATE", "search", "WHERE", "CONTAINER", "USING", "INT", "BIGINT", "STRING", "BOOLEAN", "FLOAT"];
    let strings = ["'hello'", "\"world\"", "'data1'", "\"field1\"", "'test'", "'value'", "\"example\"", "'item'", "\"record\"", "'entry'", "'info'", "\"detail\"", "'name'", "\"title\"", "'label'", "\"description\"", "'key'", "\"identifier\"", "'code'", "\"number\""]; // 20 total
    let ints = ["0", "1", "-1", "100", "-100", "123", "-456", "999", "-789", "1024", "-2048", "65535", "-32768", "2147483647", "-2147483648", "9223372036854775807", "-9223372036854775808", "5", "-15", "200"]; // 20 total
    let floats = ["0.0", "1.1", "-1.1", "3.14", "-2.718", "1.618", "-0.5", "2.0", "-3.0", "0.123", "-0.456", "10.5", "-20.7", "3.14159", "-2.71828", "6.022e23", "-1.602e-19", "9.8", "-9.8", "42.0"]; // 20 total
    let bools = ["true", "FALSE", "True", "false", "TRUE", "False", "tRuE", "fAlSe", "T", "f"]; // 10 total
    let operators = ["+", "-", "/", "%", "!", ">", "<", "=", ">=", "<=", "==", "!="]; // 12 total

    //println!("DEBUG: Adding single tokens...");
    for item in keywords.iter().chain(strings.iter()).chain(ints.iter()).chain(floats.iter()).chain(bools.iter()).chain(operators[..8].iter()) {
        let input = item.to_string();
        //println!("DEBUG: Single token generated: '{}'", input);
        safe_insert(&mut inputs, input);
    }

    // Simple queries
    let containers = (1..=10).map(|i| format!("data{}", i)).collect::<Vec<String>>();
    let fields = (1..=10).map(|i| format!("field{}", i)).collect::<Vec<String>>();
    let ops = ["==", "!=", ">", "<", ">=", "<="];
    let values = ["0", "1", "100", "0.0", "3.14", "'a'", "'test'", "true", "FALSE", "1.1"];
    let mut simple_query_count = 0;
    //println!("DEBUG: Adding simple queries...");
    for c in &containers {
        for f in &fields {
            for o in ops {
                for v in values {
                    if inputs.len() < 600 {
                        let input = format!("SEARCH CONTAINER '{}' WHERE '{}' {} {}", c, f, o, v);
                        //println!("DEBUG: Simple query generated: '{}'", input);
                        safe_insert(&mut inputs, input);
                        simple_query_count += 1;
                    }
                    if simple_query_count >= 500 {
                        break;
                    }
                }
                if simple_query_count >= 500 {
                    break;
                }
            }
            if simple_query_count >= 500 {
                break;
            }
        }
        if simple_query_count >= 500 {
            break;
        }
    }

    // Multi-condition queries
    let mut multi_query_count = 0;
    //println!("DEBUG: Adding multi-condition queries...");
    for c in &containers {
        for i in 0..5 {
            for j in (i + 1)..5 {
                let f1 = format!("field{}", i + 1);
                let f2 = format!("field{}", j + 1);
                for o1 in ops {
                    for o2 in ops {
                        for k in 0..5 {
                            for l in 5..10 {
                                let v1 = values[k];
                                let v2 = values[l];
                                if inputs.len() < 900 {
                                    let input = format!("SEARCH CONTAINER '{}' WHERE '{}' {} {} AND '{}' {} {}", c, f1, o1, v1, f2, o2, v2);
                                    //println!("DEBUG: Multi-condition query generated: '{}'", input);
                                    safe_insert(&mut inputs, input);
                                    multi_query_count += 1;
                                }
                                if multi_query_count >= 300 {
                                    break;
                                }
                            }
                            if multi_query_count >= 300 {
                                break;
                            }
                        }
                        if multi_query_count >= 300 {
                            break;
                        }
                    }
                    if multi_query_count >= 300 {
                        break;
                    }
                }
                if multi_query_count >= 300 {
                    break;
                }
            }
            if multi_query_count >= 300 {
                break;
            }
        }
        if multi_query_count >= 300 {
            break;
        }
    }

    // Groups
    let group_values = ["1, 2, 3", "'a', 'b', 'c'", "true, FALSE, true", "1.1, 2.2, 3.3", "1, 'x', true"];
    //println!("DEBUG: Adding group queries...");
    for i in 0..20 {
        let gv = group_values[i % group_values.len()];
        if inputs.len() < 1000 {
            let input = if i % 2 == 0 {
                format!("CREATE USING [{}]", gv)
            } else {
                format!("SEARCH WHERE 'field{}' == [{}]", (i % 10) + 1, gv)
            };
            //println!("DEBUG: Group query generated: '{}'", input);
            safe_insert(&mut inputs, input);
        }
    }

    // More complex nested conditions
    //println!("DEBUG: Adding complex nested condition queries...");
    for c in &containers {
        if inputs.len() >= len {
            break;
        }
        let num_conditions = rng.gen_range(2..=5);
        let mut query = format!("SEARCH CONTAINER '{}' WHERE ", c);
        for i in 0..num_conditions {
            let field_index = rng.gen_range(0..fields.len());
            let op_index = rng.gen_range(0..ops.len());
            let value_index = rng.gen_range(0..values.len());
            query.push_str(&format!("'{}' {} {}", fields[field_index], ops[op_index], values[value_index]));
            if i < num_conditions - 1 {
                let connector = if rng.gen_bool(0.5) { "AND" } else { "OR" };
                query.push_str(&format!(" {} ", connector));
            }
        }
        //println!("DEBUG: Complex nested query generated: '{}'", query);
        safe_insert(&mut inputs, query);
    }

    // Queries with mixed data types in comparisons (should still be valid lexically)
    //println!("DEBUG: Adding mixed data type comparison queries...");
    if inputs.len() < len {
        let input = format!("SEARCH CONTAINER 'data1' WHERE 'field1' == 100.0");
        //println!("DEBUG: Mixed type query generated: '{}'", input);
        safe_insert(&mut inputs, input);
    }
    if inputs.len() < len {
        let input = format!("SEARCH CONTAINER 'data2' WHERE 'field2' > 'abc'");
        //println!("DEBUG: Mixed type query generated: '{}'", input);
        safe_insert(&mut inputs, input);
    }
    if inputs.len() < len {
        let input = format!("SEARCH CONTAINER 'data3' WHERE 'field3' != true");
        //println!("DEBUG: Mixed type query generated: '{}'", input);
        safe_insert(&mut inputs, input);
    }

    // Very long strings and numbers to test limits
    //println!("DEBUG: Adding long token queries...");
    if inputs.len() < len {
        let long_string = "'".to_string() + &"a".repeat(2000) + "'";
        let input = format!("SEARCH CONTAINER 'data4' WHERE 'field4' == {}", long_string);
        //println!("DEBUG: Long string query generated: '{}'", input);
        safe_insert(&mut inputs, input);
    }
    if inputs.len() < len {
        let long_int = "1".repeat(50);
        let input = format!("SEARCH CONTAINER 'data5' WHERE 'field5' > {}", long_int);
        //println!("DEBUG: Long int query generated: '{}'", input);
        safe_insert(&mut inputs, input);
    }

    // Queries with unusual spacing
    //println!("DEBUG: Adding unusual spacing queries...");
    if inputs.len() < len {
        let input = "SEARCH  CONTAINER 'data6'   WHERE  'field6' ==   1".to_string();
        //println!("DEBUG: Unusual spacing query generated: '{}'", input);
        safe_insert(&mut inputs, input);
    }

    // Ensure at least 1000 inputs with more randomized complex queries
    //println!("DEBUG: Ensuring 1000 inputs with randomized queries...");
    while inputs.len() < len {
        let container_index = rng.gen_range(0..containers.len());
        let num_conditions = rng.gen_range(1..=4);
        let mut query = format!("SEARCH CONTAINER '{}' WHERE ", containers[container_index]);
        for i in 0..num_conditions {
            let field_index = rng.gen_range(0..fields.len());
            let op_index = rng.gen_range(0..ops.len());
            let value_type = rng.gen_range(0..4); // 0: int, 1: float, 2: string, 3: bool
            let value = match value_type {
                0 => ints[rng.gen_range(0..ints.len())].to_string(),
                1 => floats[rng.gen_range(0..floats.len())].to_string(),
                2 => strings[rng.gen_range(0..strings.len())].to_string(),
                3 => bools[rng.gen_range(0..bools.len())].to_string(),
                _ => unreachable!(),
            };
            query.push_str(&format!("'{}' {} {}", fields[field_index], ops[op_index], value));
            if i < num_conditions - 1 {
                let connector = if rng.gen_bool(0.5) { "AND" } else { "OR" };
                query.push_str(&format!(" {} ", connector));
            }
        }
        //println!("DEBUG: Randomized complex query generated: '{}'", query);
        safe_insert(&mut inputs, query);
    }

    let mut final_inputs: Vec<String> = inputs.into_iter().collect();
    final_inputs.shuffle(&mut rng); // Shuffle to further prevent compiler optimization based on order
    final_inputs.truncate(len); // Ensure exactly 1000 inputs
    if let Some(s) = final_inputs.first(){
        if s.is_empty(){
            return generate_complex_lexer_inputs(len)
        }
    }

    final_inputs
}

